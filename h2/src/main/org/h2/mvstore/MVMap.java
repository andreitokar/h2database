/*
 * Copyright 2004-2014 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.mvstore;

import java.util.*;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import org.h2.api.ErrorCode;
import org.h2.message.DbException;
import org.h2.mvstore.type.DataType;
import org.h2.mvstore.type.ObjectDataType;
import org.h2.util.New;

/**
 * A stored map.
 * <p>
 * Read operations can happen concurrently with all other
 * operations, without risk of corruption.
 * <p>
 * Write operations first read the relevant area from disk to memory
 * concurrently, and only then modify the data. The in-memory part of write
 * operations is synchronized. For scalable concurrent in-memory write
 * operations, the map should be split into multiple smaller sub-maps that are
 * then synchronized independently.
 *
 * @param <K> the key class
 * @param <V> the value class
 */
public class MVMap<K, V> extends AbstractMap<K, V>
        implements ConcurrentMap<K, V> {

    /**
     * The store.
     */
    protected MVStore store;

    private final AtomicReference<RootReference> root = new AtomicReference<>();
    private final Semaphore updateSemaphore = new Semaphore(3, true);

    /**
     * The version used for writing.
     */
    protected volatile long writeVersion;

    private int id;
    private long createVersion;
    private final DataType keyType;
    private final DataType valueType;


    /**
     * Whether the map is closed. Volatile so we don't accidentally write to a
     * closed map in multithreaded mode.
     */
    private volatile boolean closed;
    private boolean readOnly;
    private boolean isVolatile;

    protected MVMap(DataType keyType, DataType valueType) {
        this.keyType = keyType;
        this.valueType = valueType;
    }

    public MVMap<K,V>cloneFromThis() {
        return new MVMap<>(keyType, valueType);
    }

    /**
     * Get the metadata key for the root of the given map id.
     *
     * @param mapId the map id
     * @return the metadata key
     */
    static String getMapRootKey(int mapId) {
        return "root." + Integer.toHexString(mapId);
    }

    /**
     * Get the metadata key for the given map id.
     *
     * @param mapId the map id
     * @return the metadata key
     */
    static String getMapKey(int mapId) {
        return "map." + Integer.toHexString(mapId);
    }

    /**
     * Open this map with the given store and configuration.
     *
     * @param store the store
     * @param config the configuration
     */
    protected void init(MVStore store, HashMap<String, Object> config) {
        this.store = store;
        this.id = DataUtils.readHexInt(config, "id", 0);
        this.createVersion = DataUtils.readHexLong(config, "createVersion", 0);
        this.writeVersion = store.getCurrentVersion();
        setRoot(Page.createEmpty(this), -1);
    }

    /**
     * Add or replace a key-value pair.
     *
     * @param key the key (may not be null)
     * @param value the value (may not be null)
     * @return the old value if the key existed, or null otherwise
     */
    @Override
    public V put(K key, V value) {
        return put(key, value, null);
    }

    public abstract static class DecisionMaker<V> {
        static final DecisionMaker<Object> IF_ABSENT = new DecisionMaker<Object>() {
            @Override
            public Decision decide(Object existingValue, Object providedValue) {
                return existingValue == null ? Decision.PUT : Decision.ABORT;
            }

            @Override
            public String toString() {
                return "if_absent";
            }
        };

        static final DecisionMaker<Object> IF_PRESENT = new DecisionMaker<Object>() {
            @Override
            public Decision decide(Object existingValue, Object providedValue) {
                return existingValue != null ? Decision.PUT : Decision.ABORT;
            }

            @Override
            public String toString() {
                return "if_present";
            }
        };

        public abstract Decision decide(V existingValue, V providedValue);
        public V selectValue(V existingValue, V providedValue) {
            return providedValue;
        }
        public void reset() {}
    }

    public enum Decision { ABORT, REMOVE, PUT }

    /**
     * Add or replace a key-value pair.
     *
     * @param key the key (may not be null)
     * @param value the value (may not be null)
     * @return the old value if the key existed, or null otherwise
     */
    @SuppressWarnings("unchecked")
    public final V put(K key, V value, DecisionMaker<? super V> decisionMaker) {
        DataUtils.checkArgument(value != null, "The value may not be null");
        return operate(key, value, decisionMaker);
    }

    public final V operate(K key, V value, DecisionMaker<? super V> decisionMaker) {
        while (true) {
            try {
                if (updateSemaphore.tryAcquire(30, TimeUnit.SECONDS)) {
                    try {
                        return operateUnderLock(key, value, decisionMaker);
                    } finally {
                        updateSemaphore.release();
                    }
                }
                throw DbException.get(ErrorCode.LOCK_TIMEOUT_1, "Unable to obtain update permit");
            } catch (InterruptedException ignore) {
            }
        }
    }

    public V operateUnderLock(K key, V value, DecisionMaker<? super V> decisionMaker) {
        beforeWrite();
        int attempt = 0;
        while(true) {
            ++attempt;
            RootReference rootReference = getRoot();
            long version = writeVersion;
            if(rootReference.version > version) {
                throw new IllegalStateException("Inconsistent versions, current:" + rootReference.version + ", new:" + version);
            }

            CursorPos pos = traverseDown(rootReference.root, key);
            if(rootReference != getRoot()) {
//                System.out.println("    Concurrent update of "+getId()+"/"+getName()+" key="+key+", retry...");
                continue;
            }

            Page p = pos.page;
            int index = pos.index;
            CursorPos tip = pos;
            pos = pos.parent;
            final V result = index < 0 ? null : (V)p.getValue(index);
            Decision decision = decisionMaker != null ? decisionMaker.decide(result, value) :
                                value == null         ? Decision.REMOVE :
                                                        Decision.PUT;
            int unsavedMemory = 0;
            switch (decision) {
                case ABORT:
                    return result;
                case REMOVE:
                    if (index < 0) {
                        return result;
                    }
                    if (p.getTotalCount() == 1 && pos != null) {
                        p = pos.page;
                        index = pos.index;
                        pos = pos.parent;
                        if (p.getKeyCount() == 1) {
                            assert index <= 1;
                            p = p.getChildPage(1 - index);
                            break;
                        }
                        assert p.getKeyCount() > 1;
                    }
                    p = p.copy(version);
                    p.remove(index);
                    break;
                case PUT:
                    if(decisionMaker != null) {
                        value = (V)decisionMaker.selectValue(result, value);
                    }
                    if (index < 0) {
                        index = -index - 1;
                        p = p.copy(version);
                        p.insertLeaf(index, key, value);
                        int pageSplitSize = store.getPageSplitSize();
                        while (p.getMemory() > pageSplitSize && p.getKeyCount() > (p.isLeaf() ? 1 : 2)) {
                            long totalCount = p.getTotalCount();
                            int at = p.getKeyCount() / 2;
                            Object k = p.getKey(at);
                            Page split = p.split(at);
                            unsavedMemory += p.getMemory();
                            unsavedMemory += split.getMemory();
                            if (pos == null) {
                                Object keys[] = {k};
                                Page.PageReference children[] = {
                                        new Page.PageReference(p),
                                        new Page.PageReference(split)
                                };
                                p = Page.create(this, keys, null, children, totalCount, 0);
                                break;
                            }
                            Page c = p;
                            p = pos.page;
                            index = pos.index;
                            pos = pos.parent;
                            p = p.copy(version);
                            p.setChild(index, split);
                            p.insertNode(index, k, c);
                        }
                    } else {
                        p = p.copy(version);
                        p.setValue(index, value);
                    }
                    break;
            }
            unsavedMemory += p.getMemory();
            if(rootReference != getRoot()) {
//                System.out.println("--- Concurrent update of "+getId()+"/"+getName()+" key="+key+", retry...");
                if(decisionMaker != null) {
                    decisionMaker.reset();
                }
                continue;
            }

            while (pos != null) {
                Page c = p;
                p = pos.page;
                p = p.copy(version);
                p.setChild(pos.index, c);
                unsavedMemory += p.getMemory();
                pos = pos.parent;
            }
            if(newRoot(rootReference, p, writeVersion, attempt)) {
                while(tip != null) {
                    tip.page.removePage();
                    tip = tip.parent;
                }
                if(store.getFileStore() != null) {
                    store.registerUnsavedPage(unsavedMemory);
                }
                return result;
            }
            if(decisionMaker != null) {
                decisionMaker.reset();
            }
//            System.out.println("... CONCURRENT UPDATE of "+getId()+"/"+getName()+" key="+key+", retry...");
        }
    }

    private static CursorPos traverseDown(Page p, Object key) {
        CursorPos pos = null;
        while (!p.isLeaf()) {
            assert p.getKeyCount() > 0;
            int index = p.binarySearch(key) + 1;
            if (index < 0) {
                index = -index;
            }
            pos = new CursorPos(p, index, pos);
            p = p.getChildPage(index);
        }
        return new CursorPos(p, p.binarySearch(key), pos);
    }

    /**
     * Get the first key, or null if the map is empty.
     *
     * @return the first key, or null
     */
    public final K firstKey() {
        return getFirstLast(true);
    }

    /**
     * Get the last key, or null if the map is empty.
     *
     * @return the last key, or null
     */
    public final K lastKey() {
        return getFirstLast(false);
    }

    /**
     * Get the key at the given index.
     * <p>
     * This is a O(log(size)) operation.
     *
     * @param index the index
     * @return the key
     */
    @SuppressWarnings("unchecked")
    public final K getKey(long index) {
        if (index < 0 || index >= sizeAsLong()) {
            return null;
        }
        Page p = getRootPage();
        long offset = 0;
        while (true) {
            if (p.isLeaf()) {
                if (index >= offset + p.getKeyCount()) {
                    return null;
                }
                return (K) p.getKey((int) (index - offset));
            }
            int i = 0, size = getChildPageCount(p);
            for (; i < size; i++) {
                long c = p.getCounts(i);
                if (index < c + offset) {
                    break;
                }
                offset += c;
            }
            if (i == size) {
                return null;
            }
            p = p.getChildPage(i);
        }
    }

    /**
     * Get the key list. The list is a read-only representation of all keys.
     * <p>
     * The get and indexOf methods are O(log(size)) operations. The result of
     * indexOf is cast to an int.
     *
     * @return the key list
     */
    public final List<K> keyList() {
        return new AbstractList<K>() {

            @Override
            public K get(int index) {
                return getKey(index);
            }

            @Override
            public int size() {
                return MVMap.this.size();
            }

            @Override
            @SuppressWarnings("unchecked")
            public int indexOf(Object key) {
                return (int) getKeyIndex((K) key);
            }

        };
    }

    /**
     * Get the index of the given key in the map.
     * <p>
     * This is a O(log(size)) operation.
     * <p>
     * If the key was found, the returned value is the index in the key array.
     * If not found, the returned value is negative, where -1 means the provided
     * key is smaller than any keys. See also Arrays.binarySearch.
     *
     * @param key the key
     * @return the index
     */
    public final long getKeyIndex(K key) {
        Page p = getRootPage();
        if (p.getTotalCount() == 0) {
            return -1;
        }
        long offset = 0;
        while (true) {
            int x = p.binarySearch(key);
            if (p.isLeaf()) {
                if (x < 0) {
                    offset = -offset;
                }
                return offset + x;
            }
            if (x++ < 0) {
                x = -x;
            }
            for (int i = 0; i < x; i++) {
                offset += p.getCounts(i);
            }
            p = p.getChildPage(x);
        }
    }

    /**
     * Get the first (lowest) or last (largest) key.
     *
     * @param first whether to retrieve the first key
     * @return the key, or null if the map is empty
     */
    @SuppressWarnings("unchecked")
    private K getFirstLast(boolean first) {
        Page p = getRootPage();
        if (p.getTotalCount() == 0) {
            return null;
        }
        while (true) {
            if (p.isLeaf()) {
                return (K) p.getKey(first ? 0 : p.getKeyCount() - 1);
            }
            p = p.getChildPage(first ? 0 : getChildPageCount(p) - 1);
        }
    }

    /**
     * Get the smallest key that is larger than the given key, or null if no
     * such key exists.
     *
     * @param key the key
     * @return the result
     */
    public final K higherKey(K key) {
        return getMinMax(key, false, true);
    }

    /**
     * Get the smallest key that is larger or equal to this key.
     *
     * @param key the key
     * @return the result
     */
    public final K ceilingKey(K key) {
        return getMinMax(key, false, false);
    }

    /**
     * Get the largest key that is smaller or equal to this key.
     *
     * @param key the key
     * @return the result
     */
    public final K floorKey(K key) {
        return getMinMax(key, true, false);
    }

    /**
     * Get the largest key that is smaller than the given key, or null if no
     * such key exists.
     *
     * @param key the key
     * @return the result
     */
    public final K lowerKey(K key) {
        return getMinMax(key, true, true);
    }

    /**
     * Get the smallest or largest key using the given bounds.
     *
     * @param key the key
     * @param min whether to retrieve the smallest key
     * @param excluding if the given upper/lower bound is exclusive
     * @return the key, or null if no such key exists
     */
    private K getMinMax(K key, boolean min, boolean excluding) {
        return getMinMax(getRootPage(), key, min, excluding);
    }

    @SuppressWarnings("unchecked")
    private K getMinMax(Page p, K key, boolean min, boolean excluding) {
        int x = p.binarySearch(key);
        if (p.isLeaf()) {
            if (x < 0) {
                x = -x - (min ? 2 : 1);
            } else if (excluding) {
                x += min ? -1 : 1;
            }
            if (x < 0 || x >= p.getKeyCount()) {
                return null;
            }
            return (K) p.getKey(x);
        }
        if (x++ < 0) {
            x = -x;
        }
        while (true) {
            if (x < 0 || x >= getChildPageCount(p)) {
                return null;
            }
            K k = getMinMax(p.getChildPage(x), key, min, excluding);
            if (k != null) {
                return k;
            }
            x += min ? -1 : 1;
        }
    }


    /**
     * Get a value.
     *
     * @param key the key
     * @return the value, or null if not found
     */
    @Override
    @SuppressWarnings("unchecked")
    public V get(Object key) {
        Page p = getRootPage();
        return get(p, key);
    }

    public V get(Page p, Object key) {
        while (true) {
            int index = p.binarySearch(key);
            if (p.isLeaf()) {
                return index >= 0 ? (V)p.getValue(index) : null;
            } else if (index++ < 0) {
                index = -index;
            }
            p = p.getChildPage(index);
        }
    }

    @Override
    public boolean containsKey(Object key) {
        return get(key) != null;
    }

    /**
     * Remove all entries.
     */
    @Override
    public void clear() {
        beforeWrite();
        RootReference rootReference;
        long version;
        int attempt = 0;
        do {
            rootReference = getRoot();
            rootReference.root.removeAllRecursive();
            version = writeVersion;
        } while (!newRoot(rootReference, Page.createEmpty(this), version, ++attempt));
    }

    /**
     * Close the map. Accessing the data is still possible (to allow concurrent
     * reads), but it is marked as closed.
     */
    final void close() {
        closed = true;
    }

    public final boolean isClosed() {
        return closed;
    }

    /**
     * Remove a key-value pair, if the key exists.
     *
     * @param key the key (may not be null)
     * @return the old value if the key existed, or null otherwise
     */
    @Override
    @SuppressWarnings("unchecked")
    public V remove(Object key) {
        return operate((K)key, null, null);
    }

    /**
     * Add a key-value pair if it does not yet exist.
     *
     * @param key the key (may not be null)
     * @param value the new value
     * @return the old value if the key existed, or null otherwise
     */
    @Override
    public final V putIfAbsent(K key, V value) {
        return put(key, value, DecisionMaker.IF_ABSENT);
    }

    /**
     * Remove a key-value pair if the value matches the stored one.
     *
     * @param key the key (may not be null)
     * @param value the expected value
     * @return true if the item was removed
     */
    @Override
    public boolean remove(Object key, Object value) {
        EqualsDecisionMaker<V> decisionMaker = new EqualsDecisionMaker<V>(valueType, (V)value);
        V result = operate((K)key, null, decisionMaker);
        return decisionMaker.decision != Decision.ABORT;
    }

    /**
     * Check whether the two values are equal.
     *
     * @param a the first value
     * @param b the second value
     * @return true if they are equal
     */
    public final boolean areValuesEqual(Object a, Object b) {
        return areValuesEqual(valueType, a, b);
    }

    /**
     * Check whether the two values are equal.
     *
     * @param a the first value
     * @param b the second value
     * @param datatype to use for comparison
     * @return true if they are equal
     */
    public static boolean areValuesEqual(DataType datatype, Object a, Object b) {
        return a == b
            || a != null && b != null && datatype.compare(a, b) == 0;
    }

    /**
     * Replace a value for an existing key, if the value matches.
     *
     * @param key the key (may not be null)
     * @param oldValue the expected value
     * @param newValue the new value
     * @return true if the value was replaced
     */
    @Override
    public final boolean replace(K key, V oldValue, V newValue) {
        EqualsDecisionMaker<V> decisionMaker = new EqualsDecisionMaker<V>(valueType, oldValue);
        V result = put(key, newValue, decisionMaker);
        return decisionMaker.decision != Decision.ABORT;
    }

    /**
     * Replace a value for an existing key.
     *
     * @param key the key (may not be null)
     * @param value the new value
     * @return the old value, if the value was replaced, or null
     */
    @Override
    public final V replace(K key, V value) {
        return put(key, value, DecisionMaker.IF_PRESENT);
    }

    private void rollbackRoot(long version)
    {
        RootReference rootReference = getRoot();
        RootReference previous;
        while ((previous = rootReference.previous) != null && rootReference.version >= version) {
            if(root.compareAndSet(rootReference, previous)) {
                rootReference = previous;
            }
        }
    }

    /**
     * Use the new root page from now on.
     *
     * @param oldRoot the old root reference, will use the current root reference, if nuul is specified
     * @param newRoot the new root page
     */
    protected final boolean newRoot(RootReference oldRoot, Page newRoot, long newVersion, int attemptUpdateCounter) {
//        if(attemptUpdateCounter > 1000) {
//            throw DataUtils.newIllegalStateException(DataUtils.ERROR_INTERNAL,
//                    "Unable to change RootReference after {0} attempts", attemptUpdateCounter);
//        }
        RootReference currentRoot = getRoot();
        if (currentRoot != oldRoot && oldRoot != null) {
            return false;
        }

        RootReference previous = currentRoot;
        long updateCounter = 1;
        if(currentRoot != null) {
            long currentVersion = currentRoot.version;
//            if (currentVersion > newVersion) {
//                throw new IllegalStateException("Illegal root version change from " + currentVersion + " to " + newVersion);
//            }

            removeUnusedOldVersions();

            if (currentVersion == newVersion) {
                previous = currentRoot.previous;
            }

            updateCounter += currentRoot.updateCounter;
            attemptUpdateCounter += currentRoot.updateAttemptCounter;
        }
        return root.compareAndSet(currentRoot,
                new RootReference(newRoot, newVersion, previous, updateCounter, attemptUpdateCounter));
    }

    /**
     * Compare two keys.
     *
     * @param a the first key
     * @param b the second key
     * @return -1 if the first key is smaller, 1 if bigger, 0 if equal
     */
    final int compare(Object a, Object b) {
        return keyType.compare(a, b);
    }

    /**
     * Get the key type.
     *
     * @return the key type
     */
    public final DataType getKeyType() {
        return keyType;
    }

    /**
     * Get the value type.
     *
     * @return the value type
     */
    public final DataType getValueType() {
        return valueType;
    }

    /**
     * Read a page.
     *
     * @param pos the position of the page
     * @return the page
     */
    final Page readPage(long pos) {
        return store.readPage(this, pos);
    }

    /**
     * Set the position of the root page.
     *
     * @param rootPos the position, 0 for empty
     * @param version the version of the root
     */
    final void setRootPos(long rootPos, long version) {
        Page root = readOrCreateRootPage(rootPos);
        setRoot(root, version);
    }

    private Page readOrCreateRootPage(long rootPos) {
        Page root = rootPos == 0 ? Page.createEmpty(this) : readPage(rootPos);
        return root;
    }

    /**
     * Iterate over a number of keys.
     *
     * @param from the first key to return
     * @return the iterator
     */
    public final Iterator<K> keyIterator(K from) {
        return new Cursor<K, V>(this, getRootPage(), from);
    }

    /**
     * Re-write any pages that belong to one of the chunks in the given set.
     *
     * @param set the set of chunk ids
     * @return whether rewriting was successful
     */
    final boolean rewrite(Set<Integer> set) {
        // read from old version, to avoid concurrent reads
        long previousVersion = store.getCurrentVersion() - 1;
        if (previousVersion < createVersion) {
            // a new map
            return true;
        }
        MVMap<K, V> readMap;
        try {
            readMap = openVersion(previousVersion);
        } catch (IllegalArgumentException e) {
            // unknown version: ok
            // TODO should not rely on exception handling
            return true;
        }
        try {
            rewrite(readMap.getRootPage(), set);
            return true;
        } catch (IllegalStateException e) {
            // TODO should not rely on exception handling
            if (DataUtils.getErrorCode(e.getMessage()) == DataUtils.ERROR_CHUNK_NOT_FOUND) {
                // ignore
                return false;
            }
            throw e;
        }
    }

    private int rewrite(Page p, Set<Integer> set) {
        if (p.isLeaf()) {
            long pos = p.getPos();
            int chunkId = DataUtils.getPageChunkId(pos);
            if (!set.contains(chunkId)) {
                return 0;
            }
            if (p.getKeyCount() > 0) {
                @SuppressWarnings("unchecked")
                K key = (K) p.getKey(0);
                V value = get(key);
                if (value != null) {
                    replace(key, value, value);
                }
            }
            return 1;
        }
        int writtenPageCount = 0;
        for (int i = 0; i < getChildPageCount(p); i++) {
            long childPos = p.getChildPagePos(i);
            if (childPos != 0 && DataUtils.getPageType(childPos) == DataUtils.PAGE_TYPE_LEAF) {
                // we would need to load the page, and it's a leaf:
                // only do that if it's within the set of chunks we are
                // interested in
                int chunkId = DataUtils.getPageChunkId(childPos);
                if (!set.contains(chunkId)) {
                    continue;
                }
            }
            writtenPageCount += rewrite(p.getChildPage(i), set);
        }
        if (writtenPageCount == 0) {
            long pos = p.getPos();
            int chunkId = DataUtils.getPageChunkId(pos);
            if (set.contains(chunkId)) {
                // an inner node page that is in one of the chunks,
                // but only points to chunks that are not in the set:
                // if no child was changed, we need to do that now
                // (this is not needed if anyway one of the children
                // was changed, as this would have updated this
                // page as well)
                Page p2 = p;
                while (!p2.isLeaf()) {
                    p2 = p2.getChildPage(0);
                }
                @SuppressWarnings("unchecked")
                K key = (K) p2.getKey(0);
                V value = get(key);
                if (value != null) {
                    replace(key, value, value);
                }
                writtenPageCount++;
            }
        }
        return writtenPageCount;
    }

    /**
     * Get a cursor to iterate over a number of keys and values.
     *
     * @param from the first key to return
     * @return the cursor
     */
    public final Cursor<K, V> cursor(K from) {
        return cursor(from, true);
    }

    public final Cursor<K, V> cursor(K from, boolean snapshot) {
        return new Cursor<K, V>(this, getRootPage(), from, snapshot);
    }

    @Override
    public final Set<Map.Entry<K, V>> entrySet() {
        final MVMap<K, V> map = this;
        final Page root = this.getRootPage();
        return new AbstractSet<Entry<K, V>>() {

            @Override
            public Iterator<Entry<K, V>> iterator() {
                final Cursor<K, V> cursor = new Cursor<K, V>(map, root, null);
                return new Iterator<Entry<K, V>>() {

                    @Override
                    public boolean hasNext() {
                        return cursor.hasNext();
                    }

                    @Override
                    public Entry<K, V> next() {
                        K k = cursor.next();
                        return new DataUtils.MapEntry<K, V>(k, cursor.getValue());
                    }

                    @Override
                    public void remove() {
                        throw DataUtils.newUnsupportedOperationException(
                                "Removing is not supported");
                    }
                };

            }

            @Override
            public int size() {
                throw DataUtils.newUnsupportedOperationException("MVMap.entrySet().size() is not supported.");
//                return MVMap.this.size();
            }

            @Override
            public boolean contains(Object o) {
                throw DataUtils.newUnsupportedOperationException("MVMap.entrySet().contains() is not supported.");
//                return MVMap.this.containsKey(o);
            }

        };

    }

    @Override
    public Set<K> keySet() {
        final MVMap<K, V> map = this;
        final Page root = this.getRootPage();
        return new AbstractSet<K>() {

            @Override
            public Iterator<K> iterator() {
                return new Cursor<K, V>(map, root, null, true);
            }

            @Override
            public int size() {
                return MVMap.this.size();
            }

            @Override
            public boolean contains(Object o) {
                return MVMap.this.containsKey(o);
            }

        };
    }

    /**
     * Get the map name.
     *
     * @return the name
     */
    public final String getName() {
        return store.getMapName(id);
    }

    public final MVStore getStore() {
        return store;
    }

    /**
     * Get the map id. Please note the map id may be different after compacting
     * a store.
     *
     * @return the map id
     */
    public final int getId() {
        return id;
    }

    /**
     * The current root page (may not be null).
     *
     * @return the root page
     */
    public final Page getRootPage() {
        return getRoot().root;
    }

    public final RootReference getRoot() {
        return root.get();
    }

    void setRoot(Page rootPage, long version) {
        int attempt = 0;
        while (!newRoot(null, rootPage, version, ++attempt)) {/**/}
    }

    /**
     * Rollback to the given version.
     *
     * @param version the version
     */
    final void rollbackTo(long version) {
        beforeWrite();
        // check if the map was removed and re-created later ?
        if (version > createVersion) {
            rollbackRoot(version);
        }
    }

    /**
     * Forget those old versions that are no longer needed.
     */
    final void removeUnusedOldVersions() {
        long oldest = store.getOldestVersionToKeep();
        if (oldest == -1) {
            return;
        }
        RootReference rootReference = getRoot().previous;
        if(rootReference != null) {
            RootReference previous;
            while ((previous = rootReference.previous) != null) {
                if (previous.version < oldest) {
                    rootReference.previous = null;
                    break;
                }
                rootReference = previous;
            }
        }
    }

    public final boolean isReadOnly() {
        return readOnly;
    }

    /**
     * Set the volatile flag of the map.
     *
     * @param isVolatile the volatile flag
     */
    public final void setVolatile(boolean isVolatile) {
        this.isVolatile = isVolatile;
    }

    /**
     * Whether this is volatile map, meaning that changes
     * are not persisted. By default (even if the store is not persisted),
     * maps are not volatile.
     *
     * @return whether this map is volatile
     */
    public final boolean isVolatile() {
        return isVolatile;
    }

    /**
     * This method is called before writing to the map. The default
     * implementation checks whether writing is allowed, and tries
     * to detect concurrent modification.
     *
     * @throws UnsupportedOperationException if the map is read-only,
     *      or if another thread is concurrently writing
     */
    protected final void beforeWrite() {
        if (closed) {
            throw DataUtils.newIllegalStateException(
                    DataUtils.ERROR_CLOSED, "This map is closed");
        }
        if (readOnly) {
            throw DataUtils.newUnsupportedOperationException(
                    "This map is read-only");
        }
        if(store.getFileStore() != null) {
            store.beforeWrite(this);
        }
    }

    @Override
    public int hashCode() {
        return id;
    }

    @Override
    public boolean equals(Object o) {
        return this == o;
    }

    /**
     * Get the number of entries, as a integer. Integer.MAX_VALUE is returned if
     * there are more than this entries.
     *
     * @return the number of entries, as an integer
     */
    @Override
    public int size() {
        long size = sizeAsLong();
        return size > Integer.MAX_VALUE ? Integer.MAX_VALUE : (int) size;
    }

    /**
     * Get the number of entries, as a long.
     *
     * @return the number of entries
     */
    public final long sizeAsLong() {
        return getRootPage().getTotalCount();
    }

    @Override
    public boolean isEmpty() {
        Page rootPage = getRootPage();
        return rootPage.isLeaf() && rootPage.getKeyCount() == 0;
    }

    public final long getCreateVersion() {
        return createVersion;
    }

    /**
     * Remove the given page (make the space available).
     *
     * @param pos the position of the page to remove
     * @param memory the number of bytes used for this page
     */
    protected final void removePage(long pos, int memory) {
        store.removePage(this, pos, memory);
    }

    /**
     * Open an old version for the given map.
     *
     * @param version the version
     * @return the map
     */
    public final MVMap<K, V> openVersion(long version) {
        if (readOnly) {
            throw DataUtils.newUnsupportedOperationException(
                    "This map is read-only; need to call " +
                    "the method on the writable map");
        }
        DataUtils.checkArgument(version >= createVersion,
                "Unknown version {0}; this map was created in version is {1}",
                version, createVersion);
        RootReference rootReference = getRoot();
        while (rootReference != null && (rootReference.version > version || rootReference.version < 0 && store.getFileStore() != null)) {
            rootReference = rootReference.previous;
        }

        if (rootReference == null) {
            if(store.getFileStore() != null) {
                // smaller than all in-memory versions
                MVMap<K, V> map = openReadOnly(store.getRootPos(getId(), version), version);
                return map;
            }
            throw DataUtils.newIllegalArgumentException("Version {0} not found", version);
        }
        MVMap<K, V> m = openReadOnly(rootReference.root, version);
        assert m.getVersion() <= version : m.getVersion() + " <= " + version;
        return m;
    }

    /**
     * Open a copy of the map in read-only mode.
     *
     * @param rootPos position of the root page
     * @param version to open
     * @return the opened map
     */
    final MVMap<K, V> openReadOnly(long rootPos, long version) {
        Page root = readOrCreateRootPage(rootPos);
        return openReadOnly(root, version);
    }

    private MVMap<K, V> openReadOnly(Page root, long version) {
        MVMap<K, V> m = cloneFromThis();
        m.readOnly = true;
        HashMap<String, Object> config = New.hashMap();
        config.put("id", id);
        config.put("createVersion", createVersion);
        m.init(store, config);
        m.setRoot(root, version);
        return m;
    }

    public final long getVersion() {
        return getRoot().version;
    }

    /**
     * Get the child page count for this page. This is to allow another map
     * implementation to override the default, in case the last child is not to
     * be used.
     *
     * @param p the page
     * @return the number of direct children
     */
    protected int getChildPageCount(Page p) {
        return p.getRawChildPageCount();
    }

    /**
     * Get the map type. When opening an existing map, the map type must match.
     *
     * @return the map type
     */
    public String getType() {
        return null;
    }

    /**
     * Get the map metadata as a string.
     *
     * @param name the map name (or null)
     * @return the string
     */
    final String asString(String name) {
        StringBuilder buff = new StringBuilder();
        if (name != null) {
            DataUtils.appendMap(buff, "name", name);
        }
        if (createVersion != 0) {
            DataUtils.appendMap(buff, "createVersion", createVersion);
        }
        String type = getType();
        if (type != null) {
            DataUtils.appendMap(buff, "type", type);
        }
        return buff.toString();
    }

    final void setWriteVersion(long writeVersion) {
        this.writeVersion = writeVersion;
    }

    /**
     * Copy a map. All pages are copied.
     *
     * @param sourceMap the source map
     */
    final void copyFrom(MVMap<K, V> sourceMap) {
        beforeWrite();
        setRoot(copy(sourceMap.getRootPage(), null), writeVersion);
    }

    private Page copy(Page source, CursorPos parent) {
        Page target = Page.create(this, source);
        if (!source.isLeaf()) {
            CursorPos pos = new CursorPos(target, 0, parent);
            for (int i = 0; i < getChildPageCount(target); i++) {
                if (source.getChildPagePos(i) != 0) {
                    // position 0 means no child
                    // (for example the last entry of an r-tree node)
                    // (the MVMap is also used for r-trees for compacting)
                    pos.index = i;
                    Page child = copy(source.getChildPage(i), pos);
                    target.setChild(i, child);
                    pos.page = target;
                }
            }

            if(store.isSaveNeeded()) {
                Page child = target;
                for(CursorPos p = parent; p != null; p = p.parent) {
                    p.page.setChild(p.index, child);
                    child = p.page;
                }
                setRoot(child, writeVersion);
                beforeWrite();
            }
        }
        return target;
    }

    @Override
    public String toString() {
        return asString(null);
    }

    /**
     * A builder for maps.
     *
     * @param <M> the map type
     * @param <K> the key type
     * @param <V> the value type
     */
    public interface MapBuilder<M extends MVMap<K, V>, K, V> {

        /**
         * Create a new map of the given type.
         *
         * @return the map
         */
        M create();

    }

    /**
     * A builder for this class.
     *
     * @param <K> the key type
     * @param <V> the value type
     */
    public static final class Builder<K, V> implements MapBuilder<MVMap<K, V>, K, V> {

        protected DataType keyType;
        protected DataType valueType;

        /**
         * Create a new builder with the default key and value data types.
         */
        public Builder() {
            // ignore
        }

        /**
         * Set the key data type.
         *
         * @param keyType the key type
         * @return this
         */
        public Builder<K, V> keyType(DataType keyType) {
            this.keyType = keyType;
            return this;
        }

        public DataType getKeyType() {
            return keyType;
        }

        public DataType getValueType() {
            return valueType;
        }

        /**
         * Set the value data type.
         *
         * @param valueType the value type
         * @return this
         */
        public Builder<K, V> valueType(DataType valueType) {
            this.valueType = valueType;
            return this;
        }

        @Override
        public MVMap<K, V> create() {
            if (keyType == null) {
                keyType = new ObjectDataType();
            }
            if (valueType == null) {
                valueType = new ObjectDataType();
            }
            return new MVMap<K, V>(keyType, valueType);
        }

    }

    private static final class EqualsDecisionMaker<V> extends DecisionMaker<V> {
        private final DataType dataType;
        private final V        expectedValue;
        private       Decision decision;

        private EqualsDecisionMaker(DataType dataType, V expectedValue) {
            this.dataType = dataType;
            this.expectedValue = expectedValue;
        }

        @Override
        public Decision decide(V existingValue, V providedValue) {
            assert decision == null;
//            if(decision == null) {
                decision = !areValuesEqual(dataType, expectedValue, existingValue) ? Decision.ABORT :
                                                providedValue == null ? Decision.REMOVE : Decision.PUT;
//            }
            return decision;
        }

        @Override
        public void reset() {
            decision = null;
        }

        @Override
        public String toString() {
            return "equals_to "+expectedValue;
        }
    }

    public static final class RootReference
    {
        public  final    Page          root;
        public  final    long          version;
        private volatile RootReference previous;
        public           long          updateCounter;
        public           long          updateAttemptCounter;

        private RootReference(Page root, long version, RootReference previous, long updateCounter, long updateAttemptCounter) {
            this.root = root;
            this.version = version;
            this.previous = previous;
            this.updateCounter = updateCounter;
            this.updateAttemptCounter = updateAttemptCounter;
        }
    }
}
