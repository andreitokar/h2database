/*
 * Copyright 2004-2014 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.mvstore;

import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.ConcurrentMap;

import org.h2.mvstore.type.DataType;
import org.h2.mvstore.type.ExtendedDataType;
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

    /**
     * The current root page (may not be null).
     */
    protected volatile Page root;

    /**
     * The version used for writing.
     */
    protected volatile long writeVersion;

    private int id;
    private long createVersion;
    private final DataType keyType;
    private final ExtendedDataType extendedKeyType;
    private final DataType valueType;
    private final ExtendedDataType extendedValueType;

    private Deque<Page> oldRoots = new ConcurrentLinkedDeque<>();


    /**
     * Whether the map is closed. Volatile so we don't accidentally write to a
     * closed map in multithreaded mode.
     */
    private volatile boolean closed;
    private boolean readOnly;
    private boolean isVolatile;

    protected MVMap(DataType keyType, DataType valueType) {
        this.keyType = keyType;
        this.extendedKeyType = keyType instanceof ExtendedDataType ? (ExtendedDataType) keyType : new DataTypeExtentionWrapper(keyType);
        this.valueType = valueType;
        this.extendedValueType = valueType instanceof ExtendedDataType ? (ExtendedDataType) valueType : new DataTypeExtentionWrapper(valueType);
//        this.valueType = valueType instanceof ExtendedDataType ? (ExtendedDataType) valueType : new DataTypeExtentionWrapper(valueType);
//        this.root = Page.createEmpty(this,  -1);
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
        this.root = Page.createEmpty(this,  -1);
    }

    /**
     * Add or replace a key-value pair.
     *
     * @param key the key (may not be null)
     * @param value the value (may not be null)
     * @return the old value if the key existed, or null otherwise
     */
    @Override
    public /*synchronized*/ V put(K key, V value) {
        return put(key, value, null);
    }

    /**
     * Add or replace a key-value pair.
     *
     * @param key the key (may not be null)
     * @param value the value (may not be null)
     * @return the old value if the key existed, or null otherwise
     */
    @SuppressWarnings("unchecked")
    public /*synchronized*/ V put(K key, V value, DecisionMaker decisionMaker) {
        DataUtils.checkArgument(value != null, "The value may not be null");
        beforeWrite();
        long v = writeVersion;
        Page p = root.copy(v);
        p = splitRootIfNeeded(p, v);
        Object result = put(p, v, key, value, decisionMaker, p);
//        newRoot(p);
        return (V) result;
    }

    /**
     * Split the root page if necessary.
     *
     * @param p the page
     * @param writeVersion the write version
     * @return the new sibling
     */
    private Page splitRootIfNeeded(Page p, long writeVersion) {
        if (p.getMemory() > store.getPageSplitSize() && p.getKeyCount() > 1) {
            int at = p.getKeyCount() / 2;
            long totalCount = p.getTotalCount();
            Object k = p.getKey(at);
            Page split = p.split(at);
            Object keys = p.createKeyStorage(1);
            getExtendedKeyType().setValue(keys, 0, k);
            Page.PageReference[] children = {
                    new Page.PageReference(p, p.getPos(), p.getTotalCount()),
                    new Page.PageReference(split, split.getPos(), split.getTotalCount()),
            };
            p = Page.create(this, writeVersion,
                    keys, null,
                    children,
                    totalCount, 0);
        }
        return p;
    }

    public interface DecisionMaker {
        public DecisionMaker IF_ABSENT = new DecisionMaker() {
            @Override
            public boolean allow(Object currentValue) {
                return currentValue == null;
            }
        };

        public DecisionMaker IF_PRESENT = new DecisionMaker() {
            @Override
            public boolean allow(Object currentValue) {
                return currentValue != null;
            }
        };

        boolean allow(Object currentValue);
    }
    /**
     * Add or update a key-value pair.
     *
     * @param p the page
     * @param writeVersion the write version
     * @param key the key (may not be null)
     * @param value the value (may not be null)
     * @return the old value, or null
     */
    protected final Object put(Page p, long writeVersion, Object key, Object value, DecisionMaker decisionMaker, Page newRoot) {
        int index = p.binarySearch(key);
        if (p.isLeaf()) {
            if (index < 0) {
                if(decisionMaker == null || decisionMaker.allow(null)) {
                    index = -index - 1;
                    p.insertLeaf(index, key, value);
                    newRoot(newRoot);
                }
                return null;
            }
            Object currentValue = p.getValue(index);
            if(decisionMaker == null || decisionMaker.allow(currentValue)) {
                currentValue = p.setValue(index, value);
                newRoot(newRoot);
            }
            return currentValue;
        }
        // p is a node
        if (index < 0) {
            index = -index - 1;
        } else {
            index++;
        }
        Page c = p.getChildPage(index).copy(writeVersion);
        if (c.getMemory() > store.getPageSplitSize() && c.getKeyCount() > 1) {
            // split on the way down
            int at = c.getKeyCount() / 2;
            Object k = c.getKey(at);
            Page split = c.split(at);
            p.setChild(index, split);
            p.insertNode(index, k, c);
            // now we are not sure where to add
            return put(p, writeVersion, key, value, decisionMaker, newRoot);
        }
        Object result = put(c, writeVersion, key, value, decisionMaker, newRoot);
        p.setChild(index, c);
        return result;
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
        if (index < 0 || index >= size()) {
            return null;
        }
        Page p = root;
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
        if (size() == 0) {
            return -1;
        }
        Page p = root;
        long offset = 0;
        while (true) {
            int x = p.binarySearch(key);
            if (p.isLeaf()) {
                if (x < 0) {
                    return -offset + x;
                }
                return offset + x;
            }
            if (x < 0) {
                x = -x - 1;
            } else {
                x++;
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
        if (size() == 0) {
            return null;
        }
        Page p = root;
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
        return getMinMax(root, key, min, excluding);
    }

    @SuppressWarnings("unchecked")
    private K getMinMax(Page p, K key, boolean min, boolean excluding) {
        if (p.isLeaf()) {
            int x = p.binarySearch(key);
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
        int x = p.binarySearch(key);
        if (x < 0) {
            x = -x - 1;
        } else {
            x++;
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
     * Get the value for the given key, or null if not found.
     *
     * @param key the key
     * @return the value, or null if not found
     */
    @Override
    @SuppressWarnings("unchecked")
    public V get(Object key) {
        return (V) binarySearch(root, key);
    }

    /**
     * Get the value for the given key, or null if not found.
     *
     * @param p the page
     * @param key the key
     * @return the value or null
     */
    protected final Object binarySearch(Page p, Object key) {
        while (true) {
            int x = p.binarySearch(key);
            if(p.isLeaf())
            {
                return x >= 0 ? p.getValue(x) : null;
            }
            if (x < 0) {
                x = -x - 1;
            } else {
                x++;
            }
            p = p.getChildPage(x);
        }
    }

    @Override
    public final boolean containsKey(Object key) {
        return get(key) != null;
    }

    /**
     * Remove all entries.
     */
    @Override
    public final synchronized void clear() {
        beforeWrite();
        root.removeAllRecursive();
        newRoot(Page.createEmpty(this, writeVersion));
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
    public final V remove(Object key) {
        beforeWrite();
        V result = get(key);
        if (result == null) {
            return null;
        }
        long v = writeVersion;
        synchronized (this) {
            Page p = root.copy(v);
            result = (V) remove(p, v, key);
            if (!p.isLeaf() && p.getTotalCount() == 0) {
                p.removePage();
                p = Page.createEmpty(this,  p.getVersion());
            }
            newRoot(p);
        }
        return result;
    }

    /**
     * Add a key-value pair if it does not yet exist.
     *
     * @param key the key (may not be null)
     * @param value the new value
     * @return the old value if the key existed, or null otherwise
     */
    @Override
    public final /*synchronized*/ V putIfAbsent(K key, V value) {
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
    public final synchronized boolean remove(Object key, Object value) {
        V old = get(key);
        if (areValuesEqual(old, value)) {
            remove(key);
            return true;
        }
        return false;
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
        if (a == b) {
            return true;
        } else if (a == null || b == null) {
            return false;
        }
        return datatype.compare(a, b) == 0;
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
    public final /*synchronized*/ boolean replace(K key, V oldValue, V newValue) {
        EqualsDecisionMaker decisionMaker = new EqualsDecisionMaker(valueType, oldValue);
        put(key, oldValue, decisionMaker);
        return decisionMaker.wasAllowed();
    }

    /**
     * Replace a value for an existing key.
     *
     * @param key the key (may not be null)
     * @param value the new value
     * @return the old value, if the value was replaced, or null
     */
    @Override
    public final /*synchronized*/ V replace(K key, V value) {
        return put(key, value, DecisionMaker.IF_PRESENT);
    }

    /**
     * Remove a key-value pair.
     *
     * @param p the page (may not be null)
     * @param writeVersion the write version
     * @param key the key
     * @return the old value, or null if the key did not exist
     */
    protected Object remove(Page p, long writeVersion, Object key) {
        int index = p.binarySearch(key);
        Object result = null;
        if (p.isLeaf()) {
            if (index >= 0) {
                result = p.getValue(index);
                p.remove(index);
            }
            return result;
        }
        // node
        if (index < 0) {
            index = -index - 1;
        } else {
            index++;
        }
        Page cOld = p.getChildPage(index);
        Page c = cOld.copy(writeVersion);
        result = remove(c, writeVersion, key);
        if (result == null || c.getTotalCount() != 0) {
            // no change, or
            // there are more nodes
            p.setChild(index, c);
        } else {
            // this child was deleted
            if (p.getKeyCount() == 0) {
                p.setChild(index, c);
                c.removePage();
            } else {
                p.remove(index);
            }
        }
        return result;
    }

    /**
     * Use the new root page from now on.
     *
     * @param newRoot the new root page
     */
    protected final void newRoot(Page newRoot) {
        Page oldRoot = this.root;
        if (oldRoot != newRoot) {
            long currentVersion = oldRoot.getVersion();
            long newVersion = newRoot.getVersion();
            if(currentVersion > newVersion) {
                throw new IllegalStateException(currentVersion + " > " + newVersion);
            }
            if(currentVersion != newVersion) {
                removeUnusedOldVersions();
                Page last = oldRoots.peekLast();
                if (last == null || last.getVersion() != currentVersion) {
                    oldRoots.add(this.root);
                }
            }
            if(this.root != oldRoot) {
                throw new IllegalStateException("Concurrent root change");
            }
            this.root = newRoot;
        }
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
     * Get extended key type.
     *
     * @return the key type
     */
    protected final ExtendedDataType getExtendedKeyType() {
        return extendedKeyType;
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
     * Get extended value type.
     *
     * @return the value type
     */
    public final ExtendedDataType getExtendedValueType() {
        return extendedValueType;
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
        root = rootPos == 0 ? Page.createEmpty(this, -1) : readPage(rootPos);
        root.setVersion(version);
    }

    /**
     * Iterate over a number of keys.
     *
     * @param from the first key to return
     * @return the iterator
     */
    public final Iterator<K> keyIterator(K from) {
        return new Cursor<K, V>(this, root, from);
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
            rewrite(readMap.root, set);
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
        return new Cursor<K, V>(this, root, from);
    }

    @Override
    public final Set<Map.Entry<K, V>> entrySet() {
        return new MVEntrySet<>(this);

    }

    @Override
    public Set<K> keySet() {
        return new MVKeySet<>(this);
    }

    /**
     * Get the root page.
     *
     * @return the root page
     */
    public final Page getRoot() {
        return root;
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
     * Rollback to the given version.
     *
     * @param version the version
     */
    final void rollbackTo(long version) {
        beforeWrite();
        if (version <= createVersion) {
            // the map is removed later
        } else {
            while (root.getVersion() >= version) {
                Page last = oldRoots.peekLast();
                if (last == null) {
                    break;
                }
                // slow, but rollback is not a common operation
                oldRoots.removeLastOccurrence(last);
                root = last;
            }
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
        Page last = oldRoots.peekLast();
        while (true) {
            Page p = oldRoots.peekFirst();
            if (p == null || p.getVersion() >= oldest || p == last) {
                break;
            }
            oldRoots.removeFirstOccurrence(p);
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
        store.beforeWrite(this);
    }

    @Override
    public final int hashCode() {
        return id;
    }

    @Override
    public final boolean equals(Object o) {
        return this == o;
    }

    /**
     * Get the number of entries, as a integer. Integer.MAX_VALUE is returned if
     * there are more than this entries.
     *
     * @return the number of entries, as an integer
     */
    @Override
    public final int size() {
        long size = sizeAsLong();
        return size > Integer.MAX_VALUE ? Integer.MAX_VALUE : (int) size;
    }

    /**
     * Get the number of entries, as a long.
     *
     * @return the number of entries
     */
    public final long sizeAsLong() {
        return root.getTotalCount();
    }

    @Override
    public final boolean isEmpty() {
        // could also use (sizeAsLong() == 0)
        return root.isLeaf() && root.getKeyCount() == 0;
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
        Page newest = null;
        // need to copy because it can change
        Page r = root;
        if (version >= r.getVersion() &&
                (version == writeVersion ||
                r.getVersion() >= 0 ||
                version <= createVersion ||
                store.getFileStore() == null)) {
            newest = r;
        } else {
            Page last = oldRoots.peekFirst();
            if (last == null || version < last.getVersion()) {
                // smaller than all in-memory versions
                return store.openMapVersion(version, id, this);
            }
            Iterator<Page> it = oldRoots.iterator();
            while (it.hasNext()) {
                Page p = it.next();
                if (p.getVersion() > version) {
                    break;
                }
                last = p;
            }
            newest = last;
        }
        MVMap<K, V> m = openReadOnly();
        m.root = newest;
        return m;
    }

    /**
     * Open a copy of the map in read-only mode.
     *
     * @return the opened map
     */
    final MVMap<K, V> openReadOnly() {
        MVMap<K, V> m = new MVMap<K, V>(keyType, valueType);
        m.readOnly = true;
        HashMap<String, Object> config = New.hashMap();
        config.put("id", id);
        config.put("createVersion", createVersion);
        m.init(store, config);
        m.root = root;
        return m;
    }

    public final long getVersion() {
        return root.getVersion();
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
        newRoot(copy(sourceMap.root, null));
    }

    private Page copy(Page source, CursorPos parent) {
        Page target = Page.create(this, writeVersion, source);
        if (source.isLeaf()) {
            Page child = target;
            for (CursorPos p = parent; p != null; p = p.parent) {
                p.page.setChild(p.index, child);
                p.page = p.page.copy(writeVersion);
                child = p.page;
                if (p.parent == null) {
                    newRoot(p.page);
                    beforeWrite();
                }
            }
        } else {
            // temporarily, replace child pages with empty pages,
            // to ensure there are no links to the old store
            for (int i = 0; i < getChildPageCount(target); i++) {
                target.setChild(i, null);
            }
            CursorPos pos = new CursorPos(target, 0, parent);
            for (int i = 0; i < getChildPageCount(target); i++) {
                pos.index = i;
                long p = source.getChildPagePos(i);
                if (p != 0) {
                    // p == 0 means no child
                    // (for example the last entry of an r-tree node)
                    // (the MVMap is also used for r-trees for compacting)
                    copy(source.getChildPage(i), pos);
                }
            }
            target = pos.page;
        }
        return target;
    }

    @Override
    public final String toString() {
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

/*
        public DataType getKeyType() {
            return keyType;
        }

        public DataType getValueType() {
            return valueType;
        }
*/

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

    private static final class DataTypeExtentionWrapper implements ExtendedDataType {

        private final DataType dataType;

        private DataTypeExtentionWrapper(DataType dataType) {
            this.dataType = dataType;
        }

        @Override
        public Object createStorage(int size) {
            return new Object[size];
        }

        @Override
        public Object clone(Object storage) {
            Object data[] = (Object[])storage;
            return data.clone();
        }

        @Override
        public int getLength(Object storage) {
/*
            return Array.getLength(storage);
/*/
            Object data[] = (Object[])storage;
            return data.length;
//*/
        }

        @Override
        public Object getValue(Object storage, int indx) {
/*
            return Array.get(storage, indx);
/*/
            Object data[] = (Object[])storage;
            return data[indx];
//*/
        }

        @Override
        public void setValue(Object storage, int indx, Object value) {
/*
            Array.set(storage, indx, value);
/*/
            Object data[] = (Object[])storage;
            data[indx] = value;
//*/
        }

        @Override
        public int getMemorySize(Object storage) {
            int mem = 0;
            Object data[] = (Object[])storage;
            for (Object key : data) {
                mem += dataType.getMemory(key);
            }
            return mem;
        }

        @Override
        public int binarySearch(Object key, Object storage, int initialGuess) {
            Object keys[] = (Object[])storage;
            int low = 0, high = keys.length - 1;
            // the cached index minus one, so that
            // for the first time (when cachedCompare is 0),
            // the default value is used
            int x = initialGuess - 1;
            if (x < 0 || x > high) {
                x = high >>> 1;
            }
            while (low <= high) {
                int compare = dataType.compare(key, keys[x]);
                if (compare > 0) {
                    low = x + 1;
                } else if (compare < 0) {
                    high = x - 1;
                } else {
                    return x;
                }
                x = (low + high) >>> 1;
            }
            return -(low + 1);
        }

        @Override
        public void writeStorage(WriteBuffer buff, Object storage) {
            Object data[] = (Object[])storage;
            dataType.write(buff, data, data.length, true);
        }

        @Override
        public void read(ByteBuffer buff, Object storage) {
            Object data[] = (Object[])storage;
            dataType.read(buff, data, data.length, true);
        }

        @Override
        public int compare(Object a, Object b) {
            return dataType.compare(a, b);
        }

        @Override
        public int getMemory(Object obj) {
            return obj == null ? 0 : dataType.getMemory(obj);
        }

        @Override
        public void write(WriteBuffer buff, Object obj) {
            dataType.write(buff, obj);
        }

        @Override
        public void write(WriteBuffer buff, Object[] obj, int len, boolean key) {
            dataType.write(buff, obj, len, key);
        }

        @Override
        public Object read(ByteBuffer buff) {
            return dataType.read(buff);
        }

        @Override
        public void read(ByteBuffer buff, Object[] obj, int len, boolean key) {
            dataType.read(buff, obj, len, key);
        }
    }

    private static final class MVEntrySet<K, V> extends AbstractSet<Entry<K, V>> {

        private final MVMap<K,V>   map;

        private MVEntrySet(MVMap<K, V> map) {
            this.map = map;
        }

        @Override
        public Iterator<Entry<K, V>> iterator() {
            final Cursor<K, V> cursor = new Cursor<K, V>(map, map.root, null);
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
            return map.size();
        }

        @Override
        public boolean contains(Object o) {
            return map.containsKey(o);
        }

    }

    private static final class MVKeySet<K> extends AbstractSet<K> {

        private final MVMap<K, ?> map;

        private MVKeySet(MVMap<K, ?> map) {
            this.map = map;
        }

        @Override
        public Iterator<K> iterator() {
            return new Cursor<>(map, map.root, null);
        }

        @Override
        public int size() {
            return map.size();
        }

        @Override
        public boolean contains(Object o) {
            return map.containsKey(o);
        }
    }

    private static class EqualsDecisionMaker implements DecisionMaker {
        private final DataType dataType;
        private final Object expectedValue;
        private boolean wasAllowed;

        private EqualsDecisionMaker(DataType dataType, Object expectedValue) {
            this.dataType = dataType;
            this.expectedValue = expectedValue;
        }

        @Override
        public boolean allow(Object currentValue) {
            wasAllowed = areValuesEqual(dataType, expectedValue, currentValue);
            return wasAllowed;
        }

        public boolean wasAllowed() {
            return wasAllowed;
        }
    }
}
