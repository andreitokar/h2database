/*
 * Copyright 2004-2021 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (https://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.mvstore;

import static org.h2.engine.Constants.MEMORY_POINTER;

import java.util.AbstractList;
import java.util.AbstractMap;
import java.util.AbstractSet;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import org.h2.mvstore.type.DataType;
import org.h2.mvstore.Page.PageReference;
import org.h2.mvstore.type.ObjectDataType;
import org.h2.util.MemoryEstimator;

/**
 * A stored map.
 * <p>
 * All read and write operations can happen concurrently with all other
 * operations, without risk of corruption.
 *
 * @param <K> the key class
 * @param <V> the value class
 */
public class MVMap<K, V> extends AbstractMap<K, V> implements ConcurrentMap<K, V> {

    /**
     * The store.
     */
    public final MVStore store;

    /**
     * Reference to the current root page.
     */
    private final AtomicReference<RootReference<K,V>> root;

    private final int id;
    private final long createVersion;
    private final DataType<K> keyType;
    private final DataType<V> valueType;
    private final int keysPerPage;
    private final boolean singleWriter;
    private final K[] keysBuffer;
    private final V[] valuesBuffer;

    private final Object lock = new Object();
    private volatile boolean notificationRequested;

    /**
     * Whether the map is closed. Volatile so we don't accidentally write to a
     * closed map in multithreaded mode.
     */
    private volatile  boolean closed;
    private boolean readOnly;
    private boolean isVolatile;
    private final AtomicLong avgKeySize;
    private final AtomicLong avgValSize;

    /**
     * This designates the "last stored" version for a store which was
     * just open for the first time.
     */
    static final long INITIAL_VERSION = -1;

    @SuppressWarnings("rawtypes")
    private static final KVMapping[] DUMMY_BUFFER = new KVMapping[0];

    @SuppressWarnings("unchecked")
    private static <X,Y> KVMapping<X,Y>[] dummyBuffer() { return DUMMY_BUFFER; }

    @SuppressWarnings("unchecked")
    private static <X,Y> KVMapping<X,Y>[] createBuffer(int size) { return new KVMapping[size]; }

    protected MVMap(Map<String, Object> config, DataType<K> keyType, DataType<V> valueType) {
        this((MVStore) config.get("store"), keyType, valueType,
                DataUtils.readHexInt(config, "id", 0),
                DataUtils.readHexLong(config, "createVersion", 0),
                new AtomicReference<>(),
                ((MVStore) config.get("store")).getKeysPerPage(),
                config.containsKey("singleWriter") && (Boolean) config.get("singleWriter")
        );
        setInitialRoot(createEmptyLeaf(), store.getCurrentVersion());
    }

    // constructor for cloneIt()
    @SuppressWarnings("CopyConstructorMissesField")
    protected MVMap(MVMap<K, V> source) {
        this(source.store, source.keyType, source.valueType, source.id, source.createVersion,
                new AtomicReference<>(source.root.get()), source.keysPerPage, source.singleWriter);
    }

    // meta map constructor
    MVMap(MVStore store, int id, DataType<K> keyType, DataType<V> valueType) {
        this(store, keyType, valueType, id, 0, new AtomicReference<>(), store.getKeysPerPage(), false);
        setInitialRoot(createEmptyLeaf(), store.getCurrentVersion());
    }

    private MVMap(MVStore store, DataType<K> keyType, DataType<V> valueType, int id, long createVersion,
            AtomicReference<RootReference<K,V>> root, int keysPerPage, boolean singleWriter) {
        this.store = store;
        this.id = id;
        this.createVersion = createVersion;
        this.keyType = keyType;
        this.valueType = valueType;
        this.root = root;
        this.keysPerPage = keysPerPage;
        this.keysBuffer = singleWriter ? keyType.createStorage(keysPerPage) : null;
        this.valuesBuffer = singleWriter ? valueType.createStorage(keysPerPage) : null;
        this.singleWriter = singleWriter;
        this.avgKeySize = keyType.isMemoryEstimationAllowed() ? new AtomicLong() : null;
        this.avgValSize = valueType.isMemoryEstimationAllowed() ? new AtomicLong() : null;

    }

    /**
     * Clone the current map.
     *
     * @return clone of this.
     */
    protected MVMap<K, V> cloneIt() {
        return new MVMap<>(this);
    }

    /**
     * Get the metadata key for the root of the given map id.
     *
     * @param mapId the map id
     * @return the metadata key
     */
    static String getMapRootKey(int mapId) {
        return DataUtils.META_ROOT + Integer.toHexString(mapId);
    }

    /**
     * Get the metadata key for the given map id.
     *
     * @param mapId the map id
     * @return the metadata key
     */
    static String getMapKey(int mapId) {
        return DataUtils.META_MAP + Integer.toHexString(mapId);
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
        DataUtils.checkArgument(value != null, "The value may not be null");
        return operate(key, value, DecisionMaker.PUT);
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
    public final K getKey(long index) {
        if (index < 0 || index >= sizeAsLong()) {
            return null;
        }
        Page<K,V> p = getRootPage();
        long offset = 0;
        while (true) {
            if (p.isLeaf()) {
                if (index >= offset + p.getKeyCount()) {
                    return null;
                }
                K key = p.getKey((int) (index - offset));
                return key;
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
        Page<K,V> p = getRootPage();
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
    private K getFirstLast(boolean first) {
        Page<K,V> p = getRootPage();
        return getFirstLast(p, first);
    }

    private K getFirstLast(Page<K,V> p, boolean first) {
        if (p.getTotalCount() == 0) {
            return null;
        }
        while (true) {
            if (p.isLeaf()) {
                return p.getKey(first ? 0 : p.getKeyCount() - 1);
            }
            p = p.getChildPage(first ? 0 : getChildPageCount(p) - 1);
        }
    }

    /**
     * Get the smallest key that is larger than the given key (next key in ascending order),
     * or null if no such key exists.
     *
     * @param key the key
     * @return the result
     */
    public final K higherKey(K key) {
        return getMinMax(key, false, true);
    }

    /**
     * Get the smallest key that is larger than the given key, for the given
     * root page, or null if no such key exists.
     *
     * @param rootRef the root reference of the map
     * @param key to start from
     * @return the result
     */
    public final K higherKey(RootReference<K,V> rootRef, K key) {
        return getMinMax(rootRef, key, false, true);
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
     * Get the largest key that is smaller than the given key, for the given
     * root page, or null if no such key exists.
     *
     * @param rootRef the root page
     * @param key the key
     * @return the result
     */
    public final K lowerKey(RootReference<K, V> rootRef, K key) {
        return getMinMax(rootRef, key, true, true);
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
        return getMinMax(flushAndGetRoot(), key, min, excluding);
    }

    private K getMinMax(RootReference<K,V> rootRef, K key, boolean min, boolean excluding) {
        if (min && key == null) {
            return null;
        }
        if (rootRef.buffer == null && !rootRef.needFlush()) {
            return getMinMax(rootRef.root, key, min, excluding);
        }
        Cursor<K, V> cursor = cursor(rootRef, key, null, min);
        while (cursor.hasNext()) {
            K next = cursor.next();
            if (!excluding || compare(key, next) != 0) {
                return next;
            }
        }
        return null;
    }

    private K getMinMax(Page<K,V> p, K key, boolean min, boolean excluding) {
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
            return p.getKey(x);
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
     * Get the value for the given key, or null if not found.
     *
     * @param key the key
     * @return the value, or null if not found
     * @throws ClassCastException if type of the specified key is not compatible with this map
     */
    @SuppressWarnings("unchecked")
    @Override
    public final V get(Object key) {
        return getIt((K)key);
    }

    public final V getIt(K key) {
        return get(getRoot(), key);
    }

    public final V get(RootReference<K,V> rootReference, K key) {
        Page<K,V> rootPage = rootReference.root;
        KVMapping<K,V>[] buffer = rootReference.buffer;
        if (buffer != null) {
            int index = binarySearch(buffer, key, 0);
            if (index >= 0) {
                return buffer[index].value;
            }
        }
        return get(rootPage, key);
    }

    int binarySearch(KVMapping<K,V>[] buffer, K key, int low) {
        return binarySearch(buffer, key, low, buffer.length - 1);
    }

    int binarySearch(KVMapping<K,V>[] buffer, K key, int low, int high) {
        assert buffer.length > 0;
        int compare = 0;
        if (low == 0) {
            compare = compare(key, buffer[0].key);
            if (compare < 0) {
                return -1;
            } else if (compare == 0) {
                return 0;
            }
            low = 1;
        }

        if (high > 0) {
            compare = compare(key, buffer[high].key);
        }
        if (compare > 0) {
            return -high - 2;
        } else if (compare == 0) {
            return high;
        }

        return _binarySearch(buffer, key, low, high);
    }

    private int _binarySearch(KVMapping<K,V>[] buffer, K key, int low, int high) {
        while (low <= high) {
            int x = (low + high) >>> 1;
            int compare = compare(key, buffer[x].key);
            if (compare > 0) {
                low = x + 1;
            } else if (compare < 0) {
                high = x - 1;
            } else {
                return x;
            }
        }
        return ~low;
    }

    /**
     * Get the value for the given key from a snapshot, or null if not found.
     *
     * @param p the root of a snapshot
     * @param key the key
     * @return the value, or null if not found
     * @throws ClassCastException if type of the specified key is not compatible with this map
     */
    public V get(Page<K,V> p, K key) {
        return Page.get(p, key);
    }

    @Override
    public final boolean containsKey(Object key) {
        return get(key) != null;
    }

    /**
     * Remove all entries.
     */
    @Override
    public void clear() {
        clearIt();
    }

    /**
     * Remove all entries and return the root reference.
     *
     * @return the new root reference
     */
    RootReference<K,V> clearIt() {
        Page<K,V> emptyRootPage = createEmptyLeaf();
        int attempt = 0;
        while (true) {
            RootReference<K,V> rootReference = flushAndGetRoot();
            if (rootReference.getTotalCount() == 0) {
                return rootReference;
            }
            boolean locked = rootReference.isLockedByCurrentThread();
            if (!locked) {
                if (attempt++ == 0 && beforeWrite()) {
                    continue;
                } else if (attempt > 3 || rootReference.isLocked()) {
                    rootReference = lockRoot(rootReference, attempt);
                    locked = true;
                }
            }
            Page<K,V> rootPage = rootReference.root;
            long version = rootReference.version;
            try {
                if (!locked) {
                    rootReference = rootReference.updateRootPage(emptyRootPage, attempt);
                    if (rootReference == null) {
                        continue;
                    }
                }
                store.registerUnsavedMemory(rootPage.removeAllRecursive(version));
                rootPage = emptyRootPage;
                return rootReference;
            } finally {
                if(locked) {
                    unlockRoot(rootPage);
                }
            }
        }
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
     * @throws ClassCastException if type of the specified key is not compatible with this map
     */
    @Override
    @SuppressWarnings("unchecked")
    public V remove(Object key) {
        return operate((K)key, null, DecisionMaker.REMOVE);
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
        return operate(key, value, DecisionMaker.IF_ABSENT);
    }

    /**
     * Remove a key-value pair if the value matches the stored one.
     *
     * @param key the key (may not be null)
     * @param value the expected value
     * @return true if the item was removed
     */
    @SuppressWarnings("unchecked")
    @Override
    public boolean remove(Object key, Object value) {
        EqualsDecisionMaker<V> decisionMaker = new EqualsDecisionMaker<>(valueType, (V)value);
        operate((K)key, null, decisionMaker);
        return decisionMaker.getDecision() != Decision.ABORT;
    }

    /**
     * Check whether the two values are equal.
     *
     * @param a the first value
     * @param b the second value
     * @param datatype to use for comparison
     * @return true if they are equal
     */
    static <X> boolean areValuesEqual(DataType<X> datatype, X a, X b) {
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
        EqualsDecisionMaker<V> decisionMaker = new EqualsDecisionMaker<>(valueType, oldValue);
        V result = operate(key, newValue, decisionMaker);
        boolean res = decisionMaker.getDecision() != Decision.ABORT;
        assert !res || areValuesEqual(valueType, oldValue, result) : oldValue + " != " + result;
        return res;
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
        return operate(key, value, DecisionMaker.IF_PRESENT);
    }

    /**
     * Compare two keys.
     *
     * @param a the first key
     * @param b the second key
     * @return -1 if the first key is smaller, 1 if bigger, 0 if equal
     */
    @SuppressWarnings("unused")
    final int compare(K a, K b) {
        return keyType.compare(a, b);
    }

    /**
     * Get the key type.
     *
     * @return the key type
     */
    public final DataType<K> getKeyType() {
        return keyType;
    }

    /**
     * Get the value type.
     *
     * @return the value type
     */
    public final DataType<V> getValueType() {
        return valueType;
    }

    boolean isSingleWriter() {
        return singleWriter;
    }

    /**
     * Read a page.
     *
     * @param pos the position of the page
     * @return the page
     */
    final Page<K,V> readPage(long pos) {
        return store.readPage(this, pos);
    }

    /**
     * Set the position of the root page.
     * @param rootPos the position, 0 for empty
     * @param version to set for this map
     *
     */
    final void setRootPos(long rootPos, long version) {
        Page<K,V> root = readOrCreateRootPage(rootPos);
        setInitialRoot(root, version);
        setWriteVersion(store.getCurrentVersion());
    }

    private Page<K,V> readOrCreateRootPage(long rootPos) {
        Page<K,V> root = rootPos == 0 ? createEmptyLeaf() : readPage(rootPos);
        return root;
    }

    /**
     * Iterate over a number of keys.
     *
     * @param from the first key to return
     * @return the iterator
     */
    public final Iterator<K> keyIterator(K from) {
        return cursor(from, null, false);
    }

    /**
     * Iterate over a number of keys in reverse order
     *
     * @param from the first key to return
     * @return the iterator
     */
    public final Iterator<K> keyIteratorReverse(K from) {
        return cursor(from, null, true);
    }

    final boolean rewritePage(long pagePos) {
        Page<K, V> p = readPage(pagePos);
        if (p.getKeyCount()==0) {
            return true;
        }
        assert p.isSaved();
        K key = p.getKey(0);
        if (!isClosed()) {
            RewriteDecisionMaker<V> decisionMaker = new RewriteDecisionMaker<>(p.getPos());
            V result = operate(key, null, decisionMaker);
            boolean res = decisionMaker.getDecision() != Decision.ABORT;
            assert !res || result != null;
            return res;
        }
        return false;
    }

    /**
     * Get a cursor to iterate over a number of keys and values in the latest version of this map.
     *
     * @param from the first key to return
     * @return the cursor
     */
    public final Cursor<K, V> cursor(K from) {
        return cursor(from, null, false);
    }

    /**
     * Get a cursor to iterate over a number of keys and values in the latest version of this map.
     *
     * @param from the first key to return
     * @param to the last key to return
     * @param reverse if true, iterate in reverse (descending) order
     * @return the cursor
     */
    public final Cursor<K, V> cursor(K from, K to, boolean reverse) {
        return cursor(flushAppendBufferAndGetRoot(), from, to, reverse);
    }

    /**
     * Get a cursor to iterate over a number of keys and values.
     *
     * @param rootReference of this map's version to iterate over
     * @param from the first key to return
     * @param to the last key to return
     * @param reverse if true, iterate in reverse (descending) order
     * @return the cursor
     */
    public Cursor<K, V> cursor(RootReference<K,V> rootReference, K from, K to, boolean reverse) {
        if (singleWriter || rootReference.buffer == null || this == store.getMetaMap()) {
            return new Cursor<>(rootReference, from, to, reverse);
        } else {
            return new CursorBuffered<>(rootReference, from, to, reverse);
        }
    }

    @Override
    public final Set<Map.Entry<K, V>> entrySet() {
        final RootReference<K,V> rootReference = flushAppendBufferAndGetRoot();
        return new AbstractSet<Entry<K, V>>() {

            @Override
            public Iterator<Entry<K, V>> iterator() {
                final Cursor<K, V> cursor = cursor(rootReference, null, null, false);
                return new Iterator<Entry<K, V>>() {

                    @Override
                    public boolean hasNext() {
                        return cursor.hasNext();
                    }

                    @Override
                    public Entry<K, V> next() {
                        K k = cursor.next();
                        return new SimpleImmutableEntry<>(k, cursor.getValue());
                    }
                };

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

    @Override
    public Set<K> keySet() {
        final RootReference<K,V> rootReference = flushAppendBufferAndGetRoot();
        return new AbstractSet<K>() {

            @Override
            public Iterator<K> iterator() {
                return cursor(rootReference, null, null, false);
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

    protected final boolean isPersistent() {
        return store.getFileStore() != null && !isVolatile;
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
    public final Page<K,V> getRootPage() {
        return flushAndGetRoot().root;
    }

    public RootReference<K,V> getRoot() {
        return root.get();
    }

    /**
     * Get the root reference, flushing any current append buffer.
     *
     * @return current root reference
     */
    public RootReference<K,V> flushAndGetRoot() {
        RootReference<K,V> rootReference = getRoot();
        if (!singleWriter) {
            if (rootReference.buffer != null) {
                return flushBuffer(rootReference, 1);
            }
        } else if (rootReference.needFlush()) {
            return flushAppendBuffer(rootReference, true);
        }
        return rootReference;
    }

    public RootReference<K,V> flushAppendBufferAndGetRoot() {
        RootReference<K,V> rootReference = getRoot();
        if (rootReference.needFlush()) {
            return flushAppendBuffer(rootReference, true);
        }
        return rootReference;
    }

    /**
     * Set the initial root.
     *
     * @param rootPage root page
     * @param version initial version
     */
    final void setInitialRoot(Page<K,V> rootPage, long version) {
        root.set(new RootReference<>(rootPage, version));
    }

    /**
     * Compare and set the root reference.
     *
     * @param expectedRootReference the old (expected)
     * @param updatedRootReference the new
     * @return whether updating worked
     */
    final boolean compareAndSetRoot(RootReference<K,V> expectedRootReference,
                                    RootReference<K,V> updatedRootReference) {
        return root.compareAndSet(expectedRootReference, updatedRootReference);
    }

    /**
     * Rollback to the given version.
     *
     * @param version the version
     */
    final void rollbackTo(long version) {
        // check if the map was removed and re-created later ?
        if (version > createVersion) {
            rollbackRoot(version);
        }
    }

    /**
     * Roll the root back to the specified version.
     *
     * @param version to rollback to
     * @return true if rollback was a success, false if there was not enough in-memory history
     */
    boolean rollbackRoot(long version) {
        RootReference<K,V> rootReference = flushAppendBufferAndGetRoot();
        RootReference<K,V> previous;
        while (rootReference.version >= version && (previous = rootReference.previous) != null) {
            if (root.compareAndSet(rootReference, previous)) {
                rootReference = previous;
                closed = false;
            }
        }
        setWriteVersion(version);
        return rootReference.version < version;
    }

    /**
     * Use the new root page from now on.
     *
     * @param <K> the key class
     * @param <V> the value class
     * @param expectedRootReference expected current root reference
     * @param newRootPage the new root page
     * @param attemptUpdateCounter how many attempt (including current)
     *                             were made to update root
     * @return new RootReference or null if update failed
     */
    protected static <K,V> boolean updateRoot(RootReference<K,V> expectedRootReference, Page<K,V> newRootPage,
            int attemptUpdateCounter) {
        return expectedRootReference.updateRootPage(newRootPage, attemptUpdateCounter) != null;
    }

    /**
     * Forget those old versions that are no longer needed.
     * @param rootReference to inspect
     */
    private void removeUnusedOldVersions(RootReference<K,V> rootReference) {
        rootReference.removeUnusedOldVersions(store.getOldestVersionToKeep());
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
    protected final boolean beforeWrite() {
        assert !getRoot().isLockedByCurrentThread() : getRoot();
        if (closed) {
            int id = getId();
            String mapName = store.getMapName(id);
            throw DataUtils.newMVStoreException(
                    DataUtils.ERROR_CLOSED, "Map {0}({1}) is closed. {2}", mapName, id, store.getPanicException());
        }
        if (!isPersistent()) {
            return false;
        }
        if (readOnly) {
            throw DataUtils.newUnsupportedOperationException(
                    "This map is read-only");
        }
        return store.beforeWrite(this);
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
     * Get the number of entries, as a integer. {@link Integer#MAX_VALUE} is
     * returned if there are more than this entries.
     *
     * @return the number of entries, as an integer
     * @see #sizeAsLong()
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
        return getRoot().getTotalCount();
    }

    @Override
    public boolean isEmpty() {
        return sizeAsLong() == 0;
    }

    final long getCreateVersion() {
        return createVersion;
    }

    /**
     * Open an old version for the given map.
     * It will restore map at last known state of the version specified.
     * (at the point right before the commit() call, which advanced map to the next version)
     * Map is opened in read-only mode.
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
        RootReference<K,V> rootReference = flushAndGetRoot();
        removeUnusedOldVersions(rootReference);
        RootReference<K,V> previous;
        while ((previous = rootReference.previous) != null && previous.version >= version) {
            rootReference = previous;
        }
        if (previous == null && version < store.getOldestVersionToKeep()) {
            throw DataUtils.newIllegalArgumentException("Unknown version {0}", version);
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
        Page<K,V> root = readOrCreateRootPage(rootPos);
        return openReadOnly(root, version);
    }

    private MVMap<K, V> openReadOnly(Page<K,V> root, long version) {
        MVMap<K, V> m = cloneIt();
        m.readOnly = true;
        m.setInitialRoot(root, version);
        return m;
    }

    /**
     * Get version of the map, which is the version of the store,
     * at the moment when map was modified last time.
     *
     * @return version
     */
    public final long getVersion() {
        return getRoot().getVersion();
    }

    /**
     * Does the root have changes since the specified version?
     *
     * @param version root version
     * @return true if has changes
     */
    final boolean hasChangesSince(long version) {
        return getRoot().hasChangesSince(version, isPersistent());
    }

    /**
     * Get the child page count for this page. This is to allow another map
     * implementation to override the default, in case the last child is not to
     * be used.
     *
     * @param p the page
     * @return the number of direct children
     */
    protected int getChildPageCount(Page<K,V> p) {
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
    protected String asString(String name) {
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

    final RootReference<K,V> setWriteVersion(long writeVersion) {
        int attempt = 0;
        while(true) {
            RootReference<K,V> rootReference = flushAndGetRoot();
            if(rootReference.version >= writeVersion) {
                return rootReference;
            } else if (isClosed()) {
                // map was closed a while back and can not possibly be in use by now
                // it's time to remove it completely from the store (it was anonymous already)
                if (rootReference.getVersion() + 1 < store.getOldestVersionToKeep()) {
                    store.deregisterMapRoot(id);
                    return null;
                }
            }

            RootReference<K,V> lockedRootReference = null;
            if (++attempt > 3 || rootReference.isLocked()) {
                lockedRootReference = lockRoot(rootReference, attempt);
                rootReference = flushAndGetRoot();
            }

            try {
                rootReference = rootReference.tryUnlockAndUpdateVersion(writeVersion, attempt);
                if (rootReference != null) {
                    lockedRootReference = null;
                    removeUnusedOldVersions(rootReference);
                    return rootReference;
                }
            } finally {
                if (lockedRootReference != null) {
                    unlockRoot();
                }
            }
        }
    }

    /**
     * Create empty leaf node page.
     *
     * @return new page
     */
    protected Page<K,V> createEmptyLeaf() {
        return Page.createEmptyLeaf(this);
    }

    /**
     * Create empty internal node page.
     *
     * @return new page
     */
    protected Page<K,V> createEmptyNode() {
        return Page.createEmptyNode(this);
    }

    /**
     * Copy a map. All pages are copied.
     *
     * @param sourceMap the source map
     */
    final void copyFrom(MVMap<K, V> sourceMap) {
        MVStore.TxCounter txCounter = store.registerVersionUsage();
        try {
            beforeWrite();
            copy(sourceMap.getRootPage(), null, 0);
        } finally {
            store.deregisterVersionUsage(txCounter);
        }
    }

    private void copy(Page<K,V> source, Page<K,V> parent, int index) {
        Page<K,V> target = source.copy(this);
        if (parent == null) {
            setInitialRoot(target, INITIAL_VERSION);
        } else {
            parent.setChild(index, target, true);
        }
        if (!source.isLeaf()) {
            for (int i = 0; i < getChildPageCount(target); i++) {
                if (source.getChildPagePos(i) != 0) {
                    // position 0 means no child
                    // (for example the last entry of an r-tree node)
                    // (the MVMap is also used for r-trees for compacting)
                    copy(source.getChildPage(i), target, i);
                }
            }
            target.setComplete();
        }
        store.registerUnsavedMemory(target.getMemory());
        if (store.isSaveNeeded()) {
            store.commit();
        }
    }

    /**
     * If map was used in append mode, this method will ensure that append buffer
     * is flushed - emptied with all entries inserted into map as a new leaf.
     * @param rootReference current RootReference
     * @param fullFlush whether buffer should be completely flushed,
     *                 otherwise just a single empty slot is required
     * @return potentially updated RootReference
     */
    private RootReference<K,V> flushAppendBuffer(RootReference<K,V> rootReference, boolean fullFlush) {
        boolean preLocked = rootReference.isLockedByCurrentThread();
        boolean locked = preLocked;
        int keysPerPage = store.getKeysPerPage();
        try {
            IntValueHolder unsavedMemoryHolder = new IntValueHolder();
            int attempt = 0;
            int keyCount;
            int availabilityThreshold = fullFlush ? 0 : keysPerPage - 1;
            while ((keyCount = rootReference.getAppendCounter()) > availabilityThreshold) {
                if (!locked) {
                    // instead of just calling lockRoot() we loop here and check if someone else
                    // already flushed the buffer, then we don't need a lock
                    rootReference = tryLock(rootReference, ++attempt);
                    if (rootReference == null) {
                        rootReference = getRoot();
                        continue;
                    }
                    locked = true;
                }
                assert rootReference.buffer == null;
                Page<K,V> rootPage = rootReference.root;
                long version = rootReference.version;
                CursorPos<K,V> pos = rootPage.getAppendCursorPos(null);
                assert pos != null;
                assert pos.index < 0 : pos.index;
                int index = -pos.index - 1;
                assert index == pos.page.getKeyCount() : index + " != " + pos.page.getKeyCount();
                Page<K,V> p = pos.page;
                CursorPos<K,V> tip = pos;
                pos = pos.parent;

                int remainingBuffer = 0;
                Page<K,V> page = null;
                int available = keysPerPage - p.getKeyCount();
                if (available > 0) {
                    p = p.copy();
                    if (keyCount <= available) {
                        p.expand(keyCount, keysBuffer, valuesBuffer);
                    } else {
                        p.expand(available, keysBuffer, valuesBuffer);
                        keyCount -= available;
                        if (fullFlush) {
                            K[] keys = createKeyStorage(keyCount);
                            V[] values = createValueStorage(keyCount);
                            System.arraycopy(keysBuffer, available, keys, 0, keyCount);
                            System.arraycopy(valuesBuffer, available, values, 0, keyCount);
                            page = Page.createLeaf(this, keys, values, 0);
                        } else {
                            System.arraycopy(keysBuffer, available, keysBuffer, 0, keyCount);
                            System.arraycopy(valuesBuffer, available, valuesBuffer, 0, keyCount);
                            remainingBuffer = keyCount;
                        }
                    }
                } else {
                    tip = tip.parent;
                    page = Page.createLeaf(this,
                            Arrays.copyOf(keysBuffer, keyCount),
                            Arrays.copyOf(valuesBuffer, keyCount),
                            0);
                }

                unsavedMemoryHolder.value = 0;
                if (page != null) {
                    assert page.map == this;
                    assert page.getKeyCount() > 0;
                    K key = page.getKey(0);
                    unsavedMemoryHolder.value += page.getMemory();
                    while (true) {
                        if (pos == null) {
                            if (p.getKeyCount() == 0) {
                                p = page;
                            } else {
                                p = createParentPage(key, p, page, p.getTotalCount() + page.getTotalCount());
                            }
                            break;
                        }
                        p = insertPageAfter(pos, p, key, page);
                        pos = pos.parent;
                        keyCount = p.getKeyCount();
                        int at = keyCount - (p.isLeaf() ? 1 : 2);
                        if (keyCount <= keysPerPage &&
                                (p.getMemory() < store.getMaxPageSize() || at <= 0)) {
                            break;
                        }
                        key = p.getKey(at);
                        page = p.split(at);
                        unsavedMemoryHolder.value += p.getMemory() + page.getMemory();
                    }
                }
                p = replacePage(pos, p, unsavedMemoryHolder, null);
                rootReference = rootReference.updatePageAndLockedStatus(p, preLocked || isPersistent(),
                        remainingBuffer, null);
                if (rootReference != null) {
                    // should always be the case, except for spurious failure?
                    locked = preLocked || isPersistent();
                    if (isPersistent() && tip != null) {
                        store.registerUnsavedMemory(unsavedMemoryHolder.value + tip.processRemovalInfo(version));
                    }
                    assert rootReference.getAppendCounter() <= availabilityThreshold;
                    break;
                }
                rootReference = getRoot();
            }
        } finally {
            if (locked && !preLocked) {
                rootReference = unlockRoot();
            }
        }
        return rootReference;
    }

    private RootReference<K,V> flushBuffer(RootReference<K,V> rootReference, int threshold) {
        boolean preLocked = rootReference.isLockedByCurrentThread();
        boolean locked = preLocked;
        IntValueHolder unsavedMemoryHolder = new IntValueHolder();
        try {
            int attempt = 0;
            HashMap<Page<K,V>, Page<K,V>> pageMap = null;
            KVMapping<K,V>[] buffer;
            int bufferLength;
mainLoop:
            while ((buffer = rootReference.buffer) != null && (bufferLength = buffer.length) >= threshold) {
                if (!locked && ++attempt > 3) {
                    // instead of just calling lockRoot() we loop here and check if someone else
                    // already flushed the buffer, then we don't need a lock
                    rootReference = tryLock(rootReference, attempt);
                    if (rootReference == null) {
                        rootReference = getRoot();
                        continue;
                    }
                    locked = true;
                }
                assert rootReference.getAppendCounter() == 0;
                long version = rootReference.version;
                if (pageMap == null) {
                    pageMap = new HashMap<>();
                } else {
                    pageMap.clear();
                }
                Page<K,V> rootPage = rootReference.root;
                int from = 0;
                while (from < bufferLength) {
                    KVMapping<K,V> kvMapping = buffer[from];
                    K key = kvMapping.key;
                    CursorPos<K,V> path = CursorPos.traverseDown(rootPage, key);

                    RootReference<K,V> current;
                    if (!locked && (current = getRoot()) != rootReference) {
                        rootReference = current;
                        continue mainLoop;
                    }

                    Page<K,V> leaf = path.page;
                    K ceilingKey = getCeilingKey(path);
                    int to = calcUpperBound(buffer, from, ceilingKey);
                    int totalKeyCount = calcTotalKeyCount(buffer, from, leaf, to);
                    path = path.parent;
                    assert totalKeyCount >= 0;
                    if (totalKeyCount == 0) {
                        rootPage = path == null ? createEmptyLeaf() : removePage(path, unsavedMemoryHolder, pageMap);
                        registerReplacementPage(pageMap, leaf, rootPage);
                        from = to;
                        break;
                    } else {
                        int keyPerPageLimit = calculateKeyPerPageLimit(leaf);
                        int pageCount = (totalKeyCount + keyPerPageLimit - 1) / keyPerPageLimit;
                        Page<K,V>[] pages = pageCount > 1 ? createPageArray(pageCount) : null;

                        Page<K,V> replacementPage = createReplacementPages(leaf, buffer, from, to, totalKeyCount, pageCount, pages);
                        from = to;
                        if (pages != null) {
                            if (path == null) {
                                rootPage = createParentPage(pages, unsavedMemoryHolder);
                                registerReplacementPage(pageMap, leaf, rootPage);
                                unsavedMemoryHolder.value += rootPage.getMemory();
                            } else {
                                Page<K, V> parentPage = path.page;
                                int insertIndex = path.index;
                                path = path.parent;
                                boolean isOriginal = (replacementPage = findReplacement(pageMap, parentPage)) != parentPage;
                                K splitKey = null;
                                for (int i = pages.length - 1; i >= 0; i--) {
                                    Page<K, V> childPage = pages[i];
                                    if (splitKey == null) {
                                        replacementPage.setChild(insertIndex, childPage, isOriginal);
                                    } else {
                                        replacementPage.insertNode(insertIndex, splitKey, childPage);
                                    }
                                    splitKey = childPage.getKey(0);
                                    unsavedMemoryHolder.value += childPage.getMemory();
                                }
                                registerReplacementPage(pageMap, leaf, replacementPage);
                                rootPage = insertPage(path, replacementPage, unsavedMemoryHolder, pageMap);
                            }
                            break;
                        } else {
                            registerReplacementPage(pageMap, leaf, replacementPage);
                            rootPage = replacePage(path, replacementPage, unsavedMemoryHolder, pageMap);
                        }
                    }
                }

                rootReference = rootReference.updatePageAndLockedStatus(rootPage, preLocked, 0,
                                                                        createLeftoverBuffer(buffer, from));
                if (rootReference != null) {
                    locked = preLocked;
                    if (isPersistent()) {
                        pageMap.forEach((page, replacement) -> {
                            if (page != replacement) {
                                unsavedMemoryHolder.value += page.removePage(version);
                            }
                        });
                        store.registerUnsavedMemory(unsavedMemoryHolder.value);
                    }
                } else {
                    rootReference = getRoot();
                }
            }
        } finally {
            if (locked && !preLocked) {
                rootReference = unlockRoot();
            }
        }
        return rootReference;
    }


    private Page<K,V> removePage(CursorPos<K,V> parentPath, IntValueHolder unsavedMemoryHolder,
                                 Map<Page<K,V>,Page<K,V>> pageMap) {
        Page<K,V> page;
        int keyCount;
        int index;
        do {
            page = parentPath.page;
            index = parentPath.index;
            parentPath = parentPath.parent;
            keyCount = page.getKeyCount();
            // condition below should always be false, but older
            // versions (up to 1.4.197) may create
            // single-childed (with no keys) internal nodes,
            // which we skip here
        } while (keyCount == 0 && parentPath != null);

        Page<K,V> replacement = findReplacement(pageMap, page);
        if (keyCount > 1) {
            replacement.remove(index);
        } else if (keyCount == 1) {
            assert index <= 1;
            replacement = replacement.getChildPage(1 - index);
        } else {
            // if root happens to be such single-childed
            // (with no keys) internal node, then just
            // replace it with empty leaf
            return Page.createEmptyLeaf(this);
        }
        return replacePage(parentPath, replacement, unsavedMemoryHolder, pageMap);
    }

    private Page<K,V> insertPage(CursorPos<K,V> path, Page<K,V> page, IntValueHolder unsavedMemoryHolder,
                                    Map<Page<K,V>,Page<K,V>> pageMap) {
        int unsavedMemory = 0;
        while (shouldBeSplitted(page)) {
            int keyCount = page.getKeyCount();
            int at = keyCount >> 1;
            long totalCount = page.getTotalCount();
            K splitKey = page.getKey(at);
            Page<K,V> splitted = page;
            Page<K,V> splitoff = splitted.split(at);
//            assert !shouldBeSplitted(splitted);
//            assert !shouldBeSplitted(splitoff);
            unsavedMemory += page.getMemory() + splitoff.getMemory();
            if (path == null) {
                page = createParentPage(splitKey, splitted, splitoff, totalCount);
                break;
            }

            page = path.page;
            int index = path.index;
            path = path.parent;
            Page<K,V> replacement;
            boolean isOriginal = (replacement = findReplacement(pageMap, page)) != page;
            if (isOriginal) {
                unsavedMemory += replacement.getMemory();
            }
            replacement.setChild(index, splitoff, isOriginal);
            replacement.insertNode(index, splitKey, splitted);
            page = replacement;
        }
        unsavedMemoryHolder.value += unsavedMemory;
        return replacePage(path, page, unsavedMemoryHolder, pageMap);
    }

    private static <K,V> Page<K,V> replacePage(CursorPos<K,V> path, Page<K,V> replacement, IntValueHolder unsavedMemoryHolder,
                                    Map<Page<K,V>,Page<K,V>> pageMap) {
        int unsavedMemory = replacement.isSaved() ? 0 : replacement.getMemory();
        while (path != null) {
            Page<K,V> parent = path.page;
            assert !parent.isLeaf();
            // condition below should always be true, but older versions (up to 1.4.197)
            // may create single-childed (with no keys) internal nodes, which we skip here
            if (parent.getKeyCount() > 0) {
                Page<K,V> child = replacement;
                boolean original = (replacement = findReplacement(pageMap, parent)) != parent;
                if (original) {
                    unsavedMemory += replacement.getMemory();
                }
                replacement.setChild(path.index, child, original);
            }
            path = path.parent;
        }
        unsavedMemoryHolder.value += unsavedMemory;
        return replacement;
    }

    private int calcUpperBound(KVMapping<K, V>[] buffer, int from, K ceilingKey) {
        int to = buffer.length;
        if (ceilingKey != null) {
            to = binarySearch(buffer, ceilingKey, from);
            if (to < 0) {
                to = ~to;
            }
            assert to > from;
        }
        return to;
    }

    private int calcTotalKeyCount(KVMapping<K, V>[] buffer, int from, Page<K, V> leaf, int to) {
        KVMapping<K, V> kvMapping;
        int totalKeyCount = leaf.getKeyCount();
        for (int i = from; i < to; i++) {
            kvMapping = buffer[i];
//                        int ind = leaf.binarySearch(kvMapping.key);
//                        assert ind == kvMapping.index : ind + " != " + kvMapping.index;
            if (kvMapping.index < 0) {
                ++totalKeyCount;
            } else if (kvMapping.value == null) {
                --totalKeyCount;
            }
        }
        return totalKeyCount;
    }

    @SuppressWarnings("unchecked")
    private static <K,V> Page<K, V>[] createPageArray(int pageCount) {
        return (Page<K,V>[])new Page[pageCount];
    }

    private int calculateKeyPerPageLimit(Page<K, V> leaf) {
        int keyCountLimit = store.getKeysPerPage();
        if (isPersistent()) {
            int keyCount = leaf.getKeyCount();
            if (keyCount > 0) {
                long maxPageSize = store.getMaxPageSize();
                if (maxPageSize < Long.MAX_VALUE) {
                    int leafMemory = leaf.getMemory();
                    assert leafMemory > 0;
                    int estimate = (int) (((maxPageSize - 1) * keyCount + leafMemory) / leafMemory);
                    if (keyCountLimit > estimate) {
                        keyCountLimit = estimate;
                    }
                    assert keyCountLimit > 0;
                }
            }
        }
        assert keyCountLimit > 0;
        return keyCountLimit;
    }

    private Page<K, V> createReplacementPages(Page<K, V> leaf, KVMapping<K, V>[] buffer, int from, int to, int totalKeyCount, int pageCount, Page<K, V>[] pages) {
        int pageSize = (totalKeyCount + pageCount - 1) / pageCount;
        int bumpIndex = totalKeyCount  + pageCount - pageSize * pageCount;
        int nextPageIndex = 0;

        Page<K, V> updatedPage;
        int src = 0;
        K[] keyStorage = createKeyStorage(pageSize);
        V[] valueStorage = createValueStorage(pageSize);
        int dst = 0;
        KVMapping<K, V> kvMapping = buffer[from];
        int srcTo = kvMapping.index;
        boolean isInsertFromBuffer = srcTo < 0; // as oppose to replace
        if (isInsertFromBuffer) {
            srcTo = ~srcTo;
        }
        while (true) {
            if (dst == pageSize) {
                updatedPage = Page.createLeaf(this, keyStorage, valueStorage, 0);
                if (pages != null) {
                    pages[nextPageIndex++] = updatedPage;
                    if (nextPageIndex == pages.length) {
                        break;
                    }
                    if (nextPageIndex == bumpIndex) {
                        --pageSize;
                    }
                    keyStorage = createKeyStorage(pageSize);
                    valueStorage = createValueStorage(pageSize);
                    dst = 0;
                } else {
                    break;
                }
            } else if (src == srcTo) {
                V value = kvMapping.value;
                if (isInsertFromBuffer) {
                    keyStorage[dst] = kvMapping.key;
                    valueStorage[dst++] = value;
                } else {
                    assert compare(leaf.getKey(src), kvMapping.key) == 0 : leaf.getKey(src) + " != " + kvMapping.key;
                    if (value != null) {     // update (otherwise removal)
                        keyStorage[dst] = leaf.getKey(src);
                        valueStorage[dst++] = value;
                    }
                    src++;
                }
                if (++from < to) {
                    kvMapping = buffer[from];
                    srcTo = kvMapping.index;
                    isInsertFromBuffer = srcTo < 0;
                    if (isInsertFromBuffer) {
                        srcTo = ~srcTo;
                    }
                } else {
                    srcTo = Integer.MAX_VALUE;
                }
            } else {     // no change, just copy
                keyStorage[dst] = leaf.getKey(src);
                valueStorage[dst++] = leaf.getValue(src++);
            }
        }
        return updatedPage;
    }

    private static <K,V> Page<K,V> findReplacement(Map<Page<K, V>, Page<K, V>> pageMap, Page<K, V> page) {
        Page<K,V> replacement = pageMap == null ? null : pageMap.get(page);
        if (replacement == null) {
            replacement = page.copy();
            registerReplacementPage(pageMap, page, replacement);
        }
        return replacement;
    }

    private static <K,V> void registerReplacementPage(Map<Page<K,V>,Page<K,V>> pageMap, Page<K,V> page, Page<K,V> replacementPage) {
        if (pageMap != null) {
//            assert validatePageRemoval(pageMap, page);
            pageMap.put(page, replacementPage);
            if(page != replacementPage) {
                pageMap.put(replacementPage, replacementPage);
            }
        }
    }

//    private static boolean validatePageRemoval(Map<Page, Page> pageMap, Page page) {
//        long pagePos = page.getPos();
//        if (DataUtils.isPageSaved(pagePos)) {
//            for (Page p : pageMap.keySet()) {
//                assert p == page || p.getPos() != pagePos : page + "\n--------\n" + p;
//            }
//        }
//        return true;
//    }


    private static <K,V> KVMapping<K, V>[] createLeftoverBuffer(KVMapping<K, V>[] buffer, int from) {
        int remaining = buffer.length - from;
        assert remaining >= 0;
        if (remaining == 0) {
            return null;
        } else {
            KVMapping<K,V>[] newBuffer = createBuffer(remaining);
            System.arraycopy(buffer, from, newBuffer, 0, remaining);
            return newBuffer;
        }
    }

    private static <K> K getCeilingKey(CursorPos<K,?> path) {
        while ((path = path.parent) != null) {
            int ind = path.index;
            if (ind < path.page.getKeyCount()) {
                return path.page.getKey(ind);
            }
        }
        return null;
    }

    private Page<K, V> createParentPage(Page<K,V>[] pages, IntValueHolder unsavedMemoryHolder) {
        int childrenCount = pages.length;
        assert childrenCount > 0;
        K[] keys = createKeyStorage(childrenCount - 1);
        PageReference<K,V>[] children = PageReference.createRefStorage(childrenCount);
        int totalKeyCount = 0;
        int index = -1;
        for (Page<K, V> childPage : pages) {
            if (index >= 0) {
                keys[index] = childPage.getKey(0);
            }
            children[++index] = new PageReference<>(childPage);
            unsavedMemoryHolder.value += childPage.getMemory();
            totalKeyCount += childPage.getKeyCount();
        }
        return Page.createNode(this, keys, children, totalKeyCount, 0);
    }

    private Page<K, V> createParentPage(K splitKey, Page<K, V> splitted, Page<K, V> splitoff, long totalCount) {
        K[] keys = createKeyStorage(1);
        keys[0] = splitKey;
        PageReference<K,V>[] children = PageReference.createRefStorage(2);
        children[0] = new PageReference<>(splitted);
        children[1] = new PageReference<>(splitoff);
        return Page.createNode(this, keys, children, totalCount, 0);
    }

    public final K[] createKeyStorage(int size)
    {
        return getKeyType().createStorage(size);
    }

    final V[] createValueStorage(int size)
    {
        return getValueType().createStorage(size);
    }


    /**
     * Appends entry to this map. this method is NOT thread safe and can not be used
     * neither concurrently, nor in combination with any method that updates this map.
     * Non-updating method may be used concurrently, but latest appended values
     * are not guaranteed to be visible.
     * @param key should be higher in map's order than any existing key
     * @param value to be appended
     */
    public void append(K key, V value) {
        if (singleWriter) {
            beforeWrite();
            RootReference<K,V> rootReference = lockRoot(getRoot(), 1);
            int appendCounter = rootReference.getAppendCounter();
            try {
                if (appendCounter >= keysPerPage) {
                    rootReference = flushAppendBuffer(rootReference, false);
                    appendCounter = rootReference.getAppendCounter();
                    assert appendCounter < keysPerPage;
                }
                keysBuffer[appendCounter] = key;
                valuesBuffer[appendCounter] = value;
                ++appendCounter;
            } finally {
                unlockRoot(appendCounter);
            }
        } else {
            put(key, value);
        }
    }

    /**
     * Removes last entry from this map. this method is NOT thread safe and can not be used
     * neither concurrently, nor in combination with any method that updates this map.
     * Non-updating method may be used concurrently, but latest removal may not be visible.
     */
    public void trimLast() {
        if (singleWriter) {
            RootReference<K,V> rootReference = getRoot();
            int appendCounter = rootReference.getAppendCounter();
            boolean useRegularRemove = appendCounter == 0;
            if (!useRegularRemove) {
                rootReference = lockRoot(rootReference, 1);
                try {
                    appendCounter = rootReference.getAppendCounter();
                    useRegularRemove = appendCounter == 0;
                    if (!useRegularRemove) {
                        --appendCounter;
                    }
                } finally {
                    unlockRoot(appendCounter);
                }
            }
            if (useRegularRemove) {
                Page<K,V> lastLeaf = rootReference.root.getAppendCursorPos(null).page;
                assert lastLeaf.isLeaf();
                assert lastLeaf.getKeyCount() > 0;
                Object key = lastLeaf.getKey(lastLeaf.getKeyCount() - 1);
                remove(key);
            }
        } else {
            remove(lastKey());
        }
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
         * @param store which will own this map
         * @param config configuration
         *
         * @return the map
         */
        M create(MVStore store, Map<String, Object> config);

        DataType<K> getKeyType();

        DataType<V> getValueType();

        void setKeyType(DataType<? super K> dataType);

        void setValueType(DataType<? super V> dataType);

    }

    /**
     * A builder for this class.
     *
     * @param <K> the key type
     * @param <V> the value type
     */
    public abstract static class BasicBuilder<M extends MVMap<K, V>, K, V> implements MapBuilder<M, K, V> {

        private DataType<K> keyType;
        private DataType<V> valueType;

        /**
         * Create a new builder with the default key and value data types.
         */
        protected BasicBuilder() {
            // ignore
        }

        @Override
        public DataType<K> getKeyType() {
            return keyType;
        }

        @Override
        public DataType<V> getValueType() {
            return valueType;
        }

        @SuppressWarnings("unchecked")
        @Override
        public void setKeyType(DataType<? super K> keyType) {
            this.keyType = (DataType<K>)keyType;
        }

        @SuppressWarnings("unchecked")
        @Override
        public void setValueType(DataType<? super V> valueType) {
            this.valueType = (DataType<V>)valueType;
        }

        /**
         * Set the key data type.
         *
         * @param keyType the key type
         * @return this
         */
        public BasicBuilder<M, K, V> keyType(DataType<? super K> keyType) {
            setKeyType(keyType);
            return this;
        }

        /**
         * Set the value data type.
         *
         * @param valueType the value type
         * @return this
         */
        public BasicBuilder<M, K, V> valueType(DataType<? super V> valueType) {
            setValueType(valueType);
            return this;
        }

        @Override
        public M create(MVStore store, Map<String, Object> config) {
            if (getKeyType() == null) {
                setKeyType(new ObjectDataType());
            }
            if (getValueType() == null) {
                setValueType(new ObjectDataType());
            }
            DataType<K> keyType = getKeyType();
            DataType<V> valueType = getValueType();
            config.put("store", store);
            config.put("key", keyType);
            config.put("val", valueType);
            return create(config);
        }

        /**
         * Create map from config.
         * @param config config map
         * @return new map
         */
        protected abstract M create(Map<String, Object> config);

    }

    /**
     * A builder for this class.
     *
     * @param <K> the key type
     * @param <V> the value type
     */
    public static class Builder<K, V> extends BasicBuilder<MVMap<K, V>, K, V> {
        private boolean singleWriter;

        public Builder() {}

        @Override
        public Builder<K,V> keyType(DataType<? super K> dataType) {
            setKeyType(dataType);
            return this;
        }

        @Override
        public Builder<K, V> valueType(DataType<? super V> dataType) {
            setValueType(dataType);
            return this;
        }

        /**
         * Set up this Builder to produce MVMap, which can be used in append mode
         * by a single thread.
         * @see MVMap#append(Object, Object)
         * @return this Builder for chained execution
         */
        public Builder<K,V> singleWriter() {
            singleWriter = true;
            return this;
        }

        @Override
        protected MVMap<K, V> create(Map<String, Object> config) {
            config.put("singleWriter", singleWriter);
            Object type = config.get("type");
            if(type == null || type.equals("rtree")) {
                return new MVMap<>(config, getKeyType(), getValueType());
            }
            throw new IllegalArgumentException("Incompatible map type");
        }
    }

    /**
     * The decision on what to do on an update.
     */
    public enum Decision { ABORT, REMOVE, PUT, REPEAT }

    /**
     * Class DecisionMaker provides callback interface (and should become a such in Java 8)
     * for MVMap.operate method.
     * It provides control logic to make a decision about how to proceed with update
     * at the point in execution when proper place and possible existing value
     * for insert/update/delete key is found.
     * Revised value for insert/update is also provided based on original input value
     * and value currently existing in the map.
     *
     * @param <V> value type of the map
     */
    public abstract static class DecisionMaker<V> {
        /**
         * Decision maker for transaction rollback.
         */
        public static final DecisionMaker<Object> DEFAULT = new DecisionMaker<Object>() {
            @Override
            public Decision decide(Object existingValue, Object providedValue) {
                return providedValue == null ? Decision.REMOVE : Decision.PUT;
            }

            @Override
            public String toString() {
                return "default";
            }
        };

        /**
         * Decision maker for put().
         */
        public static final DecisionMaker<Object> PUT = new DecisionMaker<Object>() {
            @Override
            public Decision decide(Object existingValue, Object providedValue) {
                return Decision.PUT;
            }

            @Override
            public String toString() {
                return "put";
            }
        };

        /**
         * Decision maker for remove().
         */
        public static final DecisionMaker<Object> REMOVE = new DecisionMaker<Object>() {
            @Override
            public Decision decide(Object existingValue, Object providedValue) {
                return Decision.REMOVE;
            }

            @Override
            public String toString() {
                return "remove";
            }
        };

        /**
         * Decision maker for putIfAbsent() key/value.
         */
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

        /**
         * Decision maker for replace().
         */
        static final DecisionMaker<Object> IF_PRESENT= new DecisionMaker<Object>() {
            @Override
            public Decision decide(Object existingValue, Object providedValue) {
                return existingValue != null ? Decision.PUT : Decision.ABORT;
            }

            @Override
            public String toString() {
                return "if_present";
            }
        };

        /**
         * Makes a decision about how to proceed with the update.
         *
         * @param existingValue the old value
         * @param providedValue the new value
         * @param tip the cursor position
         * @return the decision
         */
        public Decision decide(V existingValue, V providedValue, CursorPos<?, ?> tip) {
            return decide(existingValue, providedValue);
        }

        /**
         * Makes a decision about how to proceed with the update.
         * @param existingValue value currently exists in the map
         * @param providedValue original input value
         * @return PUT if a new value need to replace existing one or
         *             a new value to be inserted if there is none
         *         REMOVE if existing value should be deleted
         *         ABORT if update operation should be aborted or repeated later
         *         REPEAT if update operation should be repeated immediately
         */
        public abstract Decision decide(V existingValue, V providedValue);

        /**
         * Provides revised value for insert/update based on original input value
         * and value currently existing in the map.
         * This method is only invoked after call to decide(), if it returns PUT.
         * @param existingValue value currently exists in the map
         * @param providedValue original input value
         * @param <T> value type
         * @return value to be used by insert/update
         */
        public <T extends V> T selectValue(T existingValue, T providedValue) {
            return providedValue;
        }

        /**
         * Resets internal state (if any) of a this DecisionMaker to it's initial state.
         * This method is invoked whenever concurrent update failure is encountered,
         * so we can re-start update process.
         */
        public void reset() {}
    }

    /**
     * Add, replace or remove a key-value pair.
     *
     * @param key the key (may not be null)
     * @param value new value, it may be null when removal is intended
     * @param decisionMaker command object to make choices during transaction.
     * @return previous value, if mapping for that key existed, or null otherwise
     */
    public V operate(K key, V value, DecisionMaker<? super V> decisionMaker) {
        if (singleWriter || this == store.getMetaMap()/* || getRoot().getTotalCount() < 10_000*/) {
            return operateAppendable(key, value, decisionMaker);
        } else {
            return operateBuffered(key, value, decisionMaker);
        }
    }

    private V operateAppendable(K key, V value, DecisionMaker<? super V> decisionMaker) {
        IntValueHolder unsavedMemoryHolder = new IntValueHolder();
        int attempt = 0;
        while(true) {
            RootReference<K,V> rootReference = flushAndGetRoot();
            boolean locked = rootReference.isLockedByCurrentThread();
            if (!locked) {
                if (attempt++ == 0 && beforeWrite()) {
                    continue;
                }
                if (attempt > 3 || rootReference.isLocked()) {
                    rootReference = lockRoot(rootReference, attempt);
                    locked = true;
                }
            }
            assert rootReference.buffer == null;
            Page<K,V> rootPage = rootReference.root;
            long version = rootReference.version;
            CursorPos<K,V> tip;
            V result;
            unsavedMemoryHolder.value = 0;
            try {
                CursorPos<K,V> pos = CursorPos.traverseDown(rootPage, key);
                if(!locked && rootReference != getRoot()) {
                    continue;
                }
                Page<K,V> p = pos.page;
                int index = pos.index;
                tip = pos;
                pos = pos.parent;
                result = index < 0 ? null : p.getValue(index);
                Decision decision = decisionMaker.decide(result, value, tip);

                switch (decision) {
                    case REPEAT:
                        decisionMaker.reset();
                        continue;
                    case ABORT:
                        if(!locked && rootReference != getRoot()) {
                            decisionMaker.reset();
                            continue;
                        }
                        return result;
                    case REMOVE: {
                        if (index < 0) {
                            if(!locked && rootReference != getRoot()) {
                                decisionMaker.reset();
                                continue;
                            }
                            return null;
                        }

                        if (p.getTotalCount() == 1 && pos != null) {
                            int keyCount;
                            do {
                                p = pos.page;
                                index = pos.index;
                                pos = pos.parent;
                                keyCount = p.getKeyCount();
                                // condition below should always be false, but older
                                // versions (up to 1.4.197) may create
                                // single-childed (with no keys) internal nodes,
                                // which we skip here
                            } while (keyCount == 0 && pos != null);

                            if (keyCount <= 1) {
                                if (keyCount == 1) {
                                    assert index <= 1;
                                    p = p.getChildPage(1 - index);
                                } else {
                                    // if root happens to be such single-childed
                                    // (with no keys) internal node, then just
                                    // replace it with empty leaf
                                    p = Page.createEmptyLeaf(this);
                                }
                                break;
                            }
                        }
                        p = p.copy();
                        p.remove(index);
                        break;
                    }
                    case PUT: {
                        value = decisionMaker.selectValue(result, value);
                        p = p.copy();
                        if (index < 0) {
                            p.insertLeaf(-index - 1, key, value);
                            int keyCount;
                            while ((keyCount = p.getKeyCount()) > store.getKeysPerPage()
                                    || p.getMemory() > store.getMaxPageSize()
                                    && keyCount > (p.isLeaf() ? 1 : 2)) {
                                long totalCount = p.getTotalCount();
                                int at = keyCount >> 1;
                                K k = p.getKey(at);
                                Page<K,V> split = p.split(at);
                                unsavedMemoryHolder.value += p.getMemory() + split.getMemory();
                                if (pos == null) {
                                    p = createParentPage(k, p, split, totalCount);
                                    break;
                                }
                                p = insertPageAfter(pos, p, k, split);
                                pos = pos.parent;
                            }
                        } else {
                            p.setValue(index, value);
                        }
                        break;
                    }
                }
                rootPage = replacePage(pos, p, unsavedMemoryHolder, null);
                if (!locked) {
                    rootReference = rootReference.updateRootPage(rootPage, attempt);
                    if (rootReference == null) {
                        decisionMaker.reset();
                        continue;
                    }
                }
                store.registerUnsavedMemory(unsavedMemoryHolder.value + tip.processRemovalInfo(version));
                return result;
            } finally {
                if(locked) {
                    unlockRoot(rootPage);
                }
            }
        }
    }

    private Page<K, V> insertPageAfter(CursorPos<K, V> path, Page<K, V> anchorPage, K key, Page<K, V> insertPage) {
        Page<K, V> parent = path.page;
        parent = parent.copy();
        int index = path.index;
        parent.setChild(index, insertPage, true);
        parent.insertNode(index, key, anchorPage);
        return parent;
    }

    /**
     * Add, replace or remove a key-value pair.
     *
     * @param key the key (may not be null)
     * @param value new value, it may be null when removal is intended
     * @param decisionMaker command object to make choices during transaction.
     * @return previous value, if mapping for that key existed, or null otherwise
     */
    private V operateBuffered(K key, V value, DecisionMaker<? super V> decisionMaker) {
        int attempt = 0;
        while(true) {
            RootReference<K,V> rootReference = flushAppendBufferAndGetRoot();
            boolean locked = rootReference.isLockedByCurrentThread();
            if (!locked) {
                if (attempt++ == 0 && beforeWrite()) {
                    continue;
                }
                if (attempt > 3 || rootReference.isLocked()) {
                    rootReference = lockRoot(rootReference, attempt);
                    locked = true;
                }
            }

            KVMapping<K,V>[] buffer = rootReference.buffer;

            if(buffer != null && buffer.length >= store.getKeysPerPage()) {
                rootReference = flushBuffer(rootReference, store.getKeysPerPage());
                assert locked == rootReference.isLockedByCurrentThread();
                buffer = rootReference.buffer;
            }

            V result;
            KVMapping<K,V>[] newBuffer = buffer;
            int bufferIndex;
            try {
                if (buffer == null) {
                    buffer = dummyBuffer();
                    bufferIndex = -1;
                } else {
                    bufferIndex = binarySearch(buffer, key, 0);
                }
                int index;
                if (bufferIndex >= 0) {
                    index = buffer[bufferIndex].index;
                    result = buffer[bufferIndex].value;
                } else {
                    Page<K, V> page = Page.findLeaf(rootReference.root, key);
                    index = page.binarySearch(key);
                    result = index < 0 ? null : page.getValue(index);
                }
                Decision decision = decisionMaker.decide(result, value);
                switch (decision) {
                    case REPEAT:
                        decisionMaker.reset();
                        continue;
                    case ABORT:
                        if (!locked && rootReference != getRoot()) {
                            decisionMaker.reset();
                            continue;
                        }
                        return result;
                    case REMOVE: {
                        if (bufferIndex >= 0) {     // found in buffer
                            if (buffer[bufferIndex].value == null) {
                                // nothing to do, already deleted
                                if(!locked && rootReference != getRoot()) {
                                    decisionMaker.reset();
                                    continue;
                                }
                                return null;
                            }
                            if (index >= 0) {
                                newBuffer = buffer.clone();
                                newBuffer[bufferIndex] = new KVMapping<>(key, null, index);
                            } else if (buffer.length > 1) {
                                newBuffer = createBuffer(buffer.length - 1);
                                DataUtils.copyExcept(buffer, newBuffer, buffer.length, bufferIndex);
                            } else {
                                newBuffer = null;
                            }
                            break;
                        } else if (index < 0) {
                            // not found in the map either
                            if(!locked && rootReference != getRoot()) {
                                decisionMaker.reset();
                                continue;
                            }
                            return null;
                        } else {    // exists on the map
                            newBuffer = createBuffer(buffer.length + 1);
                            bufferIndex = ~bufferIndex;
                            DataUtils.copyWithGap(buffer, newBuffer, buffer.length, bufferIndex);
                            newBuffer[bufferIndex] = new KVMapping<>(key, null, index);
                            break;
                        }
                    }
                    case PUT: {
                        value = decisionMaker.selectValue(result, value);
                        if (bufferIndex >= 0) {     // exists in buffer already, replace value
                            newBuffer = buffer.clone();
                        } else {                    // insert new mapping into the buffer
                            newBuffer = createBuffer(buffer.length + 1);
                            bufferIndex = ~bufferIndex;
                            DataUtils.copyWithGap(buffer, newBuffer, buffer.length, bufferIndex);
                        }
                        newBuffer[bufferIndex] = new KVMapping<>(key, value, index);
                        break;
                    }
                }
                if (locked) {
                    buffer = newBuffer;
                } else if (rootReference.updateBuffer(newBuffer, attempt) == null) {
                    decisionMaker.reset();
                    continue;
                }
            } finally {
                if(locked) {
                    unlockRoot(buffer);
                }
            }
            return result;
        }
    }

    private boolean shouldBeSplitted(Page<K,V> page) {
        return shouldBeSplitted(page.getKeyCount(), page.getMemory(), page.isLeaf());
    }

    private boolean shouldBeSplitted(int keyCount, int memory, boolean isLeaf) {
        return (keyCount > store.getKeysPerPage()
                || memory > store.getMaxPageSize() && keyCount > (isLeaf ? 1 : 2));
    }

    private RootReference<K,V> lockRoot(RootReference<K,V> rootReference, int attempt) {
        while(true) {
            RootReference<K,V> lockedRootReference = tryLock(rootReference, attempt++);
            if (lockedRootReference != null) {
                return lockedRootReference;
            }
            rootReference = getRoot();
        }
    }


    private static final int CPU_COUNT = Runtime.getRuntime().availableProcessors();
    /**
     * Try to lock the root.
     *
     * @param rootReference the old root reference
     * @param attempt the number of attempts so far
     * @return the new root reference
     */
    protected RootReference<K,V> tryLock(RootReference<K,V> rootReference, int attempt) {
        RootReference<K,V> lockedRootReference = rootReference.tryLock(attempt);
        if (lockedRootReference != null) {
            return lockedRootReference;
        }
        assert !rootReference.isLockedByCurrentThread() : rootReference;
        RootReference<K,V> oldRootReference = rootReference.previous;
        int contention = 1;
        if (oldRootReference != null) {
            long updateAttemptCounter = rootReference.updateAttemptCounter -
                                        oldRootReference.updateAttemptCounter;
            assert updateAttemptCounter >= 0 : updateAttemptCounter;
            long updateCounter = rootReference.updateCounter - oldRootReference.updateCounter;
            assert updateCounter >= 0 : updateCounter;
            assert updateAttemptCounter >= updateCounter : updateAttemptCounter + " >= " + updateCounter;
            contention += (int)((updateAttemptCounter+1) / (updateCounter+1));
        }

        if(attempt > 4 || contention > CPU_COUNT / 2) {
            if (attempt <= 12) {
                Thread.yield();
            } else if (attempt <= 70 - 2 * contention) {
                try {
                    Thread.sleep(contention);
                } catch (InterruptedException ex) {
                    throw new RuntimeException(ex);
                }
            } else {
                synchronized (lock) {
                    notificationRequested = true;
                    try {
                        lock.wait(5);
                    } catch (InterruptedException ignore) {
                    }
                }
            }
        }
        return null;
    }

    /**
     * Unlock the root page, the new root being null.
     *
     * @return the new root reference (never null)
     */
    private RootReference<K,V> unlockRoot() {
        return unlockRoot(null, -1, dummyBuffer());
    }

    /**
     * Unlock the root page.
     *
     * @param newRootPage the new root
     * @return the new root reference (never null)
     */
    protected RootReference<K,V> unlockRoot(Page<K,V> newRootPage) {
        return unlockRoot(newRootPage, -1, dummyBuffer());
    }

    private void unlockRoot(KVMapping<K,V>[] buffer) {
        unlockRoot(null, -1, buffer);
    }

    private void unlockRoot(int appendCounter) {
        unlockRoot(null, appendCounter, dummyBuffer());
    }

    private RootReference<K,V> unlockRoot(Page<K,V> newRootPage, int appendCounter, KVMapping<K,V>[] buffer) {
        RootReference<K,V> updatedRootReference;
        do {
            RootReference<K,V> rootReference = getRoot();
            assert rootReference.isLockedByCurrentThread();
            updatedRootReference = rootReference.updatePageAndLockedStatus(
                                        newRootPage == null ? rootReference.root : newRootPage,
                                        false,
                                        appendCounter == -1 ? rootReference.getAppendCounter() : appendCounter,
                                        buffer == DUMMY_BUFFER ? rootReference.buffer : buffer);
        } while(updatedRootReference == null);

        notifyWaiters();
        return updatedRootReference;
    }

    private void notifyWaiters() {
        if (notificationRequested) {
            synchronized (lock) {
                notificationRequested = false;
                lock.notify();
            }
        }
    }

    final boolean isMemoryEstimationAllowed() {
        return avgKeySize != null || avgValSize != null;
    }

    final int evaluateMemoryForKeys(K[] storage, int count) {
        if (avgKeySize == null) {
            return calculateMemory(keyType, storage, count);
        }
        return MemoryEstimator.estimateMemory(avgKeySize, keyType, storage, count);
    }

    final int evaluateMemoryForValues(V[] storage, int count) {
        if (avgValSize == null) {
            return calculateMemory(valueType, storage, count);
        }
        return MemoryEstimator.estimateMemory(avgValSize, valueType, storage, count);
    }

    private static <T> int calculateMemory(DataType<T> keyType, T[] storage, int count) {
        int mem = count * MEMORY_POINTER;
        for (int i = 0; i < count; i++) {
            mem += keyType.getMemory(storage[i]);
        }
        return mem;
    }

    final int evaluateMemoryForKey(K key) {
        if (avgKeySize == null) {
            return keyType.getMemory(key);
        }
        return MemoryEstimator.estimateMemory(avgKeySize, keyType, key);
    }

    final int evaluateMemoryForValue(V value) {
        if (avgValSize == null) {
            return valueType.getMemory(value);
        }
        return MemoryEstimator.estimateMemory(avgValSize, valueType, value);
    }

    static int samplingPct(AtomicLong stats) {
        return MemoryEstimator.samplingPct(stats);
    }

    private static final class EqualsDecisionMaker<V> extends DecisionMaker<V> {
        private final DataType<V> dataType;
        private final V           expectedValue;
        private       Decision    decision;

        EqualsDecisionMaker(DataType<V> dataType, V expectedValue) {
            this.dataType = dataType;
            this.expectedValue = expectedValue;
        }

        @Override
        public Decision decide(V existingValue, V providedValue) {
            assert decision == null;
            decision = !areValuesEqual(dataType, expectedValue, existingValue) ? Decision.ABORT :
                                            providedValue == null ? Decision.REMOVE : Decision.PUT;
            return decision;
        }

        @Override
        public void reset() {
            decision = null;
        }

        Decision getDecision() {
            return decision;
        }

        @Override
        public String toString() {
            return "equals_to "+expectedValue;
        }
    }

    private static final class RewriteDecisionMaker<V> extends DecisionMaker<V> {
        private final long pagePos;
        private Decision decision;

        RewriteDecisionMaker(long pagePos) {
            this.pagePos = pagePos;
        }

        @Override
        public Decision decide(V existingValue, V providedValue, CursorPos<?,?> tip) {
            assert decision == null;
            decision = Decision.ABORT;
            if(!DataUtils.isLeafPosition(pagePos)) {
                while ((tip = tip.parent) != null) {
                    if (tip.page.getPos() == pagePos) {
                        decision = decide(existingValue, providedValue);
                        break;
                    }
                }
            } else if (tip.page.getPos() == pagePos) {
                decision = decide(existingValue, providedValue);
            }
            return decision;
        }

        @Override
        public Decision decide(V existingValue, V providedValue) {
            decision = existingValue == null ? Decision.ABORT : Decision.PUT;
            return decision;
        }

        @Override
        public <T extends V> T selectValue(T existingValue, T providedValue) {
            return existingValue;
        }

        @Override
        public void reset() {
            decision = null;
        }

        Decision getDecision() {
            return decision;
        }

        @Override
        public String toString() {
            return "rewrite";
        }
    }

    private static final class IntValueHolder {
        int value;

        IntValueHolder() {}
    }
}
