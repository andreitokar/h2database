/*
 * Copyright 2004-2022 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (https://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.mvstore;

import static org.h2.engine.Constants.MEMORY_ARRAY;
import static org.h2.engine.Constants.MEMORY_OBJECT;
import static org.h2.engine.Constants.MEMORY_POINTER;
import static org.h2.mvstore.DataUtils.PAGE_TYPE_LEAF;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collection;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.atomic.AtomicLongFieldUpdater;
import java.util.function.BiFunction;
import org.h2.compress.Compressor;
import org.h2.mvstore.type.DataType;
import org.h2.util.Utils;

/**
 * A page (a node or a leaf).
 * <p>
 * For b-tree nodes, the key at a given index is larger than the largest key of
 * the child at the same index.
 * <p>
 * Serialized format:
 * length of a serialized page in bytes (including this field): int
 * check value: short
 * page number (0-based sequential number within a chunk): varInt
 * map id: varInt
 * number of keys: varInt
 * type: byte (0: leaf, 1: node; +2: compressed)
 * children of the non-leaf node (1 more than keys)
 * compressed: bytes saved (varInt)
 * keys
 * values of the leaf node (one for each key)
 */
public abstract class Page<K,V> implements Cloneable {

    /**
     * Map this page belongs to
     */
    public final MVMap<K,V> map;

    /**
     * Position of this page's saved image within a Chunk
     * or 0 if this page has not been saved yet
     * or 1 if this page has not been saved yet, but already removed
     * This "removed" flag is to keep track of pages that concurrently
     * changed while they are being stored, in which case the live bookkeeping
     * needs to be aware of such cases.
     * Field need to be volatile to avoid races between saving thread setting it
     * and other thread reading it to access the page.
     * On top of this update atomicity is required so removal mark and saved position
     * can be set concurrently.
     *
     * @see DataUtils#getPagePos(int, int, int, int) for field format details
     */
    private volatile long pos;

    /**
     * Sequential 0-based number of the page within containing chunk.
     */
    public int pageNo = -1;

    /**
     * The last result of a find operation is cached.
     */
    private int cachedCompare;

    /**
     * The estimated memory used in persistent case, IN_MEMORY marker value otherwise.
     */
    private int memory;

    /**
     * Amount of used disk space by this page only in persistent case.
     */
    private int diskSpaceUsed;

    /**
     * The keys.
     */
    K[] keys;

    /**
     * Updater for pos field, which can be updated when page is saved,
     * but can be concurrently marked as removed
     */
    @SuppressWarnings("rawtypes")
    private static final AtomicLongFieldUpdater<Page> posUpdater =
                                                AtomicLongFieldUpdater.newUpdater(Page.class, "pos");
    /**
     * The estimated number of bytes used per child entry.
     */
    static final int PAGE_MEMORY_CHILD = MEMORY_POINTER + 16; //  16 = two longs

    /**
     * The estimated number of bytes used per base page.
     */
    private static final int PAGE_MEMORY =
            MEMORY_OBJECT +           // this
            2 * MEMORY_POINTER +      // map, keys
            MEMORY_ARRAY +            // Object[] keys
            17;                       // pos, cachedCompare, memory, removedInMemory
    /**
     * The estimated number of bytes used per empty internal page object.
     */
    static final int PAGE_NODE_MEMORY =
            PAGE_MEMORY +             // super
            MEMORY_POINTER +          // children
            MEMORY_ARRAY +            // Object[] children
            8;                        // totalCount

    /**
     * The estimated number of bytes used per empty leaf page.
     */
    static final int PAGE_LEAF_MEMORY =
            PAGE_MEMORY +             // super
            MEMORY_POINTER +          // values
            MEMORY_ARRAY;             // Object[] values

    /**
     * Marker value for memory field, meaning that memory accounting is replaced by key count.
     */
    private static final int IN_MEMORY = Integer.MIN_VALUE;

    @SuppressWarnings("rawtypes")
    private static final PageReference[] SINGLE_EMPTY = { PageReference.EMPTY };

    @SuppressWarnings("unchecked")
    static <X,Y> KVMapping<X,Y>[] createBuffer(int size) { return new KVMapping[size]; }


    Page(MVMap<K,V> map) {
        this.map = map;
    }

    Page(MVMap<K,V> map, Page<K,V> source) {
        this(map, source.keys);
        memory = source.memory;
    }

    Page(MVMap<K,V> map, K[] keys) {
        this.map = map;
        this.keys = keys;
        assert validateCreation(map, keys);
    }

    private static <K,V> boolean validateCreation(MVMap<K,V> map, K[] keys) {
        if (map.getKeyType().isComparable()) {
            for (int i = 1; i < keys.length; i++) {
                assert map.compare(keys[i - 1], keys[i]) < 0;
            }
        }
        return true;
    }

    /**
     * Create a new, empty leaf page.
     *
     * @param <K> key type
     * @param <V> value type
     *
     * @param map the map
     * @return the new page
     */
    static <K,V> Page<K,V> createEmptyLeaf(MVMap<K,V> map) {
        return createLeaf(map, map.getKeyType().createStorage(0),
                map.getValueType().createStorage(0), PAGE_LEAF_MEMORY);
    }

    /**
     * Create a new, empty internal node page.
     *
     * @param <K> key type
     * @param <V> value type
     *
     * @param map the map
     * @return the new page
     */
    @SuppressWarnings("unchecked")
    static <K,V> Page<K,V> createEmptyNode(MVMap<K,V> map) {
        return createNode(map, map.getKeyType().createStorage(0), SINGLE_EMPTY, 0,
                            PAGE_NODE_MEMORY + MEMORY_POINTER + PAGE_MEMORY_CHILD); // there is always one child
    }

    /**
     * Create a new non-leaf page. The arrays are not cloned.
     *
     * @param <K> the key class
     * @param <V> the value class
     * @param map the map
     * @param keys the keys
     * @param children the child page positions
     * @param totalCount the total number of keys
     * @param memory the memory used in bytes
     * @return the page
     */
    public static <K,V> Page<K,V> createNode(MVMap<K,V> map, K[] keys, PageReference<K,V>[] children,
                                    long totalCount, int memory) {
        assert keys != null;
        Page<K,V> page = new NonLeaf<>(map, keys, children, totalCount);
        page.initMemoryAccount(memory);
        return page;
    }

    public static <K,V> Page<K,V> createNode(NonLeaf<K,V> source, KVMapping<K,V>[] buffer) {
        Page<K, V> page;
        if (buffer == null || buffer.length == 0) {
            page = new NonLeaf<>(source.map, source.keys, source.children, 0);
        } else {
            page = new BufferedNonLeaf<>(source.map, source.keys, source.children, buffer);
        }
        page.initMemoryAccount(0);
        page.recalculateTotalCount();
        return page;
    }

    public static <K,V> Page<K,V> createNode(MVMap<K, V> map, K[] keys, PageReference<K, V>[] children,
                                             KVMapping<K, V>[] buffer) {
        assert keys != null;
        Page<K,V> page;
        if (buffer == null || buffer.length == 0) {
            page = new NonLeaf<>(map, keys, children, 0);
        } else {
            page = new BufferedNonLeaf<>(map, keys, children, buffer);
        }
        page.initMemoryAccount(0);
        page.recalculateTotalCount();
        return page;
    }

    /**
     * Create a new leaf page. The arrays are not cloned.
     *
     * @param <K> key type
     * @param <V> value type
     *
     * @param map the map
     * @param keys the keys
     * @param values the values
     * @param memory the memory used in bytes
     * @return the page
     */
    static <K,V> Page<K,V> createLeaf(MVMap<K,V> map, K[] keys, V[] values, int memory) {
        assert keys != null;
        Page<K,V> page = new Leaf<>(map, keys, values);
        page.initMemoryAccount(memory);
        return page;
    }

/*
    public static <T> int merge(
                            T[] a, int lowA, int highA,
                            T[] b, int lowB, int highB,
                            T[] res, int posRes,
                            Comparator<? super T> comparator,
                            BiFunction<? super T, ? super T, ? extends T> aggregator,
                            boolean preferB) {
        T[] res2 = res.clone();
        int count = mergeSmall(a, lowA, highA,
                                b, lowB, highB,
                                res, posRes,
                                comparator,
                                aggregator,
                                preferB);
        int count2 = merge2(a, lowA, highA,
                                b, lowB, highB,
                                res2, posRes,
                                comparator,
                                aggregator,
                                preferB);
        assert count == count2;
        for (int i = posRes; i < count; i++) {
            assert comparator.compare(res[i], res2[i]) == 0;
        }
        return count;
    }
*/


    public static <T> int merge(T[] a, int lowA, int highA,
                                T[] b, int lowB, int highB,
                                T[] res, int posRes,
                                Comparator<? super T> comparator,
                                BiFunction<? super T, ? super T, ? extends T> aggregator,
                                boolean preferB) {
        while (true) {
            int lenA = highA - lowA;
            assert lenA >= 0;
            int lenB = highB - lowB;
            assert lenB >= 0;
//            if (lenA + lenB <= 7) {
//                return mergeSmall(a, lowA, highA, b, lowB, highB, res, posRes, comparator, aggregator, preferB);
//            }
//*
            if (lenA > lenB) {
                T[] tmp = a;
                a = b;
                b = tmp;

                int t = lowA;
                lowA = lowB;
                lowB = t;

                t = highA;
                highA = highB;
                highB = t;

                t = lenA;
                lenA = lenB;
                lenB = t;

                preferB = !preferB;
            }
            if (lenA == 0) {
                System.arraycopy(b, lowB, res, posRes, lenB);
                return posRes + lenB;
            }
            int midA = (lowA + highA) >> 1;
            T itemA = a[midA];
            int midB = Arrays.binarySearch(b, lowB, highB, itemA, comparator);
            boolean match = midB >= 0;
            if (!match) {
                midB = ~midB;
            }

            posRes = merge(a, lowA, midA, b, lowB, midB, res, posRes, comparator, aggregator, preferB);

            if (match) {
                T itemB = b[midB++];
                if (preferB) {
                    T tmp = itemA;
                    itemA = itemB;
                    itemB = tmp;
                }
                itemA = aggregator.apply(itemA, itemB);
                match = itemA == null;
            }
            if (!match) {
                res[posRes++] = itemA;
            }
            lowA = ++midA;
            lowB = midB;
        }
    }

    public static <T> int mergeBig(T[] a, int lowA, int highA,
                            T[] b, int lowB, int highB,
                            T[] res, int posRes,
                            Comparator<? super T> comparator,
                            BiFunction<? super T, ? super T, ? extends T> aggregator,
                            boolean preferB) {
        while(true) {
            int lenA = highA - lowA;
            assert lenA >= 0;
            int lenB = highB - lowB;
            assert lenB >= 0;
//            if (lenA + lenB <= 9) {
//                return mergeSmall(a, lowA, highA, b, lowB, highB, res, posRes, comparator, aggregator, preferB);
//            }
            if (lenA > lenB) {
                T[] tmp = a;
                a = b;
                b = tmp;

                int t = lowA;
                lowA = lowB;
                lowB = t;

                t = highA;
                highA = highB;
                highB = t;

                preferB = !preferB;
                continue;
            }
            if (lenA == 0) {
                System.arraycopy(b, lowB, res, posRes, lenB);
                return posRes + lenB;
            }
            T itemA = a[lowA++];
            int midB = Arrays.binarySearch(b, lowB, highB, itemA, comparator);
            boolean match = midB >= 0;
            if (!match) {
                midB = ~midB;
            }
            lenB = midB - lowB;

            System.arraycopy(b, lowB, res, posRes, lenB);
            posRes += lenB;
            lowB = midB;

            if (match) {
                T itemB = b[lowB++];
//                match = aggregator != null;
//                if (!match) {
//                    if (!preferB) {
//                        T tmp = itemA;
//                        itemA = itemB;
//                        itemB = tmp;
//                    }
//                    res[posRes++] = itemB;
//                } else {
                    if (preferB) {
                        T tmp = itemA;
                        itemA = itemB;
                        itemB = tmp;
                    }
//                        itemA = aggregator.apply(itemB, itemA);
//                    } else {
                        itemA = aggregator.apply(itemA, itemB);
//                    }
                    match = itemA == null;
//                }
            }
            if (!match) {
                res[posRes++] = itemA;
            }
        }
    }

    public static <T> int mergeSmall(
                            T[] a, int lowA, int highA,
                            T[] b, int lowB, int highB,
                            T[] res, int posRes,
                            Comparator<? super T> comparator,
                            BiFunction<? super T, ? super T, ? extends T> aggregator,
                            boolean preferB) {
        T itemA = null;
        T itemB = null;
        --lowA;
        --lowB;
        int compare = 0;
        while (true) {
            if (compare <= 0) {
                if (++lowA >= highA) {
                    if (compare == 0) {
                        ++lowB;
                    }
                    highB -= lowB;
                    System.arraycopy(b, lowB, res, posRes, highB);
                    return posRes + highB;
                }
                itemA = a[lowA];
            }
            if (compare >= 0) {
                if (++lowB >= highB) {
                    highA -= lowA;
                    System.arraycopy(a, lowA, res, posRes, highA);
                    return posRes + highA;
                }
                itemB = b[lowB];
            }
            compare = comparator.compare(itemA, itemB);
            T item = compare < 0 ? itemA :
                     compare > 0 ? itemB :
                     preferB ? aggregator.apply(itemB, itemA) :
                               aggregator.apply(itemA, itemB);
            if (item != null) {
                res[posRes++] = item;
            }
        }
    }

    private void initMemoryAccount(int memoryCount) {
        if(!map.isPersistent()) {
            memory = IN_MEMORY;
        } else if (memoryCount == 0) {
            recalculateMemory();
        } else {
            addMemory(memoryCount);
            assert memoryCount == getMemory();
        }
    }

    /**
     * Get the value for the given key, or null if not found.
     * Search is done in the tree rooted at given page.
     *
     * @param <K> key type
     * @param <V> value type
     *
     * @param key the key
     * @param p the root page
     * @return the value, or null if not found
     */
    static <K,V> V get(Page<K,V> p, K key) {
        MVMap<K, V> map = p.map;
        while (true) {
            KVMapping<K, V>[] buffer = p.getBuffer();
            if (buffer != null) {
                int index = map.binarySearch(buffer, key, 0);
                if (index >= 0) {
                    return buffer[index].value;
                }
            }
            int index = p.binarySearch(key);
            if (p.isLeaf()) {
                return index >= 0 ? p.getValue(index) : null;
            }
            p = p.getChildPage(++index < 0 ? -index : index);
        }
    }

    /**
     * Read a page.
     *
     * @param <K> key type
     * @param <V> value type
     *
     * @param buff ByteBuffer containing serialized page info
     * @param pos the position
     * @param map the map
     * @return the page
     */
    static <K,V> Page<K,V> read(ByteBuffer buff, long pos, MVMap<K,V> map) {
        boolean leaf = (DataUtils.getPageType(pos) & 1) == PAGE_TYPE_LEAF;
        Page<K,V> p = leaf ? new Leaf<>(map) : new BufferedNonLeaf<>(map);
        p.pos = pos;
        p.read(buff);
        return p;
    }

    /**
     * Get the id of the page's owner map
     * @return id
     */
    public final int getMapId() {
        return map.getId();
    }

    /**
     * Create a copy of this page with potentially different owning map.
     * This is used exclusively during bulk map copying.
     * Child page references for nodes are cleared (re-pointed to an empty page)
     * to be filled-in later to copying procedure. This way it can be saved
     * mid-process without tree integrity violation
     *
     * @param map new map to own resulting page
     * @param eraseChildrenRefs whether cloned Page should have no child references or keep originals
     * @return the page
     */
    abstract Page<K,V> copy(MVMap<K, V> map, boolean eraseChildrenRefs);

    /**
     * Get the key at the given index.
     *
     * @param index the index
     * @return the key
     */
    public K getKey(int index) {
        return keys[index];
    }

    /**
     * Get the child page at the given index.
     *
     * @param index the index
     * @return the child page
     */
    public abstract Page<K,V> getChildPage(int index);

    /**
     * Get the position of the child.
     *
     * @param index the index
     * @return the position
     */
    public abstract long getChildPagePos(int index);

    /**
     * Get the value at the given index.
     *
     * @param index the index
     * @return the value
     */
    public abstract V getValue(int index);

    /**
     * Get the number of keys in this page.
     *
     * @return the number of keys
     */
    public final int getKeyCount() {
        return keys.length;
    }

    /**
     * Check whether this is a leaf page.
     *
     * @return true if it is a leaf
     */
    public final boolean isLeaf() {
        return getNodeType() == PAGE_TYPE_LEAF;
    }

    public abstract int getNodeType();

    /**
     * Get the position of the page
     *
     * @return the position
     */
    public final long getPos() {
        return pos;
    }

    @Override
    public String toString() {
        StringBuilder buff = new StringBuilder();
        dump(buff);
        return buff.toString();
    }

    /**
     * Dump debug data for this page.
     *
     * @param buff append buffer
     */
    protected void dump(StringBuilder buff) {
        buff.append("id: ").append(System.identityHashCode(this)).append('\n');
        buff.append("pos: ").append(Long.toHexString(pos)).append('\n');
        if (isSaved()) {
            int chunkId = DataUtils.getPageChunkId(pos);
            buff.append("chunk: ").append(Long.toHexString(chunkId)).append('\n');
        }
    }

    public KVMapping<K, V>[] getBuffer() {
        return null;
    }

    /**
     * Create a copy of this page.
     *
     * @return a mutable copy of this page
     */
    public final Page<K,V> copy() {
        Page<K,V> newPage = clone();
        newPage.pos = 0;
        return newPage;
    }

    @SuppressWarnings("unchecked")
    @Override
    protected final Page<K,V> clone() {
        Page<K,V> clone;
        try {
            clone = (Page<K,V>) super.clone();
        } catch (CloneNotSupportedException impossible) {
            throw new RuntimeException(impossible);
        }
        return clone;
    }

    /**
     * Search the key in this page using a binary search. Instead of always
     * starting the search in the middle, the last found index is cached.
     * <p>
     * If the key was found, the returned value is the index in the key array.
     * If not found, the returned value is negative, where -1 means the provided
     * key is smaller than any keys in this page. See also Arrays.binarySearch.
     *
     * @param key the key
     * @return the value or null
     */
    final int binarySearch(K key) {
        int res = map.getKeyType().binarySearch(key, keys, getKeyCount(), cachedCompare);
        cachedCompare = res < 0 ? ~res : res + 1;
        return res;
    }

    protected final int binarySearch(KVMapping<K,V>[] buffer, K key, int low, int high) {
        DataType<K> comparator = map.getKeyType();
        while (low <= high) {
            int x = (low + high) >>> 1;
            int compare = comparator.compare(key, buffer[x].key);
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

    protected final int calculateNumberOfPages(int totalChildPages, int bufferLen) {
        MVStore store = map.getStore();
        int keyCountLimit = store.getKeysPerPage();
        int numberOfPages = (totalChildPages + keyCountLimit) / (keyCountLimit + 1);
        assert numberOfPages > 0;
        if (isPersistent()) {
            long maxPageSize = store.getMaxPageSize();
            if (maxPageSize != Long.MAX_VALUE) {
                int avgKeySize = map.evaluateMemoryForKey();
                int avgValueSize = map.evaluateMemoryForValue();
                int estimate = (int) ((maxPageSize + PAGE_NODE_MEMORY + totalChildPages * (avgKeySize + PAGE_MEMORY_CHILD) + (avgKeySize + avgValueSize + 2 * MEMORY_POINTER) * bufferLen) / maxPageSize);
                assert estimate > 0;
                int maxNumberOfPages = (totalChildPages + 1) / 2;
                if (estimate > maxNumberOfPages) {
                    estimate = maxNumberOfPages;
                    if (estimate > numberOfPages) {
                        numberOfPages = estimate;
                    }
                }
            }
        }
        return numberOfPages;
    }

    protected final int calculateKeyPerLeafLimit() {
        MVStore store = map.getStore();
        int keyCountLimit = store.getKeysPerPage();
        if (isPersistent()) {
            long maxPageSize = store.getMaxPageSize();
            if (maxPageSize != Long.MAX_VALUE) {
                int avgKeySize = map.evaluateMemoryForKey();
                int avgValueSize = map.evaluateMemoryForValue();
                int memBasedLimit = (int) ((maxPageSize - PAGE_LEAF_MEMORY) / (avgKeySize + avgValueSize + MEMORY_OBJECT + 2 * MEMORY_POINTER));
                if (memBasedLimit == 0) {
                    keyCountLimit = 1;
                } else if (memBasedLimit < keyCountLimit) {
                    keyCountLimit = memBasedLimit;
                }
            }
        }
        return keyCountLimit;
    }

    protected final boolean verifyBufferLen(int bufferLen, int extraChildren) {
        MVStore store = map.getStore();
        int keyCountLimit = store.getKeysPerPage();
        int keyCount = getKeyCount();
        if (bufferLen > keyCountLimit || keyCount + extraChildren > keyCountLimit) {
            return false;
        }
        if (!isPersistent()) {
            return true;
        }

        long maxPageSize = store.getMaxPageSize();
        int pageMemory = 0;
        int avgKeySize = map.evaluateMemoryForKey();
        int avgValueSize = map.evaluateMemoryForValue();
        extraChildren += getRawChildPageCount();
        pageMemory += extraChildren * (avgKeySize + PAGE_MEMORY_CHILD) + (avgKeySize + avgValueSize + 2 * MEMORY_POINTER) * bufferLen;
        return pageMemory <= maxPageSize;
    }

    abstract int acceptMappings(KVMapping<K, V>[] buffer,
                                K[] keyHolder, Page<K, V>[] pageHolder,
                                Collection<Page<K, V>> removedPages, MVMap.IntValueHolder unsavedMemoryHolder);

    abstract int acceptMappings(KVMapping<K, V>[] buffer, int position, int count,
                                K[] keyHolder, Page<K, V>[] pageHolder, int holderPosition,
                                Collection<Page<K, V>> removedPages, MVMap.IntValueHolder unsavedMemoryHolder);

    /**
     * Split the page. This modifies the current page.
     *
     * @param at the split index
     * @return the page with the entries after the split index
     */
    abstract Page<K,V> split(int at);

    /**
     * Split the current keys array into two arrays.
     *
     * @param aCount size of the first array.
     * @param bCount size of the second array/
     * @return the second array.
     */
    final K[] splitKeys(int aCount, int bCount) {
        assert aCount + bCount <= getKeyCount();
        K[] aKeys = createKeyStorage(aCount);
        K[] bKeys = createKeyStorage(bCount);
        System.arraycopy(keys, 0, aKeys, 0, aCount);
        System.arraycopy(keys, getKeyCount() - bCount, bKeys, 0, bCount);
        keys = aKeys;
        return bKeys;
    }

    /**
     * Append additional key/value mappings to this Page.
     * New mappings suppose to be in correct key order.
     *
     * @param extraKeyCount number of mappings to be added
     * @param extraKeys to be added
     * @param extraValues to be added
     */
    abstract void expand(int extraKeyCount, K[] extraKeys, V[] extraValues);

    /**
     * Expand the keys array.
     *
     * @param extraKeyCount number of extra key entries to create
     * @param extraKeys extra key values
     */
    final void expandKeys(int extraKeyCount, K[] extraKeys) {
        int keyCount = getKeyCount();
        K[] newKeys = createKeyStorage(keyCount + extraKeyCount);
        if (extraKeys != null) {
            System.arraycopy(keys, 0, newKeys, 0, keyCount);
            System.arraycopy(extraKeys, 0, newKeys, keyCount, extraKeyCount);
        }
        keys = newKeys;
    }

    /**
     * Get the total number of key-value pairs, including child pages.
     *
     * @return the number of key-value pairs
     */
    public abstract long getTotalCount();

    protected void recalculateTotalCount() {}


    /**
     * Get the number of key-value pairs for a given child.
     *
     * @param index the child index
     * @return the descendant count
     */
    abstract long getCounts(int index);

    /**
     * Replace the child page.
     *
     * @param index the index
     * @param c the new child page
     */
    public abstract void setChild(int index, Page<K,V> c);

    /**
     * Replace the key at an index in this page.
     *
     * @param index the index
     * @param key the new key
     */
    public final void setKey(int index, K key) {
        keys = keys.clone();
        if(isPersistent()) {
            K old = keys[index];
            if (!map.isMemoryEstimationAllowed() || old == null) {
                int mem = map.evaluateMemoryForKey(key);
                if (old != null) {
                    mem -= map.evaluateMemoryForKey(old);
                }
                addMemory(mem);
            }
        }
        keys[index] = key;
    }

    /**
     * Replace the value at an index in this page.
     *
     * @param index the index
     * @param value the new value
     * @return the old value
     */
    public abstract V setValue(int index, V value);

    /**
     * Insert a key-value pair into this leaf.
     *
     * @param index the index
     * @param key the key
     * @param value the value
     */
    public abstract void insertLeaf(int index, K key, V value);

    /**
     * Insert a child page into this node.
     *
     * @param index the index
     * @param key the key
     * @param childPage the child page
     */
    public abstract void insertNode(int index, K key, Page<K,V> childPage);

    /**
     * Insert a key into the key array
     *
     * @param index index to insert at
     * @param key the key value
     */
    final void insertKey(int index, K key) {
        int keyCount = getKeyCount();
        assert index <= keyCount : index + " > " + keyCount;
        K[] newKeys = createKeyStorage(keyCount + 1);
        DataUtils.copyWithGap(keys, newKeys, keyCount, index);
        keys = newKeys;

        keys[index] = key;

        if (isPersistent()) {
            addMemory(MEMORY_POINTER + map.evaluateMemoryForKey(key));
        }
    }

    /**
     * Remove the key and value (or child) at the given index.
     *
     * @param index the index
     */
    public void remove(int index) {
        int keyCount = getKeyCount();
        if (index == keyCount) {
            --index;
        }
        if(isPersistent()) {
            if (!map.isMemoryEstimationAllowed()) {
                K old = getKey(index);
                addMemory(-MEMORY_POINTER - map.evaluateMemoryForKey(old));
            }
        }
        K[] newKeys = createKeyStorage(keyCount - 1);
        DataUtils.copyExcept(keys, newKeys, keyCount, index);
        keys = newKeys;
    }

    /**
     * Read the page from the buffer.
     *
     * @param buff the buffer to read from
     */
    private void read(ByteBuffer buff) {
        int chunkId = DataUtils.getPageChunkId(pos);
        int offset = DataUtils.getPageOffset(pos);

        int start = buff.position();
        int pageLength = buff.getInt(); // does not include optional part (pageNo)
        int remaining = buff.remaining() + 4;
        if (pageLength > remaining || pageLength < 4) {
            throw DataUtils.newMVStoreException(DataUtils.ERROR_FILE_CORRUPT,
                    "File corrupted in chunk {0}, expected page length 4..{1}, got {2}", chunkId, remaining,
                    pageLength);
        }

        short check = buff.getShort();
        int checkTest = DataUtils.getCheckValue(chunkId)
                ^ DataUtils.getCheckValue(offset)
                ^ DataUtils.getCheckValue(pageLength);
        if (check != (short) checkTest) {
            throw DataUtils.newMVStoreException(DataUtils.ERROR_FILE_CORRUPT,
                    "File corrupted in chunk {0}, expected check value {1}, got {2}", chunkId, checkTest, check);
        }

        pageNo = DataUtils.readVarInt(buff);
        if (pageNo < 0) {
            throw DataUtils.newMVStoreException(DataUtils.ERROR_FILE_CORRUPT,
                    "File corrupted in chunk {0}, got negative page No {1}", chunkId, pageNo);
        }

        int mapId = DataUtils.readVarInt(buff);
        if (mapId != map.getId()) {
            throw DataUtils.newMVStoreException(DataUtils.ERROR_FILE_CORRUPT,
                    "File corrupted in chunk {0}, expected map id {1}, got {2}", chunkId, map.getId(), mapId);
        }

        int keyCount = DataUtils.readVarInt(buff);
        keys = createKeyStorage(keyCount);
        int type = buff.get();
        if(isLeaf() != ((type & 1) == PAGE_TYPE_LEAF)) {
            throw DataUtils.newMVStoreException(
                    DataUtils.ERROR_FILE_CORRUPT,
                    "File corrupted in chunk {0}, expected node type {1}, got {2}",
                    chunkId, isLeaf() ? "0" : "1" , type);
        }

        // to restrain hacky GenericDataType, which grabs the whole remainder of the buffer
        buff.limit(compressedRegionEnd);
        readChildren(buff);
        boolean compressed = (type & DataUtils.PAGE_COMPRESSED) != 0;
        if (compressed) {
            Compressor compressor;
            if ((type & DataUtils.PAGE_COMPRESSED_HIGH) ==
                    DataUtils.PAGE_COMPRESSED_HIGH) {
                compressor = map.getStore().getCompressorHigh();
            } else {
                compressor = map.getStore().getCompressorFast();
            }
            int lenAdd = DataUtils.readVarInt(buff);
            int compLen = buff.remaining();
            byte[] comp;
            int pos = 0;
            if (buff.hasArray()) {
                comp = buff.array();
                pos = buff.arrayOffset() + buff.position();
            } else {
                comp = Utils.newBytes(compLen);
                buff.get(comp);
            }
            int l = compLen + lenAdd;
            buff = ByteBuffer.allocate(l);
            compressor.expand(comp, pos, compLen, buff.array(),
                    buff.arrayOffset(), l);
        }
        map.getKeyType().read(buff, keys, keyCount);
        readPayLoad(buff, type);
        diskSpaceUsed = pageLength;
        recalculateMemory();
        recalculateTotalCount();
    }

    /**
     * Read the page payload from the buffer.
     * @param buff the buffer to read from
     * @param flags one byte per KVMapping (bit0 - mapping's value is null, bit1 - mapping for existing key)
     */
    protected abstract void readPayLoad(ByteBuffer buff, byte[] flags);

    protected abstract void readChildren(ByteBuffer buff);

    public final boolean isSaved() {
        return DataUtils.isPageSaved(pos);
    }

    public final boolean isRemoved() {
        return DataUtils.isPageRemoved(pos);
    }

    /**
     * Mark this page as removed "in memory". That means that only adjustment of
     * "unsaved memory" amount is required. On the other hand, if page was
     * persisted, it's removal should be reflected in occupancy of the
     * containing chunk.
     *
     * @return true if it was marked by this call or has been marked already,
     *         false if page has been saved already.
     */
    private boolean markAsRemoved() {
        assert getTotalCount() > 0 : this;
        long pagePos;
        do {
            pagePos = pos;
            if (DataUtils.isPageSaved(pagePos)) {
                return false;
            }
            assert !DataUtils.isPageRemoved(pagePos);
        } while (!posUpdater.compareAndSet(this, 0L, 1L));
        return true;
    }

    /**
     * Store the page and update the position.
     *
     * @param chunk the chunk
     * @param buff the target buffer
     * @param toc prospective table of content
     * @return the position of the buffer just after the type
     */
    protected final int write(Chunk chunk, WriteBuffer buff, List<Long> toc) {
        pageNo = toc.size();
        int keyCount = getKeyCount();
        int start = buff.position();
        buff.putInt(0)          // placeholder for pageLength
            .putShort((byte)0) // placeholder for check
            .putVarInt(pageNo)
            .putVarInt(map.getId())
            .putVarInt(keyCount);
        int typePos = buff.position();
        int type = isLeaf() ? PAGE_TYPE_LEAF : DataUtils.PAGE_TYPE_NODE;
        buff.put((byte)type);
        int childrenPos = buff.position();
        writeChildren(buff, true);
        int compressStart = buff.position();
        map.getKeyType().write(buff, keys, keyCount);
        writePayload(buff);
        MVStore store = map.getStore();
        int expLen = buff.position() - compressStart;
        if (expLen > 16) {
            int compressionLevel = store.getCompressionLevel();
            if (compressionLevel > 0) {
                Compressor compressor;
                int compressType;
                if (compressionLevel == 1) {
                    compressor = store.getCompressorFast();
                    compressType = DataUtils.PAGE_COMPRESSED;
                } else {
                    compressor = store.getCompressorHigh();
                    compressType = DataUtils.PAGE_COMPRESSED_HIGH;
                }
                byte[] comp = new byte[expLen * 2];
                ByteBuffer byteBuffer = buff.getBuffer();
                int pos = 0;
                byte[] exp;
                if (byteBuffer.hasArray()) {
                    exp = byteBuffer.array();
                    pos = byteBuffer.arrayOffset()  + compressStart;
                } else {
                    exp = Utils.newBytes(expLen);
                    buff.position(compressStart).get(exp);
                }
                int compLen = compressor.compress(exp, pos, expLen, comp, 0);
                int plus = DataUtils.getVarIntLen(expLen - compLen);
                if (compLen + plus < expLen) {
                    buff.position(flagsPos)
                        .put((byte) (flags | compressType));
                    buff.position(compressStart)
                        .putVarInt(expLen - compLen)
                        .put(comp, 0, compLen);
                }
            }
        }
        if (hasBuffer) {
            for (KVMapping<K, V> kvMapping : buffer) {
                int bits = (kvMapping.value == null ? 1 : 0) | (kvMapping.existing ? 2 : 0);
                buff.put((byte)bits);
            }
            buff.putShort((short)buffer.length);
        }
        int pageLength = buff.position() - start;
        long tocElement = DataUtils.getTocElement(getMapId(), start, buff.position() - start, type);
        toc.add(tocElement);
        int chunkId = chunk.id;
        int check = DataUtils.getCheckValue(chunkId)
                ^ DataUtils.getCheckValue(start)
                ^ DataUtils.getCheckValue(pageLength);
        buff.putInt(start, pageLength)
            .putShort(start + 4, (short) check);
        if (isSaved()) {
            throw DataUtils.newMVStoreException(
                    DataUtils.ERROR_INTERNAL, "Page already stored");
        }
        long pagePos = DataUtils.getPagePos(chunkId, tocElement);
        boolean isDeleted = isRemoved();
        while (!posUpdater.compareAndSet(this, isDeleted ? 1L : 0L, pagePos)) {
            isDeleted = isRemoved();
        }
        store.cachePage(this);
        if (!isLeaf()) {
            // cache again - this will make sure nodes stays in the cache
            // for a longer time
            store.cachePage(this);
        }
        int pageLengthEncoded = DataUtils.getPageMaxLength(pos);
        boolean singleWriter = map.isSingleWriter();
        chunk.accountForWrittenPage(pageLengthEncoded, singleWriter);
        if (isDeleted) {
            store.accountForRemovedPage(pagePos, chunk.version + 1, singleWriter, pageNo);
        }
        diskSpaceUsed = pageLengthEncoded != DataUtils.PAGE_LARGE ? pageLengthEncoded : pageLength;
        return childrenPos;
    }

    /**
     * Write values that the buffer contains to the buff.
     *
     * @param buff the target buffer
     */
    protected abstract void writePayload(WriteBuffer buff);

    /**
     * Write page children to the buff.
     *
     * @param buff the target buffer
     * @param withCounts true if the descendant counts should be written
     */
    protected abstract void writeChildren(WriteBuffer buff, boolean withCounts);

    /**
     * Store this page and all children that are changed, in reverse order, and
     * update the position and the children.
     * @param chunk the chunk
     * @param buff the target buffer
     * @param toc table of content for the chunk
     */
    abstract void writeUnsavedRecursive(Chunk chunk, WriteBuffer buff, List<Long> toc);

    /**
     * Unlink the children recursively after all data is written.
     */
    abstract void releaseSavedPages();

    public abstract int getRawChildPageCount();

    protected final boolean isPersistent() {
        return memory != IN_MEMORY;
    }

    public final int getMemory() {
        if (isPersistent()) {
//            assert memory == calculateMemory() :
//                    "Memory calculation error " + memory + " != " + calculateMemory();
            return memory;
        }
        return 0;
    }

    /**
     * Amount of used disk space in persistent case including child pages.
     *
     * @return amount of used disk space in persistent case
     */
    public long getDiskSpaceUsed() {
        long r = 0;
        if (isPersistent()) {
            r += diskSpaceUsed;
            if (!isLeaf()) {
                for (int i = 0; i < getRawChildPageCount(); i++) {
                    long pos = getChildPagePos(i);
                    if (pos != 0) {
                        r += getChildPage(i).getDiskSpaceUsed();
                    }
                }
            }
        }
        return r;
    }

    /**
     * Increase estimated memory used in persistent case.
     *
     * @param mem additional memory size.
     */
    final void addMemory(int mem) {
        memory += mem;
        assert memory >= 0;
    }

    /**
     * Recalculate estimated memory used in persistent case.
     */
    final void recalculateMemory() {
        assert isPersistent();
        memory = calculateMemory();
    }

    /**
     * Calculate estimated memory used in persistent case.
     *
     * @return memory in bytes
     */
    protected int calculateMemory() {
//*
        return map.evaluateMemoryForKeys(keys, getKeyCount());
/*/
        int keyCount = getKeyCount();
        int mem = keyCount * MEMORY_POINTER;
        DataType<K> keyType = map.getKeyType();
        for (int i = 0; i < keyCount; i++) {
            mem += getMemory(keyType, keys[i]);
        }
        return mem;
//*/
    }

    public boolean isComplete() {
        return true;
    }

    /**
     * Called when done with copying page.
     */
    public void setComplete() {}

    /**
     * Make accounting changes (chunk occupancy or "unsaved" RAM), related to
     * this page removal.
     *
     * @param version at which page was removed
     * @return amount (negative), by which "unsaved memory" should be adjusted,
     *         if page is unsaved one, and 0 for page that was already saved, or
     *         in case of non-persistent map
     */
    public final int removePage(long version) {
        if(isPersistent() && getTotalCount() > 0) {
            MVStore store = map.store;
            if (!markAsRemoved()) { // only if it has been saved already
                long pagePos = pos;
                store.accountForRemovedPage(pagePos, version, map.isSingleWriter(), pageNo);
            } else {
                return -memory;
            }
        }
        return 0;
    }

    /**
     * Extend path from a given CursorPos chain to "prepend point" in a B-tree, rooted at this Page.
     *
     * @param cursorPos presumably pointing to this Page (null if real root), to build upon
     * @return new head of the CursorPos chain
     */
    public abstract CursorPos<K,V> getPrependCursorPos(CursorPos<K,V> cursorPos);

    /**
     * Extend path from a given CursorPos chain to "append point" in a B-tree, rooted at this Page.
     *
     * @param cursorPos presumably pointing to this Page (null if real root), to build upon
     * @return new head of the CursorPos chain
     */
    public abstract CursorPos<K,V> getAppendCursorPos(CursorPos<K,V> cursorPos);

    /**
     * Remove all page data recursively.
     * @param version at which page got removed
     * @return adjustment for "unsaved memory" amount
     */
    public abstract int removeAllRecursive(long version);

    public abstract int normalizeIndex(int index, boolean reverse);

    /**
     * Create array for keys storage.
     *
     * @param size number of entries
     * @return values array
     */
    public final K[] createKeyStorage(int size) {
        return map.getKeyType().createStorage(size);
    }

    /**
     * Create array for values storage.
     *
     * @param size number of entries
     * @return values array
     */
    final V[] createValueStorage(int size) {
        return map.getValueType().createStorage(size);
    }

    static <K,V> PageReference<K,V>[] constructEmptyPageRefs(int size) {
        // replace child pages with empty pages
        PageReference<K,V>[] children = PageReference.createRefStorage(size);
        Arrays.fill(children, PageReference.empty());
        return children;
    }

    /**
     * Create an array of page references.
     *
     * @param <K> the key class
     * @param <V> the value class
     * @param size the number of entries
     * @return the array
     */
    @SuppressWarnings("unchecked")
    public static <K,V> PageReference<K,V>[] createRefStorage(int size) {
        return new PageReference[size];
    }

    /**
     * A pointer to a page, either in-memory or using a page position.
     */
    public static final class PageReference<K,V> {

        /**
         * Singleton object used when arrays of PageReference have not yet been filled.
         */
        @SuppressWarnings("rawtypes")
        static final PageReference EMPTY = new PageReference<>(null, 0, 0);

        /**
         * The position, if known, or 0.
         */
        private long pos;

        /**
         * The page, if in memory, or null.
         */
        private Page<K,V> page;

        /**
         * The descendant count for this child page.
         */
        final long count;

        /**
         * Get an empty page reference.
         *
         * @param <X> the key class
         * @param <Y> the value class
         * @return the page reference
         */
        @SuppressWarnings("unchecked")
        public static <X,Y> PageReference<X,Y> empty() {
            return EMPTY;
        }

        @SuppressWarnings("unchecked")
        public static <K,V> PageReference<K,V>[] createRefStorage(int size) {
            return new PageReference[size];
        }

        public PageReference(Page<K,V> page) {
            this(page, page.getPos(), page.getTotalCount());
        }

        PageReference(long pos, long count) {
            this(null, pos, count);
            assert DataUtils.isPageSaved(pos);
        }

        private PageReference(Page<K,V> page, long pos, long count) {
            this.page = page;
            this.pos = pos;
            this.count = count;
        }

        Page<K,V> getReferencedPage(MVMap<K, V> map) {
            Page<K, V> result = page;
            if(result == null) {
                result = map.readPage(pos);
                assert pos == result.getPos();
                assert count == result.getTotalCount();
            }
            return result;
        }

        public Page<K,V> getPage() {
            return page;
        }

        /**
         * Clear if necessary, reference to the actual child Page object,
         * so it can be garbage collected if not actively used elsewhere.
         * Reference is cleared only if corresponding page was already saved on a disk.
         */
        void clearPageReference() {
            if (page != null) {
                page.releaseSavedPages();
                assert page.isSaved() || !page.isComplete();
                if (page.isSaved()) {
                    assert pos == page.getPos();
                    assert /*!page.isComplete() || page instanceof IncompleteNonLeaf ||*/ count == page.getTotalCount() : count + " != " + page.getTotalCount();
                    page = null;
                }
            }
        }

        long getPos() {
            return pos;
        }

        /**
         * Re-acquire position from in-memory page.
         */
        void resetPos() {
            Page<K,V> p = page;
            if (p != null && p.isSaved()) {
                pos = p.getPos();
                assert /*!p.isComplete() || p instanceof IncompleteNonLeaf ||*/ count == p.getTotalCount();
            }
        }

        @Override
        public String toString() {
            return "Cnt:" + count + ", pos:" + (pos == 0 ? "0" : DataUtils.getPageChunkId(pos) +
                    (page == null ? "" : "/" + page.pageNo) +
                    "-" + DataUtils.getPageOffset(pos) + ":" + DataUtils.getPageMaxLength(pos)) +
                    ((page == null ? DataUtils.getPageType(pos) == 0 : page.isLeaf()) ? " leaf" : " node") +
                    ", page:{" + page + "}";
        }
    }


    public static class NonLeaf<K,V> extends Page<K,V> {
        /**
         * The child page references.
         */
        PageReference<K,V>[] children;

        /**
        * The total entry count of this page and all children.
        */
        private long totalCount;

        NonLeaf(MVMap<K,V> map) {
            super(map);
        }

        NonLeaf(MVMap<K,V> map, NonLeaf<K,V> source, PageReference<K,V>[] children) {
            super(map, source);
            this.children = children;
            this.totalCount = source.totalCount;
            assert validateAfterCreation();
        }

        NonLeaf(MVMap<K, V> map, K[] keys, PageReference<K, V>[] children, long totalCount) {
            super(map, keys);
            this.children = children;
            this.totalCount = totalCount;
            assert validateAfterCreation();
        }

        private boolean validateAfterCreation() {
            if (map.getKeyType().isComparable()) {
                for (int i = 0; i < getKeyCount(); i++) {
                    K key = getKey(i);
                    Page<K, V> leftChildPage = getChildPage(i);
                    Page<K, V> rightChildPage = getChildPage(i + 1);
                    assert map.compare(leftChildPage.getKey(leftChildPage.getKeyCount() - 1), key) <= 0;
                    assert map.compare(key, rightChildPage.getKey(0)) <= 0;
                    KVMapping<K, V>[] buffer = leftChildPage.getBuffer();
                    assert buffer == null || map.compare(buffer[buffer.length - 1].key, key) <= 0;
                    buffer = rightChildPage.getBuffer();
                    assert buffer == null || map.compare(key, buffer[0].key) <= 0;
                }
            }
            return true;
        }

        @Override
        public int getNodeType() {
            return DataUtils.PAGE_TYPE_NODE;
        }

        @Override
        public Page<K,V> copy(MVMap<K, V> map, boolean eraseChildrenRefs) {
            return eraseChildrenRefs ?
                    new IncompleteNonLeaf<>(map, this) :
                    new NonLeaf<>(map, this, children, totalCount);
        }

        @Override
        public Page<K,V> getChildPage(int index) {
            PageReference<K,V> ref = children[index];
            Page<K, V> page = ref.getReferencedPage(map);
            return page;
        }

        @Override
        public long getChildPagePos(int index) {
            return children[index].getPos();
        }

        @Override
        public V getValue(int index) {
            throw new UnsupportedOperationException();
        }

        @Override
        int acceptMappings(KVMapping<K, V>[] buffer,
                           K[] keyHolder, Page<K, V>[] pageHolder,
                           Collection<Page<K, V>> removedPages, MVMap.IntValueHolder unsavedMemoryHolder) {
            removedPages.add(this);
            keyHolder[0] = null;
            Page<K, V> page = Page.createNode(this, buffer);
            pageHolder[0] = page;
            if (page.getTotalCount() > 0) {
                unsavedMemoryHolder.value += page.getMemory();
                return 1;
            }
            return 0;
        }

        @Override
        int acceptMappings(KVMapping<K, V>[] buffer, int position, int count,
                           K[] keyHolder, Page<K, V>[] pageHolder, int holderPosition,
                           Collection<Page<K, V>> removedPages, MVMap.IntValueHolder unsavedMemoryHolder) {
            removedPages.add(this);
            keyHolder[holderPosition] = null;    // can be removed?
            Page<K, V> page = Page.createNode(this, Arrays.copyOfRange(buffer, position, position + count));
            pageHolder[holderPosition] = page;
            if (page.getTotalCount() > 0) {
                unsavedMemoryHolder.value += page.getMemory();
                return 1;
            }
            return 0;
        }

        @Override
        public Page<K,V> split(int at) {
            assert !isSaved();
            int b = getKeyCount() - at;
            K[] bKeys = splitKeys(at, b - 1);
            PageReference<K,V>[] aChildren = PageReference.createRefStorage(at + 1);
            PageReference<K,V>[] bChildren = PageReference.createRefStorage(b);
            System.arraycopy(children, 0, aChildren, 0, at + 1);
            System.arraycopy(children, at + 1, bChildren, 0, b);
            children = aChildren;

            long t = 0;
            for (PageReference<K,V> x : aChildren) {
                t += x.count;
            }
            totalCount = t;
            t = 0;
            for (PageReference<K,V> x : bChildren) {
                t += x.count;
            }
            Page<K,V> newPage = createNode(map, bKeys, bChildren, t, 0);
            if(isPersistent()) {
                recalculateMemory();
            }
            return newPage;
        }

        @Override
        public void expand(int keyCount, Object[] extraKeys, Object[] extraValues) {
            throw new UnsupportedOperationException();
        }

        @Override
        public long getTotalCount() {
            assert !isComplete() || totalCount == calculateTotalCount() :
                        "Total count: " + totalCount + " != " + calculateTotalCount();
            return totalCount;
        }

        long calculateTotalCount() {
            long check = 0;
            int keyCount = getKeyCount();
            for (int i = 0; i <= keyCount; i++) {
                check += children[i].count;
            }
            return check;
        }

        protected void recalculateTotalCount() {
            totalCount = calculateTotalCount();
        }

        @Override
        long getCounts(int index) {
            return children[index].count;
        }

        @Override
        public void setChild(int index, Page<K,V> c) {
            assert c != null;
            PageReference<K,V> child = children[index];
            if (c != child.getPage() || c.getPos() != child.getPos()) {
                totalCount += c.getTotalCount() - child.count;
                children = children.clone();
                children[index] = new PageReference<>(c);
            }
        }

        @Override
        public V setValue(int index, V value) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void insertLeaf(int index, K key, V value) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void insertNode(int index, K key, Page<K,V> childPage) {
            int childCount = getRawChildPageCount();
            insertKey(index, key);

            PageReference<K,V>[] newChildren = PageReference.createRefStorage(childCount + 1);
            DataUtils.copyWithGap(children, newChildren, childCount, index);
            children = newChildren;
            children[index] = new PageReference<>(childPage);

            totalCount += childPage.getTotalCount();
            if (isPersistent()) {
                addMemory(MEMORY_POINTER + PAGE_MEMORY_CHILD);
            }
        }

        @Override
        public void remove(int index) {
            int childCount = getRawChildPageCount();
            super.remove(index);
            if(isPersistent()) {
                if (map.isMemoryEstimationAllowed()) {
                    addMemory(-getMemory() / childCount);
                } else {
                    addMemory(-MEMORY_POINTER - PAGE_MEMORY_CHILD);
                }
            }
            totalCount -= children[index].count;
            PageReference<K,V>[] newChildren = PageReference.createRefStorage(childCount - 1);
            DataUtils.copyExcept(children, newChildren, childCount, index);
            children = newChildren;
        }

        @Override
        public int removeAllRecursive(long version) {
            int unsavedMemory = removePage(version);
            if (isPersistent()) {
                for (int i = 0, size = map.getChildPageCount(this); i < size; i++) {
                    PageReference<K,V> ref = children[i];
                    Page<K,V> page = ref.getPage();
                    if (page != null) {
                        unsavedMemory += page.removeAllRecursive(version);
                    } else {
                        long pagePos = ref.getPos();
                        assert DataUtils.isPageSaved(pagePos);
                        if (DataUtils.isLeafPosition(pagePos)) {
                            map.store.accountForRemovedPage(pagePos, version, map.isSingleWriter(), -1);
                        } else {
                            unsavedMemory += map.readPage(pagePos).removeAllRecursive(version);
                        }
                    }
                }
            }
            return unsavedMemory;
        }

        public final int normalizeIndex(int index, boolean revese) {
            if (++index < 0) {
                index = -index;
            }
            return index;
        }

        @Override
        public CursorPos<K,V> getPrependCursorPos(CursorPos<K,V> cursorPos) {
            Page<K,V> childPage = getChildPage(0);
            return childPage.getPrependCursorPos(new CursorPos<>(this, 0, cursorPos));
        }

        @Override
        public CursorPos<K,V> getAppendCursorPos(CursorPos<K,V> cursorPos) {
            int keyCount = getKeyCount();
            Page<K,V> childPage = getChildPage(keyCount);
            return childPage.getAppendCursorPos(new CursorPos<>(this, keyCount, cursorPos));
        }

        @Override
        protected void readPayLoad(ByteBuffer buff, byte[] flags) {}

        @Override
        protected void readChildren(ByteBuffer buff) {
            int keyCount = getKeyCount();
            children = PageReference.createRefStorage(keyCount + 1);
            long[] p = new long[keyCount + 1];
            for (int i = 0; i <= keyCount; i++) {
                p[i] = buff.getLong();
            }
//            long total = 0;
            for (int i = 0; i <= keyCount; i++) {
                long count = DataUtils.readVarLong(buff);
                long position = p[i];
                assert position == 0 ? count == 0 : count >= 0;
//                total += count;
                children[i] = position == 0 ?
                        PageReference.empty() :
                        new PageReference<>(position, count);
            }
//            totalCount = total;
        }

        @Override
        protected void writePayload(WriteBuffer buff) {}

        @Override
        protected void writeChildren(WriteBuffer buff, boolean withCounts) {
            int keyCount = getKeyCount();
            for (int i = 0; i <= keyCount; i++) {
                buff.putLong(children[i].getPos());
            }
            if(withCounts) {
                for (int i = 0; i <= keyCount; i++) {
                    buff.putVarLong(children[i].count);
                }
            }
        }

        @Override
        void writeUnsavedRecursive(Chunk chunk, WriteBuffer buff, List<Long> toc) {
            if (!isSaved()) {
                int patch = write(chunk, buff, toc);
                writeChildrenRecursive(chunk, buff, toc);
                int old = buff.position();
                buff.position(patch);
                writeChildren(buff, false);
                buff.position(old);
            }
        }

        void writeChildrenRecursive(Chunk chunk, WriteBuffer buff, List<Long> toc) {
            int len = getRawChildPageCount();
            for (int i = 0; i < len; i++) {
                PageReference<K,V> ref = children[i];
                Page<K,V> p = ref.getPage();
                if (p != null) {
                    p.writeUnsavedRecursive(chunk, buff, toc);
                    ref.resetPos();
                }
            }
        }

        @Override
        void releaseSavedPages() {
            int len = getRawChildPageCount();
            for (int i = 0; i < len; i++) {
                children[i].clearPageReference();
            }
        }

        @Override
        public int getRawChildPageCount() {
            return getKeyCount() + 1;
        }

        @Override
        protected int calculateMemory() {
            return super.calculateMemory() + PAGE_NODE_MEMORY +
                        getRawChildPageCount() * (MEMORY_POINTER + PAGE_MEMORY_CHILD);
        }

        @Override
        public void dump(StringBuilder buff) {
            super.dump(buff);
            int keyCount = getKeyCount();
            for (int i = 0; i <= keyCount; i++) {
                if (i > 0) {
                    buff.append(" ");
                }
                buff.append("[").append(Long.toHexString(children[i].getPos())).append("]");
                if(i < keyCount) {
                    buff.append(" ").append(getKey(i));
                }
            }
        }
    }


    public static class BufferedNonLeaf<K,V> extends NonLeaf<K,V> {

        private KVMapping<K,V>[] buffer;

        public BufferedNonLeaf(MVMap<K, V> map) {
            super(map);
        }

        public BufferedNonLeaf(MVMap<K, V> map, NonLeaf<K,V> source) {
            super(map, source, constructEmptyPageRefs(source.getRawChildPageCount()));
            this.buffer = source.getBuffer();
        }

        public BufferedNonLeaf(MVMap<K, V> map, K[] keys, PageReference<K, V>[] children, KVMapping<K, V>[] buffer) {
            super(map, keys, children, 0);
            this.buffer = buffer;
            assert validateCreation();
        }

        private boolean validateCreation() {
            KVMapping<K, V>[] buffer = getBuffer();
            assert !isComplete() || buffer != null && buffer.length > 0;
            if (map.getKeyType().isComparable()) {
                for (int i = 1; i < buffer.length; i++) {
                    assert map.compare(buffer[i - 1].key, buffer[i].key) < 0;
                }
            }
            return true;
        }

        public KVMapping<K, V>[] getBuffer() {
            assert buffer == null || buffer.length > 0;
            return buffer;
        }

        @Override
        protected void readPayLoad(ByteBuffer buff, byte[] flags) {
            if (flags != null && flags.length > 0) {
                buffer = Page.createBuffer(flags.length);
                DataType<K> keyType = map.getKeyType();
                DataType<V> valueType = map.getValueType();
                for (int i = 0; i < flags.length; i++) {
                    byte flag = flags[i];
                    boolean existing = (flag & 2) != 0;
                    K key = keyType.read(buff);
                    V value = (flag & 1) != 0 ? null : valueType.read(buff);
                    buffer[i] = new KVMapping<>(key, value, existing);
                }
            }
            recalculateTotalCount();
        }

        @Override
        protected void writePayload(WriteBuffer buff) {
            if (buffer != null) {
                assert buffer.length > 0;
//                buff.putVarInt(buffer.length);
                DataType<K> keyType = map.getKeyType();
                DataType<V> valueType = map.getValueType();
                for (KVMapping<K, V> kvMapping : buffer) {
//                    int flags = (kvMapping.value == null ? 1 : 0) | (kvMapping.existing ? 2 : 0);
//                    buff.put((byte)flags);
                    keyType.write(buff, kvMapping.key);
                    if (kvMapping.value != null) {
                        valueType.write(buff, kvMapping.value);
                    }
                }
            }
        }

        @Override
        protected int calculateMemory() {
            int mem = super.calculateMemory();
            if (buffer != null) {
                mem += MEMORY_ARRAY + buffer.length *
                            (map.evaluateMemoryForKey(buffer[0].key) + map.evaluateMemoryForValue(buffer[0].value) +
                                    MEMORY_OBJECT + 3 * MEMORY_POINTER);
            }
            return mem;
        }

        @Override
        int acceptMappings(KVMapping<K, V>[] buffer,
                           K[] keyHolder, Page<K, V>[] pageHolder,
                           Collection<Page<K, V>> removedPages, MVMap.IntValueHolder unsavedMemoryHolder) {
            return acceptMappings(buffer, buffer.length, keyHolder, pageHolder, 0, removedPages, unsavedMemoryHolder);
        }

        @Override
        int acceptMappings(KVMapping<K, V>[] buffer, int position, int count,
                           K[] keyHolder, Page<K, V>[] pageHolder, int holderPosition,
                           Collection<Page<K, V>> removedPages, MVMap.IntValueHolder unsavedMemoryHolder) {
            assert count > 0;
            KVMapping<K, V>[] ownBuffer = getBuffer();
            if (ownBuffer == null) {
                return super.acceptMappings(buffer, position, count, keyHolder, pageHolder, holderPosition, removedPages, unsavedMemoryHolder);
            }
            
            int ownCount = ownBuffer.length;
            assert ownCount > 0;
            KVMapping<K, V>[] merged = Page.createBuffer(ownCount + count);
            DataType<K> comparator = map.getKeyType();
            int mergedCount = merge(buffer, position, position + count, ownBuffer, 0, ownCount, merged, 0,
                    (o1, o2) -> comparator.compare(o1.key, o2.key),
                    (o1, o2) -> o1.value == null && !o2.existing ? null : new KVMapping<>(o1.key, o1.value, o2.existing),
                    false);

            return acceptMappings(merged, mergedCount, keyHolder, pageHolder, holderPosition, removedPages, unsavedMemoryHolder);
        }

        private int acceptMappings(KVMapping<K, V>[] merged, int mergedCount,
                           K[] keyHolder, Page<K, V>[] pageHolder, int holderPosition,
                           Collection<Page<K, V>> removedPages, MVMap.IntValueHolder unsavedMemoryHolder) {
            removedPages.add(this);
            if (verifyBufferLen(mergedCount, 0)) {
                keyHolder[holderPosition] = null;
                if (mergedCount < merged.length) {
                    merged = Arrays.copyOf(merged, mergedCount);
                }
                Page<K, V> page = Page.createNode(this, merged);
                pageHolder[holderPosition] = page;
                return page.getTotalCount() > 0 ? 1 : 0;
            }

            int keyCount = getKeyCount();
            long[] intervals = new long[keyCount + 2];    // extra one for a possible sentinel
            int intervalCount = 0;
            int start = 0;
            for (int child = 0; child <= keyCount; child++) {
                int index = child == keyCount ? mergedCount :
                                binarySearch(merged, getKey(child), start, mergedCount - 1);
                if (index < 0) {
                    index = ~index;
                }
                int length = index - start;
                if (length > 0) {
                    intervals[intervalCount] = (((long)length << 16 | start) << 16 | intervalCount) << 16 | child;
                    ++intervalCount;
                    start = index;
                }
            }
            long[] reorderedIntervals = Arrays.copyOf(intervals, intervalCount);
            Arrays.sort(reorderedIntervals);
            int totalPushed = 0;

            boolean needSentinel = true;
            int totalChildPageCount = keyCount + 1;
            int extraChildPageCount = 0;
            int topHolderPos = holderPosition;
            for (int i = intervalCount - 1; i >= 0 && (!verifyBufferLen(mergedCount - totalPushed, extraChildPageCount) || totalChildPageCount < 2); i--) {
                long composite = reorderedIntervals[i];
                int child = (int)(composite & 0xFFFF);
                int index = (int)((composite >> 16) & 0xFFFF);
                start = (int)((composite >> 32) & 0xFFFF);
                int length = (int) (composite >> 48);
                assert intervals[index] == composite;
                totalPushed += length;

                int numberOfPages = getChildPage(child)
                        .acceptMappings(merged, start, length, keyHolder, pageHolder, topHolderPos, removedPages, unsavedMemoryHolder);

                intervals[index] = ((~((long)length) << 16 | topHolderPos) << 16 | numberOfPages) << 16 | child;
                topHolderPos += numberOfPages;
                extraChildPageCount += numberOfPages - 1;
                totalChildPageCount += numberOfPages - 1;
                if (child == keyCount) {
                    needSentinel = false;
                }
            }

            if (totalChildPageCount == 0) {
                return 0;
            }
            if (needSentinel) {
                intervals[intervalCount++] = ((-1L << 16 | topHolderPos) << 16 | 1) << 16 | keyCount;
                keyHolder[topHolderPos] = null;   // do we need this?
                pageHolder[topHolderPos++] = getChildPage(keyCount);
            }

            int numberOfPages = calculateNumberOfPages(totalChildPageCount, mergedCount - totalPushed);
            int pageSize = (totalChildPageCount + numberOfPages - 1) / numberOfPages;
            assert pageSize > 0;
            int decreaseAtIndex = totalChildPageCount  - (pageSize - 1) * numberOfPages;
            if (pageSize == 2 && decreaseAtIndex < numberOfPages) {
                pageSize = 3;
                --numberOfPages;
                decreaseAtIndex = 1;
            }
            int nextPageIndex = 0;
            int pageIndex = 0;
            int targetIndex = 0;
            int bufferFromIndex = 0;
            int bufferToIndex = 0;
            K anchorKey = null;

            K[] keyStorage = createKeyStorage(pageSize - 1);
            PageReference<K,V>[] refStorage = PageReference.createRefStorage(pageSize);

            int result = numberOfPages;
            for (int i = 0; i < intervalCount; i++) {
                long composite = intervals[i];
                int child = (int)(composite & 0xFFFF);
                int length = 0;
                int numberOfChildPages = 0;
                boolean pushDown = composite < 0;
                int childHolderPos = -1;
                if (!pushDown) {
                    composite >>= 32;
                    start = (int) (composite & 0xFFFF);
                    length = (int) (composite >> 16);
                } else {
                    composite >>= 16;
                    numberOfChildPages = (int) (composite & 0xFFFF);
                    composite >>= 16;
                    childHolderPos = (int) (composite & 0xFFFF);
                }
                while (true) {
                    int numberOfChildrenToCopy = child - pageIndex;
                    if (numberOfChildrenToCopy > 0) {
                        int remainingSpace = pageSize - targetIndex;
                        int numberOfKeysToCopy = numberOfChildrenToCopy;
                        if (numberOfChildrenToCopy >= remainingSpace) {
                            numberOfChildrenToCopy = remainingSpace;
                            numberOfKeysToCopy = numberOfChildrenToCopy - 1;
                            anchorKey = keys[pageIndex + numberOfKeysToCopy];
                        }
                        System.arraycopy(keys, pageIndex, keyStorage, targetIndex, numberOfKeysToCopy);
                        assert numberOfKeysToCopy == 0 || targetIndex == 0 || map.compare(keyStorage[targetIndex-1], keyStorage[targetIndex]) < 0;
                        System.arraycopy(children, pageIndex, refStorage, targetIndex, numberOfChildrenToCopy);
                        pageIndex += numberOfChildrenToCopy;
                        targetIndex += numberOfChildrenToCopy;
                    } else {
                        if (!pushDown) {
                            if (bufferToIndex != start) {
                                assert bufferToIndex < start;
                                System.arraycopy(merged, start, merged, bufferToIndex, length);
                            }
                            bufferToIndex += length;
                            break;
                        } else {
                            if (numberOfChildPages-- > 0) {
                                refStorage[targetIndex] = new PageReference<>(pageHolder[childHolderPos++]);
                                K key;
                                if (numberOfChildPages > 0) {
                                    key = keyHolder[childHolderPos];
                                } else {
                                    key = pageIndex < keyCount ? keys[pageIndex] : null;
                                }
                                if (targetIndex < pageSize - 1) {
                                    keyStorage[targetIndex] = key;
                                    assert targetIndex == 0 || map.compare(keyStorage[targetIndex-1], key) < 0;
                                } else {
                                    anchorKey = key;
                                }
                                ++targetIndex;
                            } else {
                                ++pageIndex;
                                break;
                            }
                        }
                    }
                    if (targetIndex == pageSize) {
                        KVMapping<K, V>[] pageBuffer = null;
                        if (bufferFromIndex != bufferToIndex) {
                            pageBuffer = createBuffer(bufferToIndex - bufferFromIndex);
                            System.arraycopy(merged, bufferFromIndex, pageBuffer, 0, pageBuffer.length);
                            bufferFromIndex = bufferToIndex;
                            assert anchorKey == null || map.getKeyType().compare(pageBuffer[pageBuffer.length - 1].key, anchorKey) < 0;
                        }

                        if (++nextPageIndex == decreaseAtIndex) {
                            --pageSize;
                        }
                        Page<K, V> page;
                        if (keyStorage.length == 0) {
                            page = refStorage[0].getReferencedPage(map);
                            assert pageBuffer == null;
                        } else {
                            page = createNode(map, keyStorage, refStorage, pageBuffer);
                        }
                        if (page.getTotalCount() == 0) {
                            --result;
                            if (nextPageIndex == decreaseAtIndex && nextPageIndex < numberOfPages) {
                                keyStorage = createKeyStorage(pageSize - 1);
                                refStorage = PageReference.createRefStorage(pageSize);
                            }
                        } else {
                            pageHolder[topHolderPos++] = page;
                            unsavedMemoryHolder.value += page.getMemory();
                            if (nextPageIndex < numberOfPages) {
                                assert map.getKeyType().compare(page.getKey(page.getKeyCount() - 1), anchorKey) < 0;
                                keyHolder[topHolderPos] = anchorKey;
                                assert nextPageIndex - (numberOfPages - result) == 1 || map.compare(keyHolder[topHolderPos - 1], anchorKey) < 0;
                                keyStorage = createKeyStorage(pageSize - 1);
                                refStorage = PageReference.createRefStorage(pageSize);
                            }
                        }
                        targetIndex = 0;
                    }
                }
            }
            assert targetIndex == 0;
            assert nextPageIndex == numberOfPages;
            System.arraycopy(keyHolder, topHolderPos - result, keyHolder, holderPosition, result);
            System.arraycopy(pageHolder, topHolderPos - result, pageHolder, holderPosition, result);
            assert validateResult(keyHolder, holderPosition, result);
            return result;
        }

        private boolean validateResult(K[] keyHolder, int holderPosition, int result) {
            for (int i = 2; i < result; i++) {
                assert map.compare(keyHolder[holderPosition + i - 1], keyHolder[holderPosition + i]) < 0;
            }
            return true;
        }

        long calculateTotalCount() {
            long count = super.calculateTotalCount();
            if (buffer != null) {
                for (KVMapping<K, V> kvMapping : buffer) {
                    if (kvMapping.value == null) {
                        assert kvMapping.existing;
                        --count;
                    } else if (!kvMapping.existing) {
                        ++count;
                    }
                }
            }
            return count;
        }

        @Override
        public void dump(StringBuilder buff) {
            super.dump(buff);
            if (buffer != null) {
                buff.append(", buff:").append(buffer.length);
            }
        }
    }

    private static final class IncompleteNonLeaf<K,V> extends BufferedNonLeaf<K,V> {

        private boolean complete;

        IncompleteNonLeaf(MVMap<K,V> map, NonLeaf<K,V> source) {
            super(map, source);
        }

        @Override
        void writeUnsavedRecursive(Chunk chunk, WriteBuffer buff, List<Long> toc) {
            if (complete) {
                super.writeUnsavedRecursive(chunk, buff, toc);
            } else if (!isSaved()) {
                writeChildrenRecursive(chunk, buff, toc);
            }
        }

        @Override
        public boolean isComplete() {
            return complete;
        }

        @Override
        public void setComplete() {
            recalculateTotalCount();
            complete = true;
        }

        @Override
        public void dump(StringBuilder buff) {
            super.dump(buff);
            buff.append(", complete:").append(complete);
        }
    }

    private static final class Leaf<K,V> extends Page<K,V> {
        /**
         * The storage for values.
         */
        private V[] values;

        Leaf(MVMap<K,V> map) {
            super(map);
        }

        private Leaf(MVMap<K,V> map, Leaf<K,V> source) {
            super(map, source);
            this.values = source.values;
        }

        Leaf(MVMap<K,V> map, K[] keys, V[] values) {
            super(map, keys);
            this.values = values;
        }

        @Override
        public int getNodeType() {
            return PAGE_TYPE_LEAF;
        }

        @Override
        public Page<K,V> copy(MVMap<K, V> map, boolean eraseChildrenRefs) {
            return new Leaf<>(map, this);
        }

        @Override
        public Page<K,V> getChildPage(int index) {
            throw new UnsupportedOperationException();
        }

        @Override
        public long getChildPagePos(int index) {
            throw new UnsupportedOperationException();
        }

        @Override
        public V getValue(int index) {
            return values == null ? null : values[index];
        }

        @Override
        int acceptMappings(KVMapping<K, V>[] buffer,
                           K[] keyHolder, Page<K, V>[] pageHolder,
                           Collection<Page<K, V>> removedPages, MVMap.IntValueHolder unsavedMemoryHolder) {
            return acceptMappings(buffer, 0, buffer.length, keyHolder, pageHolder, 0, removedPages, unsavedMemoryHolder);
        }

        @Override
        int acceptMappings(KVMapping<K, V>[] buffer, int bufferPosition, int bufferCount,
                           K[] keyHolder, Page<K, V>[] pageHolder, int holderPosition,
                           Collection<Page<K, V>> removedPages, MVMap.IntValueHolder unsavedMemoryHolder) {
            removedPages.add(this);
            int keyCount = getKeyCount();
            int totalKeyCount = calculateTotalKeyCount(keyCount, buffer, bufferPosition, bufferCount);
            if (totalKeyCount == 0) {
                return 0;
            }
            int keyPerPageLimit = calculateKeyPerLeafLimit();
            int numberOfPages = (totalKeyCount + keyPerPageLimit - 1) / keyPerPageLimit;
            totalKeyCount += numberOfPages;
            int pageSize = (totalKeyCount - 1) / numberOfPages;
            totalKeyCount -= pageSize * numberOfPages;
            int pageIndex = -1;
            --bufferPosition;
            DataType<K> comparator = map.getKeyType();
            int compare = 0;
            K pageKey = null;
            K bufferKey = null;
            for (int nextPageIndex = 0; nextPageIndex < numberOfPages; nextPageIndex++) {
                if (nextPageIndex == totalKeyCount) {
                    --pageSize;
                }
                K[] keyStorage = createKeyStorage(pageSize);
                V[] valueStorage = createValueStorage(pageSize);
                int combinedIndex = 0;
                do {
                    if (compare >= 0) {
                        pageKey = ++pageIndex >= keyCount ? null : getKey(pageIndex);
                    }
                    if (compare <= 0) {
                        bufferKey = --bufferCount < 0 ? null : buffer[++bufferPosition].key;
                    }
                    compare = pageKey == null ? -1 :
                                bufferKey == null ? 1 :
                                comparator.compare(bufferKey, pageKey);
                    if (compare > 0) {
                        keyStorage[combinedIndex] = pageKey;
                        valueStorage[combinedIndex++] = getValue(pageIndex);
                    } else if (buffer[bufferPosition].value != null) {
                        keyStorage[combinedIndex] = bufferKey;
                        valueStorage[combinedIndex++] = buffer[bufferPosition].value;
                    } else {
                        assert compare == 0;
                    }
                } while (combinedIndex < pageSize);
                Page<K,V> page = Page.createLeaf(map, keyStorage, valueStorage, 0);
                keyHolder[holderPosition] = keyStorage[0];
                pageHolder[holderPosition++] = page;
                unsavedMemoryHolder.value += page.getMemory();
            }
            return numberOfPages;
        }

        private static <K,V> int calculateTotalKeyCount(int keyCount, KVMapping<K, V>[] buffer,
                                                        int bufferPosition, int bufferCount) {
            int totalKeyCount = keyCount + bufferCount;
            for (int index = bufferPosition; index < bufferPosition + bufferCount; index++) {
                KVMapping<K, V> kvMapping = buffer[index];
                if (kvMapping.existing) {
                    --totalKeyCount;
                    if (kvMapping.value == null) {
                        --totalKeyCount;
                    }
                }
            }
            assert totalKeyCount >= 0;
            return totalKeyCount;
        }

        @Override
        public Page<K,V> split(int at) {
            assert !isSaved();
            int b = getKeyCount() - at;
            K[] bKeys = splitKeys(at, b);
            V[] bValues = createValueStorage(b);
            if(values != null) {
                V[] aValues = createValueStorage(at);
                System.arraycopy(values, 0, aValues, 0, at);
                System.arraycopy(values, at, bValues, 0, b);
                values = aValues;
            }
            Page<K,V> newPage = createLeaf(map, bKeys, bValues, 0);
            if(isPersistent()) {
                recalculateMemory();
            }
            return newPage;
        }

        @Override
        public void expand(int extraKeyCount, K[] extraKeys, V[] extraValues) {
            int keyCount = getKeyCount();
            expandKeys(extraKeyCount, extraKeys);
            if(values != null) {
                V[] newValues = createValueStorage(keyCount + extraKeyCount);
                if (extraValues != null) {
                    System.arraycopy(values, 0, newValues, 0, keyCount);
                    System.arraycopy(extraValues, 0, newValues, keyCount, extraKeyCount);
                }
                values = newValues;
            }
            if(isPersistent()) {
                recalculateMemory();
            }
        }

        @Override
        public long getTotalCount() {
            return getKeyCount();
        }

        @Override
        long getCounts(int index) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void setChild(int index, Page<K,V> c) {
            throw new UnsupportedOperationException();
        }

        @Override
        public V setValue(int index, V value) {
            values = values.clone();
            V old = setValueInternal(index, value);
            if(isPersistent()) {
                if (!map.isMemoryEstimationAllowed()) {
                    addMemory(map.evaluateMemoryForValue(value) -
                            map.evaluateMemoryForValue(old));
                }
            }
            return old;
        }

        private V setValueInternal(int index, V value) {
            V old = values[index];
            values[index] = value;
            return old;
        }

        @Override
        public void insertLeaf(int index, K key, V value) {
            int keyCount = getKeyCount();
            insertKey(index, key);

            if(values != null) {
                V[] newValues = createValueStorage(keyCount + 1);
                DataUtils.copyWithGap(values, newValues, keyCount, index);
                values = newValues;
                setValueInternal(index, value);
                if (isPersistent()) {
                    addMemory(MEMORY_POINTER + map.evaluateMemoryForValue(value));
                }
            }
        }

        @Override
        public void insertNode(int index, K key, Page<K,V> childPage) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void remove(int index) {
            int keyCount = getKeyCount();
            super.remove(index);
            if (values != null) {
                if(isPersistent()) {
                    if (map.isMemoryEstimationAllowed()) {
                        addMemory(-getMemory() / keyCount);
                    } else {
                        V old = getValue(index);
                        addMemory(-MEMORY_POINTER - map.evaluateMemoryForValue(old));
                    }
                }
                V[] newValues = createValueStorage(keyCount - 1);
                DataUtils.copyExcept(values, newValues, keyCount, index);
                values = newValues;
            }
        }

        @Override
        public int removeAllRecursive(long version) {
            return removePage(version);
        }

        public int normalizeIndex(int index, boolean reverse) {
            if (index < 0) {
                index = ~index;
                if (reverse) {
                    --index;
                }
            }
            return index;
        }

        @Override
        public CursorPos<K,V> getPrependCursorPos(CursorPos<K,V> cursorPos) {
            return new CursorPos<>(this, -1, cursorPos);
        }

        @Override
        public CursorPos<K,V> getAppendCursorPos(CursorPos<K,V> cursorPos) {
            int keyCount = getKeyCount();
            return new CursorPos<>(this, ~keyCount, cursorPos);
        }

        @Override
        protected void readChildren(ByteBuffer buff) {}

        @Override
        protected void readPayLoad(ByteBuffer buff, byte[] flags) {
            int keyCount = getKeyCount();
            values = createValueStorage(keyCount);
            map.getValueType().read(buff, values, keyCount);
        }

        @Override
        protected void writePayload(WriteBuffer buff) {
            map.getValueType().write(buff, values, getKeyCount());
        }

        @Override
        protected void writeChildren(WriteBuffer buff, boolean withCounts) {}

        @Override
        void writeUnsavedRecursive(Chunk chunk, WriteBuffer buff, List<Long> toc) {
            if (!isSaved()) {
                write(chunk, buff, toc);
            }
        }

        @Override
        void releaseSavedPages() {}

        @Override
        public int getRawChildPageCount() {
            return 0;
        }

        @Override
        protected int calculateMemory() {
//*
            return super.calculateMemory() + PAGE_LEAF_MEMORY +
                    (values == null ? 0 : map.evaluateMemoryForValues(values, getKeyCount()));
/*/
            int keyCount = getKeyCount();
            int mem = super.calculateMemory() + PAGE_LEAF_MEMORY + keyCount * MEMORY_POINTER;
            DataType<V> valueType = map.getValueType();
            for (int i = 0; i < keyCount; i++) {
                mem += getMemory(valueType, values[i]);
            }
            return mem;
//*/
        }

        @Override
        public void dump(StringBuilder buff) {
            super.dump(buff);
            int keyCount = getKeyCount();
            for (int i = 0; i < keyCount; i++) {
                if (i > 0) {
                    buff.append(" ");
                }
                buff.append(getKey(i));
                if (values != null) {
                    buff.append(':');
                    buff.append(getValue(i));
                }
            }
        }
    }
}
