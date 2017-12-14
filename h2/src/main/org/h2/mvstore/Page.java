/*
 * Copyright 2004-2014 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.mvstore;

import java.lang.reflect.Array;
import java.nio.ByteBuffer;
import java.util.Arrays;
import org.h2.compress.Compressor;
import org.h2.engine.Constants;
import org.h2.mvstore.type.ExtendedDataType;
import static org.h2.mvstore.DataUtils.PAGE_TYPE_LEAF;

/**
 * A page (a node or a leaf).
 * <p>
 * For b-tree nodes, the key at a given index is larger than the largest key of
 * the child at the same index.
 * <p>
 * File format:
 * page length (including length): int
 * check value: short
 * map id: varInt
 * number of keys: varInt
 * type: byte (0: leaf, 1: node; +2: compressed)
 * compressed: bytes saved (varInt)
 * keys
 * leaf: values (one for each key)
 * node: children (1 more than keys)
 */
public abstract class Page implements Cloneable {

    /**
     * Marker value for memory field, meaning that memory accounting is replaced by key count.
     */
    private static final int IN_MEMORY = Integer.MIN_VALUE;


    /**
     * Map this page belongs to
     */
    protected final MVMap<?, ?> map;

    /**
     * Position of this page's saved image within a Chunk or 0 if this page has not been saved yet.
     */
    private long pos;

    /**
     * The last result of a find operation is cached.
     */
    private int cachedCompare;

    /**
     * The estimated memory used in persistent case, IN_MEMORY marker value otherwise.
     */
    private int memory;

    /**
     * Number of keys stored on this page
     */
    private int keyCount;

    /**
     * The storage for keys.
     */
    private Object keys;

    /**
     * Whether the page is an in-memory (not stored, or not yet stored) page,
     * and it is removed. This is to keep track of pages that concurrently
     * changed while they are being stored, in which case the live bookkeeping
     * needs to be aware of such cases.
     */
    private volatile boolean removedInMemory;


    private Page(MVMap<?, ?> map) {
        this.map = map;
    }

    private Page(MVMap<?, ?> map, Page source) {
        this(map, source.keyCount, source.keys);
        memory = source.memory;
    }

    private Page(MVMap<?, ?> map, int keyCount, Object keys) {
        this.map = map;
        this.keyCount = keyCount;
        this.keys = keys;
    }

    /**
     * Create a new, empty page.
     *
     * @param map the map
     * @return the new page
     */
    static Page createEmptyLeaf(MVMap<?, ?> map, boolean capable) {
        int capacity = capable ? map.getStore().getKeysPerPage() : 0;
        Object keys = map.getKeyType() != map.getExtendedKeyType() ? new Object[capacity] : //EMPTY_OBJECT_ARRAY :
                                                                     map.getExtendedKeyType().createStorage(capacity);
        Object values = map.getValueType() != map.getExtendedValueType() ? new Object[capacity] : //EMPTY_OBJECT_ARRAY :
                                                                     map.getExtendedValueType().createStorage(capacity);
        return create(map, 0, keys, values, null, 0, DataUtils.PAGE_LEAF_EMPTY_MEMORY);
    }

    public static Page createEmptyNode(MVMap<?, ?> map) {
        return createEmptyNode(map, false);
    }

    public static Page createEmptyNode(MVMap<?, ?> map, boolean capable) {
        int capacity = capable ? map.getStore().getKeysPerPage() : 0;
        Object keys = map.getKeyType() != map.getExtendedKeyType() ? new Object[capacity] : //EMPTY_OBJECT_ARRAY :
                                                                     map.getExtendedKeyType().createStorage(capacity);
        return create(map, 0, keys, null, PageReference.SINGLE_EMPTY, 0, DataUtils.PAGE_LEAF_EMPTY_MEMORY);
    }

    /**
     * Create a new page. The arrays are not cloned.
     *
     * @param map the map
     * @param keyCount number of keys
     * @param keys the keys
     * @param values the values
     * @param children the child page positions
     * @param totalCount the total number of keys
     * @param memory the memory used in bytes
     * @return the page
     */
    public static Page create(MVMap<?, ?> map, int keyCount, Object keys,
                              Object values, PageReference[] children,
                              long totalCount, int memory) {
        assert keys != null;
        Page p = children == null ? new Leaf(map, keyCount, keys, values) :
                                    new NonLeaf(map, keyCount, keys, children, totalCount);
        // the position is 0
        MVStore store = map.store;
        if(store.getFileStore() == null) {
            p.memory = IN_MEMORY;
        } else if (memory == 0) {
            p.recalculateMemory();
        } else {
            p.addMemory(memory);
            assert memory == p.getMemory();
        }
        return p;
    }

    /**
     * Read a page.
     *
     * @param fileStore the file store
     * @param pos the position
     * @param map the map
     * @param filePos the position in the file
     * @param maxPos the maximum position (the end of the chunk)
     * @return the page
     */
    static Page read(FileStore fileStore, long pos, MVMap<?, ?> map,
            long filePos, long maxPos) {
        ByteBuffer buff;
        int maxLength = DataUtils.getPageMaxLength(pos);
        if (maxLength == DataUtils.PAGE_LARGE) {
            buff = fileStore.readFully(filePos, 128);
            maxLength = buff.getInt();
            // read the first bytes again
        }
        maxLength = (int) Math.min(maxPos - filePos, maxLength);
        int length = maxLength;
        if (length < 0) {
            throw DataUtils.newIllegalStateException(
                    DataUtils.ERROR_FILE_CORRUPT,
                    "Illegal page length {0} reading at {1}; max pos {2} ",
                    length, filePos, maxPos);
        }
        buff = fileStore.readFully(filePos, length);
        boolean leaf = (DataUtils.getPageType(pos) & 1) == PAGE_TYPE_LEAF;
        Page p = leaf ? new Leaf(map) : new NonLeaf(map);
        p.pos = pos;
        int chunkId = DataUtils.getPageChunkId(pos);
        int offset = DataUtils.getPageOffset(pos);
        p.read(buff, chunkId, offset, maxLength);
        return p;
    }

    /**
     * Read an inner node page from the buffer, but ignore the keys and
     * values.
     *
     * @param fileStore the file store
     * @param pos the position
     * @param filePos the position in the file
     * @param maxPos the maximum position (the end of the chunk)
     */
    static void readChildrensPositions(FileStore fileStore, long pos,
                                       long filePos, long maxPos,
                                       MVStore.ChunkIdsCollector collector) {
        ByteBuffer buff;
        int maxLength = DataUtils.getPageMaxLength(pos);
        if (maxLength == DataUtils.PAGE_LARGE) {
            buff = fileStore.readFully(filePos, 128);
            maxLength = buff.getInt();
            // read the first bytes again
        }
        maxLength = (int) Math.min(maxPos - filePos, maxLength);
        int length = maxLength;
        if (length < 0) {
            throw DataUtils.newIllegalStateException(
                    DataUtils.ERROR_FILE_CORRUPT,
                    "Illegal page length {0} reading at {1}; max pos {2} ",
                    length, filePos, maxPos);
        }
        buff = fileStore.readFully(filePos, length);
        int chunkId = DataUtils.getPageChunkId(pos);
        int offset = DataUtils.getPageOffset(pos);
        int start = buff.position();
        int pageLength = buff.getInt();
        if (pageLength > maxLength) {
            throw DataUtils.newIllegalStateException(
                    DataUtils.ERROR_FILE_CORRUPT,
                    "File corrupted in chunk {0}, expected page length =< {1}, got {2}",
                    chunkId, maxLength, pageLength);
        }
        buff.limit(start + pageLength);
        short check = buff.getShort();
        int m = DataUtils.readVarInt(buff);
        int mapId = collector.getMapId();
        if (m != mapId) {
            throw DataUtils.newIllegalStateException(
                    DataUtils.ERROR_FILE_CORRUPT,
                    "File corrupted in chunk {0}, expected map id {1}, got {2}",
                    chunkId, mapId, m);
        }
        int checkTest = DataUtils.getCheckValue(chunkId)
                ^ DataUtils.getCheckValue(offset)
                ^ DataUtils.getCheckValue(pageLength);
        if (check != (short) checkTest) {
            throw DataUtils.newIllegalStateException(
                    DataUtils.ERROR_FILE_CORRUPT,
                    "File corrupted in chunk {0}, expected check value {1}, got {2}",
                    chunkId, checkTest, check);
        }
        int len = DataUtils.readVarInt(buff);
        int type = buff.get();
        if ((type & 1) != DataUtils.PAGE_TYPE_NODE) {
            throw DataUtils.newIllegalStateException(
                    DataUtils.ERROR_FILE_CORRUPT,
                    "Position {0} expected to be a non-leaf", pos);
        }
        for (int i = 0; i <= len; i++) {
            collector.visit(buff.getLong());
        }
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
     * This is used exclusively during bulk map copiing.
     * Child page references for nodes are cleared (repointed to an empty page)
     * to be filled-in later to copiing procedure. This way it can be saved
     * mid-process without tree integrity violation
     *
     * @param map new map to own resulting page
     * @return the page
     */
    abstract Page copy(MVMap<?, ?> map);

    /**
     * Get the key at the given index.
     *
     * @param index the index
     * @return the key
     */
    public final Object getKey(int index) {
        return map.getExtendedKeyType().getValue(keys, index);
    }

    /**
     * Get the child page at the given index.
     *
     * @param index the index
     * @return the child page
     */
    public abstract Page getChildPage(int index);

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
    public abstract Object getValue(int index);

    /**
     * Get the number of keys in this page.
     *
     * @return the number of keys
     */
    public final int getKeyCount() {
        return keyCount;
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

    /**
     * Get the value for the given key, or null if not found.
     * Search is done in the tree rooted at given page.
     *
     * @param key the key
     * @param p the root page
     * @return the value, or null if not found
     */
    public static Object get(Page p, Object key) {
        while (true) {
            int index = p.binarySearch(key);
            if (p.isLeaf()) {
                return index >= 0 ? p.getValue(index) : null;
            } else if (index++ < 0) {
                index = -index;
            }
            p = p.getChildPage(index);
        }
    }

    @Override
    public String toString() {
        StringBuilder buff = new StringBuilder();
        dump(buff);
        return buff.toString();
    }

    protected void dump(StringBuilder buff) {
        buff.append("id: ").append(System.identityHashCode(this)).append('\n');
        buff.append("pos: ").append(Long.toHexString(pos)).append("\n");
        if (isSaved()) {
            int chunkId = DataUtils.getPageChunkId(pos);
            buff.append("chunk: ").append(Long.toHexString(chunkId)).append("\n");
        }
    }

    /**
     * Create a copy of this page.
     *
     * @return a mutable copy of this page
     */
    public final Page copy() {
        return copy(false);
    }

    public final Page copy(boolean countRemoval) {
        Page newPage = clone();
        newPage.pos = 0;
        // mark the old as deleted
        if(countRemoval) {
            removePage();
            if(isPersistent()) {
                map.store.registerUnsavedPage(newPage.getMemory());
            }
        }
        return newPage;
    }

    @Override
    protected final Page clone() {
        Page clone;
        try {
            clone = (Page) super.clone();
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
    public final int binarySearch(Object key) {
        int result = map.getExtendedKeyType().binarySearch(key, keys, keyCount, cachedCompare);
        cachedCompare = result < 0 ? -(result + 1) : result + 1;
        return result;
    }

    /**
     * Split the page. This modifies the current page.
     *
     * @param at the split index
     * @return the page with the entries after the split index
     */
    abstract Page split(int at);

    @SuppressWarnings("SuspiciousSystemArraycopy")
    protected final Object splitKeys(int aCount, int bCount) {
        assert aCount + bCount <= getKeyCount();
        Object aKeys = createKeyStorage(aCount);
        Object bKeys = createKeyStorage(bCount);
        System.arraycopy(keys, 0, aKeys, 0, aCount);
        System.arraycopy(keys, getKeyCount() - bCount, bKeys, 0, bCount);
        keyCount = aCount;
        keys = aKeys;
        return bKeys;
    }

    /**
     * Get the total number of key-value pairs, including child pages.
     *
     * @return the number of key-value pairs
     */
    public abstract long getTotalCount();

    /**
     * Get the descendant counts for the given child.
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
    public abstract void setChild(int index, Page c);

    /**
     * Replace the key at an index in this page.
     *
     * @param index the index
     * @param key the new key
     */
    public final void setKey(int index, Object key) {
        ExtendedDataType extendedKeyType = map.getExtendedKeyType();
        keys = extendedKeyType.clone(keys);
        if(isPersistent()) {
            int mem = extendedKeyType.getMemory(key);
            Object old = extendedKeyType.getValue(keys, index);
            if (old != null) {
                mem -= extendedKeyType.getMemory(old);
            }
            addMemory(mem);
        }
        extendedKeyType.setValue(keys, index, key);
    }

    /**
     * Replace the value at an index in this page.
     *
     * @param index the index
     * @param value the new value
     * @return the old value
     */
    public abstract Object setValue(int index, Object value);

    /**
     * Insert a key-value pair into this leaf.
     *
     * @param index the index
     * @param key the key
     * @param value the value
     */
    public abstract void insertLeaf(int index, Object key, Object value);

    /**
     * Insert a child page into this node.
     *
     * @param index the index
     * @param key the key
     * @param childPage the child page
     */
    public abstract void insertNode(int index, Object key, Page childPage);

    public boolean hasCapacity() {
        ExtendedDataType keyType = map.getExtendedKeyType();
        int capacity = keyType.getCapacity(keys);
        return getKeyCount() < capacity;
    }

    public boolean canInsert() {
        return getKeyCount() < map.store.getKeysPerPage();
    }

    @SuppressWarnings("SuspiciousSystemArraycopy")
    public void extendCapacity() {
        Object newKeys = createKeyStorage(map.store.getKeysPerPage());
        System.arraycopy(keys, 0, newKeys, 0, keyCount);
        keys = newKeys;
    }

    @SuppressWarnings("SuspiciousSystemArraycopy")
    protected final void insertKey(int index, Object key) {
        assert index <= keyCount : index + " > " + keyCount;
        ExtendedDataType keyType = map.getExtendedKeyType();
        int capacity = keyType.getCapacity(keys);
        assert capacity == Array.getLength(keys);
        if (index != keyCount || index == capacity || !map.isSingleWriter() || keyType.getValue(keys, index) != null) {
            Object newKeys = createKeyStorage(keyCount + 1);
            DataUtils.copyWithGap(keys, newKeys, keyCount, index);
            keys = newKeys;
        }

        ++keyCount;
        keyType.setValue(keys, index, key);

        if (isPersistent()) {
            addMemory(keyType.getMemory(key));
        }
    }

    /**
     * Remove the key and value (or child) at the given index.
     *
     * @param index the index
     */
    public void remove(int index) {
        ExtendedDataType keyType = map.getExtendedKeyType();
        int indx = index;
        if (indx == keyCount) {
            --indx;
        }
        if(isPersistent()) {
            Object old = getKey(indx);
            addMemory(-keyType.getMemory(old));
        }
        if (index != keyCount || !map.isSingleWriter()) {
            Object newKeys = createKeyStorage(keyCount - 1);
            DataUtils.copyExcept(keys, newKeys, keyCount, indx);
            keys = newKeys;
        }
        --keyCount;
    }

    /**
     * Read the page from the buffer.
     *
     * @param buff the buffer
     * @param chunkId the chunk id
     * @param offset the offset within the chunk
     * @param maxLength the maximum length
     */
    private void read(ByteBuffer buff, int chunkId, int offset, int maxLength) {
        int start = buff.position();
        int pageLength = buff.getInt();
        if (pageLength > maxLength || pageLength < 4) {
            throw DataUtils.newIllegalStateException(
                    DataUtils.ERROR_FILE_CORRUPT,
                    "File corrupted in chunk {0}, expected page length 4..{1}, got {2}",
                    chunkId, maxLength, pageLength);
        }
        buff.limit(start + pageLength);
        short check = buff.getShort();
        int mapId = DataUtils.readVarInt(buff);
        if (mapId != map.getId()) {
            throw DataUtils.newIllegalStateException(
                    DataUtils.ERROR_FILE_CORRUPT,
                    "File corrupted in chunk {0}, expected map id {1}, got {2}",
                    chunkId, map.getId(), mapId);
        }
        int checkTest = DataUtils.getCheckValue(chunkId)
                ^ DataUtils.getCheckValue(offset)
                ^ DataUtils.getCheckValue(pageLength);
        if (check != (short) checkTest) {
            throw DataUtils.newIllegalStateException(
                    DataUtils.ERROR_FILE_CORRUPT,
                    "File corrupted in chunk {0}, expected check value {1}, got {2}",
                    chunkId, checkTest, check);
        }
        keyCount = DataUtils.readVarInt(buff);
        keys = createKeyStorage(keyCount);
        int type = buff.get();
        if(isLeaf() != ((type & 1) == PAGE_TYPE_LEAF)) {
            throw DataUtils.newIllegalStateException(
                    DataUtils.ERROR_FILE_CORRUPT,
                    "File corrupted in chunk {0}, expected node type {1}, got {2}",
                    chunkId, isLeaf() ? "0" : "1" , type);
        }
        if (!isLeaf()) {
            readPayLoad(buff);
        }
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
            int compLen = pageLength + start - buff.position();
            byte[] comp = DataUtils.newBytes(compLen);
            buff.get(comp);
            int l = compLen + lenAdd;
            buff = ByteBuffer.allocate(l);
            compressor.expand(comp, 0, compLen, buff.array(),
                    buff.arrayOffset(), l);
        }
        map.getExtendedKeyType().read(buff, keys, keyCount);
        if (isLeaf()) {
            readPayLoad(buff);
        }
        recalculateMemory();
    }

    protected abstract void readPayLoad(ByteBuffer buff);

    public final boolean isSaved() {
        return DataUtils.isPageSaved(pos);
    }

    /**
     * Store the page and update the position.
     *
     * @param chunk the chunk
     * @param buff the target buffer
     * @return the position of the buffer just after the type
     */
    protected final int write(Chunk chunk, WriteBuffer buff) {
        int start = buff.position();
        int len = getKeyCount();
        int type = isLeaf() ? PAGE_TYPE_LEAF : DataUtils.PAGE_TYPE_NODE;
        buff.putInt(0).
            putShort((byte) 0).
            putVarInt(map.getId()).
            putVarInt(len);
        int typePos = buff.position();
        buff.put((byte) type);
        writeChildren(buff, true);
        int compressStart = buff.position();
        map.getExtendedKeyType().writeStorage(buff, keys, keyCount);
        writeValues(buff);
        MVStore store = map.getStore();
        int expLen = buff.position() - compressStart;
        if (expLen > 16) {
            int compressionLevel = store.getCompressionLevel();
            if (compressionLevel > 0) {
                Compressor compressor;
                int compressType;
                if (compressionLevel == 1) {
                    compressor = map.getStore().getCompressorFast();
                    compressType = DataUtils.PAGE_COMPRESSED;
                } else {
                    compressor = map.getStore().getCompressorHigh();
                    compressType = DataUtils.PAGE_COMPRESSED_HIGH;
                }
                byte[] exp = new byte[expLen];
                buff.position(compressStart).get(exp);
                byte[] comp = new byte[expLen * 2];
                int compLen = compressor.compress(exp, expLen, comp, 0);
                int plus = DataUtils.getVarIntLen(compLen - expLen);
                if (compLen + plus < expLen) {
                    buff.position(typePos).
                        put((byte) (type + compressType));
                    buff.position(compressStart).
                        putVarInt(expLen - compLen).
                        put(comp, 0, compLen);
                }
            }
        }
        int pageLength = buff.position() - start;
        int chunkId = chunk.id;
        int check = DataUtils.getCheckValue(chunkId)
                ^ DataUtils.getCheckValue(start)
                ^ DataUtils.getCheckValue(pageLength);
        buff.putInt(start, pageLength).
            putShort(start + 4, (short) check);
        if (isSaved()) {
            throw DataUtils.newIllegalStateException(
                    DataUtils.ERROR_INTERNAL, "Page already stored");
        }
        pos = DataUtils.getPagePos(chunkId, start, pageLength, type);
        store.cachePage(this);
        if (type == DataUtils.PAGE_TYPE_NODE) {
            // cache again - this will make sure nodes stays in the cache
            // for a longer time
            store.cachePage(this);
        }
        long max = DataUtils.getPageMaxLength(pos);
        chunk.maxLen += max;
        chunk.maxLenLive += max;
        chunk.pageCount++;
        chunk.pageCountLive++;
        if (removedInMemory) {
            // if the page was removed _before_ the position was assigned, we
            // need to mark it removed here, so the fields are updated
            // when the next chunk is stored
            map.removePage(pos, memory);
        }
        return typePos + 1;
    }

    protected abstract void writeValues(WriteBuffer buff);

    protected abstract void writeChildren(WriteBuffer buff, boolean withCounts);

    /**
     * Store this page and all children that are changed, in reverse order, and
     * update the position and the children.
     *
     * @param chunk the chunk
     * @param buff the target buffer
     */
    abstract void writeUnsavedRecursive(Chunk chunk, WriteBuffer buff);

    /**
     * Unlink the children recursively after all data is written.
     */
    abstract void writeEnd();

    public abstract int getRawChildPageCount();

    @Override
    public final boolean equals(Object other) {
        return other == this || other instanceof Page && isSaved() && ((Page) other).pos == pos;
    }

    @Override
    public final int hashCode() {
        return isSaved() ? (int) (pos | (pos >>> 32)) : super.hashCode();
    }

    protected final boolean isPersistent() {
        return memory != IN_MEMORY;
    }

    public final int getMemory() {
        if (isPersistent()) {
            if (MVStore.ASSERT) {
                int mem = memory;
                recalculateMemory();
                if (mem != memory) {
                    throw DataUtils.newIllegalStateException(
                            DataUtils.ERROR_INTERNAL, "Memory calculation error {0} != {1}", mem, memory);
                }
            }
            return memory;
        }
        return 0; //getKeyCount();
    }

    protected final void addMemory(int mem) {
        memory += mem;
    }

    protected void recalculateMemory() {
        assert isPersistent();
        memory = Constants.MEMORY_ARRAY + map.getExtendedKeyType().getMemorySize(keys, keyCount);
    }

    /**
     * Remove the page.
     */
    public final void removePage() {
        if(isPersistent()) {
            long p = pos;
            if (p == 0) {
                removedInMemory = true;
            }
            map.removePage(p, memory);
        }
    }

    public abstract void removeAllRecursive();

    public abstract CursorPos getAppendCursorPos(CursorPos cursorPos);

    private Object createKeyStorage(int size)
    {
        return map.getExtendedKeyType().createStorage(size);
    }

    protected final Object createValueStorage(int size)
    {
        return map.getExtendedValueType().createStorage(size);
    }

    /**
     * A pointer to a page, either in-memory or using a page position.
     */
    public static final class PageReference {

        public static final PageReference EMPTY = new PageReference(null, 0, 0);
        private static final PageReference SINGLE_EMPTY[] = { EMPTY };

        /**
         * The position, if known, or 0.
         */
        final long pos;

        /**
         * The page, if in memory, or null.
         */
        private final Page page;

        /**
         * The descendant count for this child page.
         */
        private final long count;

        public PageReference(Page page) {
            this(page, page.pos, page.getTotalCount());
        }

        private PageReference(long pos, long count) {
            this(null, pos, count);
        }

        private PageReference(Page page, long pos, long count) {
            this.page = page;
            this.pos = pos;
            this.count = count;
        }

        @Override
        public String toString() {
            return "Cnt:" + count + ", pos:" + DataUtils.getPageChunkId(pos) +
                    "-" + DataUtils.getPageOffset(pos) + ":" + DataUtils.getPageMaxLength(pos) +
                    (DataUtils.getPageType(pos) == 0 ? " leaf" : " node") + ", " + page;
        }
    }


    private static final class Leaf extends Page
    {
        /**
         * The storage for values.
         */
        private Object values;

        private Leaf(MVMap<?, ?> map) {
            super(map);
        }

        private Leaf(MVMap<?, ?> map, Leaf source) {
            super(map, source);
            this.values = source.values;
        }

        private Leaf(MVMap<?, ?> map, int keyCount, Object keys, Object values) {
            super(map, keyCount, keys);
            this.values = values;
        }

        @Override
        public int getNodeType() {
            return PAGE_TYPE_LEAF;
        }

        @Override
        public Page copy(MVMap<?, ?> map) {
            return new Leaf(map, this);
        }

        @Override
        public Page getChildPage(int index) {
            throw new UnsupportedOperationException();
        }

        @Override
        public long getChildPagePos(int index) {
            throw new UnsupportedOperationException();
        }

        @Override
        public Object getValue(int index) {
            return map.getExtendedValueType().getValue(values, index);
        }

        @Override
        @SuppressWarnings("SuspiciousSystemArraycopy")
        public Page split(int at) {
            assert !isSaved();
            int b = getKeyCount() - at;
            Object bKeys = splitKeys(at, b);
            Object bValues = createValueStorage(b);
            if(values != null) {
                Object aValues = createValueStorage(at);
                System.arraycopy(values, 0, aValues, 0, at);
                System.arraycopy(values, at, bValues, 0, b);
                values = aValues;
            }
            Page newPage = create(map, b, bKeys, bValues, null, b, 0);
            if(isPersistent()) {
                recalculateMemory();
            }
            return newPage;
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
        public void setChild(int index, Page c) {
            throw new UnsupportedOperationException();
        }

        @Override
        public Object setValue(int index, Object value) {
            ExtendedDataType valueType = map.getExtendedValueType();
            values = valueType.clone(values);
            Object old = setValueInternal(index, value);
            if(isPersistent()) {
                addMemory(valueType.getMemory(value) -
                          valueType.getMemory(old));
            }
            return old;
        }

        private Object setValueInternal(int index, Object value) {
            ExtendedDataType valueType = map.getExtendedValueType();
            Object old = valueType.getValue(values, index);
            valueType.setValue(values, index, value);
            return old;
        }

        @Override
        @SuppressWarnings("SuspiciousSystemArraycopy")
        public void insertLeaf(int index, Object key, Object value) {
            int keyCount = getKeyCount();
            insertKey(index, key);

            if(values != null) {
                ExtendedDataType valueType = map.getExtendedValueType();
                int capacity = valueType.getCapacity(values);
                assert capacity == Array.getLength(values);
                if (index != keyCount || index == capacity || !map.isSingleWriter() || valueType.getValue(values, index) != null) {
                    Object newValues = createValueStorage(keyCount + 1);
                    DataUtils.copyWithGap(values, newValues, keyCount, index);
                    values = newValues;
                }
                setValueInternal(index, value);
                if (isPersistent()) {
                    addMemory(map.getValueType().getMemory(value));
                }
            }
        }

        @Override
        public void insertNode(int index, Object key, Page childPage) {
            throw new UnsupportedOperationException();
        }

        @SuppressWarnings("SuspiciousSystemArraycopy")
        public void extendCapacity() {
            super.extendCapacity();
            if (values != null) {
                Object newValues = createValueStorage(map.store.getKeysPerPage());
                System.arraycopy(values, 0, newValues, 0, getKeyCount());
                values = newValues;
            }
        }

        @Override
        public void remove(int index) {
            int keyCount = getKeyCount();
            super.remove(index);
            if (values != null) {
                if(isPersistent()) {
                    Object old = getValue(index);
                    addMemory(-map.getValueType().getMemory(old));
                }
                if (index != keyCount || !map.isSingleWriter()) {
                    Object newValues = createValueStorage(keyCount - 1);
                    DataUtils.copyExcept(values, newValues, keyCount, index);
                    values = newValues;
                }
            }
        }

        @Override
        public void removeAllRecursive() {
            removePage();
        }

        @Override
        public CursorPos getAppendCursorPos(CursorPos cursorPos) {
            int keyCount = getKeyCount();
            return new CursorPos(this, -keyCount - 1, cursorPos);
        }

        @Override
        protected void readPayLoad(ByteBuffer buff) {
            int keyCount = getKeyCount();
            values = createValueStorage(keyCount);
            map.getExtendedValueType().read(buff, values, getKeyCount());
        }

        @Override
        protected void writeValues(WriteBuffer buff) {
            map.getExtendedValueType().writeStorage(buff, values, getKeyCount());
        }

        @Override
        protected void writeChildren(WriteBuffer buff, boolean withCounts) {}

        @Override
        void writeUnsavedRecursive(Chunk chunk, WriteBuffer buff) {
            if (!isSaved()) {
                write(chunk, buff);
            }
        }

        @Override
        void writeEnd() {}

        @Override
        public int getRawChildPageCount() {
            return 0;
        }

        @Override
        protected void recalculateMemory() {
            if(isPersistent()) {
                super.recalculateMemory();
                int mem = DataUtils.PAGE_LEAF_MEMORY + Constants.MEMORY_ARRAY +
                            map.getExtendedValueType().getMemorySize(values, getKeyCount());
                addMemory(mem);
            }
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

    private static final class NonLeaf extends Page
    {
        /**
         * The child page references.
         */
        private PageReference[] children;

        /**
        * The total entry count of this page and all children.
        */
       private long totalCount;

        private NonLeaf(MVMap<?, ?> map) {
            super(map);
        }

        private NonLeaf(MVMap<?, ?> map, NonLeaf source, PageReference[] children, long totalCount) {
            super(map, source);
            this.children = children;
            this.totalCount = totalCount;
        }

        private NonLeaf(MVMap<?, ?> map, int keyCount, Object keys, PageReference[] children, long totalCount) {
            super(map, keyCount, keys);
            this.children = children;
            this.totalCount = totalCount;
        }

        @Override
        public int getNodeType() {
            return DataUtils.PAGE_TYPE_NODE;
        }

        @Override
        public Page copy(MVMap<?, ?> map) {
            // replace child pages with empty pages
            PageReference[] children = new PageReference[this.children.length];
            Arrays.fill(children, PageReference.EMPTY);
            return new NonLeaf(map, this, children, 0);
        }

        @Override
        public Page getChildPage(int index) {
            PageReference ref = children[index];
            Page page = ref.page;
            if(page == null) {
                page = map.readPage(ref.pos);
                assert ref.pos == page.pos;
                assert ref.count == page.getTotalCount();
            }
            return page;
        }

        @Override
        public long getChildPagePos(int index) {
            return children[index].pos;
        }

        @Override
        public Object getValue(int index) {
            throw new UnsupportedOperationException();
        }

        @SuppressWarnings("SuspiciousSystemArraycopy")
        public Page split(int at) {
            assert !isSaved();
            int b = getKeyCount() - at;
            Object bKeys = splitKeys(at, b - 1);
            PageReference[] aChildren = new PageReference[at + 1];
            PageReference[] bChildren = new PageReference[b];
            System.arraycopy(children, 0, aChildren, 0, at + 1);
            System.arraycopy(children, at + 1, bChildren, 0, b);
            children = aChildren;

            long t = 0;
            for (PageReference x : aChildren) {
                t += x.count;
            }
            totalCount = t;
            t = 0;
            for (PageReference x : bChildren) {
                t += x.count;
            }
            Page newPage = create(map, b - 1, bKeys, null, bChildren, t, 0);
            if(isPersistent()) {
                recalculateMemory();
            }
            return newPage;
        }

        @Override
        public long getTotalCount() {
            if (MVStore.ASSERT) {
                long check = 0;
                int keyCount = getKeyCount();
                for (int i = 0; i <= keyCount; i++) {
                    check += children[i].count;
                }
                if (check != totalCount) {
                    throw DataUtils.newIllegalStateException(
                            DataUtils.ERROR_INTERNAL,
                            "Expected: {0} got: {1}", check, totalCount);
                }
            }
            return totalCount;
        }

        @Override
        long getCounts(int index) {
            return children[index].count;
        }

        @Override
        public void setChild(int index, Page c) {
            assert c != null;
            PageReference child = children[index];
            if (c != child.page || c.pos != child.pos) {
                totalCount += c.getTotalCount() - child.count;
                children = children.clone();
                children[index] = new PageReference(c);
            }
        }

        @Override
        public Object setValue(int index, Object value) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void insertLeaf(int index, Object key, Object value) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void insertNode(int index, Object key, Page childPage) {
            int childCount = getRawChildPageCount();
            insertKey(index, key);

            int capacity = children.length;
            if (index != childCount || index == capacity || !map.isSingleWriter() || children[index] != null) {
                PageReference[] newChildren = new PageReference[childCount + 1];
                DataUtils.copyWithGap(children, newChildren, childCount, index);
                children = newChildren;
            }
            children[index] = new PageReference(childPage);

            totalCount += childPage.getTotalCount();
            if (isPersistent()) {
                addMemory(Constants.MEMORY_POINTER + DataUtils.PAGE_MEMORY_CHILD);
            }
        }

        @SuppressWarnings("SuspiciousSystemArraycopy")
        public void extendCapacity() {
            super.extendCapacity();
            PageReference[] newChildren = new PageReference[map.store.getKeysPerPage() + 1];
            System.arraycopy(children, 0, newChildren, 0, getRawChildPageCount());
            children = newChildren;
        }

        @Override
        public void remove(int index) {
            int childCount = getRawChildPageCount();
            super.remove(index);
            if(isPersistent()) {
                addMemory(-(Constants.MEMORY_POINTER + DataUtils.PAGE_MEMORY_CHILD));
            }
            totalCount -= children[index].count;
            if (index != childCount || !map.isSingleWriter()) {
                PageReference[] newChildren = new PageReference[childCount - 1];
                DataUtils.copyExcept(children, newChildren, childCount, index);
                children = newChildren;
            }
        }

        @Override
        public void removeAllRecursive() {
            if (isPersistent()) {
                for (int i = 0, size = map.getChildPageCount(this); i < size; i++) {
                    PageReference ref = children[i];
                    if (ref.page != null) {
                        ref.page.removeAllRecursive();
                    } else {
                        long c = children[i].pos;
                        int type = DataUtils.getPageType(c);
                        if (type == PAGE_TYPE_LEAF) {
                            int mem = DataUtils.getPageMaxLength(c);
                            map.removePage(c, mem);
                        } else {
                            map.readPage(c).removeAllRecursive();
                        }
                    }
                }
            }
            removePage();
        }

        @Override
        public CursorPos getAppendCursorPos(CursorPos cursorPos) {
            int keyCount = getKeyCount();
            Page childPage = getChildPage(keyCount);
            return childPage.getAppendCursorPos(new CursorPos(this, keyCount, cursorPos));
        }

        @Override
        protected void readPayLoad(ByteBuffer buff) {
            int keyCount = getKeyCount();
            children = new PageReference[keyCount + 1];
            long[] p = new long[keyCount + 1];
            for (int i = 0; i <= keyCount; i++) {
                p[i] = buff.getLong();
            }
            long total = 0;
            for (int i = 0; i <= keyCount; i++) {
                long s = DataUtils.readVarLong(buff);
                long position = p[i];
                assert position == 0 ? s == 0 : s >= 0;
                total += s;
                children[i] = position == 0 ? PageReference.EMPTY : new PageReference(position, s);
            }
            totalCount = total;
        }

        @Override
        protected void writeValues(WriteBuffer buff) {}

        @Override
        protected void writeChildren(WriteBuffer buff, boolean withCounts) {
            int keyCount = getKeyCount();
            for (int i = 0; i <= keyCount; i++) {
                buff.putLong(children[i].pos);
            }
            if(withCounts) {
                for (int i = 0; i <= keyCount; i++) {
                    buff.putVarLong(children[i].count);
                }
            }
        }

        @Override
        void writeUnsavedRecursive(Chunk chunk, WriteBuffer buff) {
            if (!isSaved()) {
                int patch = write(chunk, buff);
                if (!isLeaf()) {
                    int len = getRawChildPageCount();
                    for (int i = 0; i < len; i++) {
                        Page p = children[i].page;
                        if (p != null) {
                            p.writeUnsavedRecursive(chunk, buff);
                            children[i] = new PageReference(p);
                        }
                    }
                    int old = buff.position();
                    buff.position(patch);
                    writeChildren(buff, false);
                    buff.position(old);
                }
            }
        }

        @Override
        void writeEnd() {
            int len = getRawChildPageCount();
            for (int i = 0; i < len; i++) {
                PageReference ref = children[i];
                if (ref.page != null) {
                    if (!ref.page.isSaved()) {
                        throw DataUtils.newIllegalStateException(
                                DataUtils.ERROR_INTERNAL, "Page not written");
                    }
                    ref.page.writeEnd();
                    children[i] = new PageReference(ref.pos, ref.count);
                }
            }
        }

        @Override
        public int getRawChildPageCount() {
//            assert children.length == getKeyCount() + 1;
            return getKeyCount() + 1;
        }

        @Override
        protected void recalculateMemory() {
            if(isPersistent()) {
                super.recalculateMemory();
                int mem = DataUtils.PAGE_NODE_MEMORY + Constants.MEMORY_ARRAY +
                        getRawChildPageCount() * (Constants.MEMORY_POINTER + DataUtils.PAGE_MEMORY_CHILD);
                addMemory(mem);
            }
        }

        @Override
        public void dump(StringBuilder buff) {
            super.dump(buff);
            int keyCount = getKeyCount();
            for (int i = 0; i <= keyCount; i++) {
                if (i > 0) {
                    buff.append(" ");
                }
                buff.append("[").append(Long.toHexString(children[i].pos)).append("]");
                if(i < keyCount) {
                    buff.append(" ").append(getKey(i));
                }
            }
        }
    }
}
