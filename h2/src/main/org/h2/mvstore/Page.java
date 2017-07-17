/*
 * Copyright 2004-2014 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.mvstore;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.HashSet;

import org.h2.compress.Compressor;
import org.h2.engine.Constants;
import org.h2.mvstore.type.ExtendedDataType;
import org.h2.util.New;

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
     * An empty object array.
     */
    public static final Object[] EMPTY_OBJECT_ARRAY = new Object[0];

    /**
     * Marker value for memory field, meaning that memory accounting is replaced by key count.
     */
    private static final int IN_MEMORY = Integer.MIN_VALUE;


    /**
     * Map this page belongs to
     */
    public final MVMap<?, ?> map;   //TODO make it private, look into map id change issue on compact/reopen

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
     * Number of keys on this page, may be smaller than storage capacity
     */
    private int keyCount;

    /**
     * The storage for keys.
     */
    private Object keys;

    private volatile boolean removed;

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
        this.map = map;
        assert source.getKeyCount() == source.map.getExtendedKeyType().getLength(source.keys);
        this.keyCount = source.keyCount;
        this.keys = source.keys;
    }

    /**
     * Create a new, empty page.
     *
     * @param map the map
     * @return the new page
     */
    public static Page createEmpty(MVMap<?, ?> map) {
        return create(map, null, null, null, 0, 0, DataUtils.PAGE_LEAF_EMPTY_MEMORY);
    }

    /**
     * Create a new page. The arrays are not cloned.
     *
     * @param map the map
     * @param keys the keys
     * @param values the values
     * @param children the child page positions
     * @param keyCount number of keys on the page
     * @param totalCount the total number of keys
     * @param memory the memory used in bytes
     * @return the page
     */
    public static Page create(MVMap<?, ?> map,
                              Object keys, Object values, PageReference[] children,
                              int keyCount, long totalCount, int memory) {
        Page p = children == null ? new Leaf(map, values) : new NonLeaf(map, children, totalCount);
        p.keyCount = keyCount;
        // the position is 0
        p.keys = keys != null ? keys :
                map.getKeyType() != map.getExtendedKeyType() ? EMPTY_OBJECT_ARRAY :
                                                               p.createKeyStorage(0);
        assert keyCount == 0 || map.getExtendedKeyType().getLength(p.keys) == keyCount : map.getExtendedKeyType().getLength(p.keys) + " == " + keyCount;
        MVStore store = map.store;
        if(store.getFileStore() == null) {
            p.memory = IN_MEMORY;
        } else if (memory == 0) {
            p.recalculateMemory();
        } else {
            p.addMemory(memory);
            assert memory == p.getMemory();
        }
        assert p.getKeyCount() == p.map.getExtendedKeyType().getLength(p.keys);
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
    public Object getKey(int index) {
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
    public int getKeyCount() {
        return keyCount;
    }

    /**
     * Get storage capacity (the number of keys) in this page.
     *
     * @return the number of keys this page is able to store
     */
    public int getKeyCapacity() {
        return map.getExtendedKeyType().getLength(keys);
    }

    /**
     * Check whether this is a leaf page.
     *
     * @return true if it is a leaf
     */
    public boolean isLeaf() {
        return getNodeType() == PAGE_TYPE_LEAF;
    }

    public abstract int getNodeType();

    /**
     * Get the position of the page
     *
     * @return the position
     */
    public long getPos() {
        return pos;
    }

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
        buff.append("id: ").append(System.identityHashCode(this)).append('\n');
        buff.append("pos: ").append(Long.toHexString(pos)).append("\n");
        if (pos != 0) {
            int chunkId = DataUtils.getPageChunkId(pos);
            buff.append("chunk: ").append(Long.toHexString(chunkId)).append("\n");
        }
        dump(buff);
        return buff.toString();
    }

    protected abstract void dump(StringBuilder buff);

    /**
     * Create a copy of this page.
     *
     * @return a mutable copy of this page
     */
    public Page copy() {
        return copy(false);
    }

    public Page copy(boolean countRemoval) {
        Page newPage = clone();
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
    protected Page clone() {
        Page clone;
        try {
            clone = (Page) super.clone();
        } catch (CloneNotSupportedException impossible) {
            throw new RuntimeException(impossible);
        }
        clone.pos = 0;
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
    public int binarySearch(Object key) {
        int result = map.getExtendedKeyType().binarySearch(key, keys, cachedCompare);
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
        keys = aKeys;
        keyCount = aCount;
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
    public void setKey(int index, Object key) {
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

    protected void insertKey(int index, Object key) {
        assert getKeyCount() == map.getExtendedKeyType().getLength(keys);
        int len = getKeyCount() + 1;
        Object newKeys = createKeyStorage(len);
        DataUtils.copyWithGap(keys, newKeys, len - 1, index);
        keys = newKeys;
        map.getExtendedKeyType().setValue(keys, index, key);
        keyCount++;

        if (isPersistent()) {
            addMemory(map.getKeyType().getMemory(key));
        }
    }

    /**
     * Insert a key-value pair into this leaf.
     * It should always operate on a shallow copy of the Page
     *
     * @param index the index
     * @param key the key
     * @param value the value
     * @return split-off Page if split has happened or null if there was no split
     */
/*
    public Page insertIntoLeaf(int index, Object key, Object value) {
        if(index < getKeyCount()) {
            if (getKeyCount() < map.getStore().getPageSplitSize()) {
                // growth with storage cloning
                int len = getKeyCount() + 1;
                Object newKeys = createKeyStorage(len);
                DataUtils.copyWithGap(keys, newKeys, len - 1, index);
                keys = newKeys;
                map.getExtendedKeyType().setValue(keys, index, key);

                if (!isLeaf()) {
                    int childCount = children.length;
                    PageReference[] newChildren = new PageReference[childCount + 1];
                    DataUtils.copyWithGap(children, newChildren, childCount, index);
                    Page childPage = (Page) value;
                    newChildren[index] = new PageReference(childPage);
                    children = newChildren;
                    totalCount += childPage.totalCount;
                } else {
                    if(values != null) {
                        Object newValues = createValueStorage(len);
                        DataUtils.copyWithGap(values, newValues, len - 1, index);
                        values = newValues;
                        map.getExtendedValueType().setValue(values, index, value);
                    }
                    totalCount++;
                }
                if (isPersistent()) {
                    addMemory(map.getKeyType().getMemory(key) +
                            (isLeaf() ? map.getValueType().getMemory(value) : DataUtils.PAGE_MEMORY_CHILD));
                }
                return null;
            } else {
                // regular split in half
                if (!isLeaf()) {
                    int at = (getKeyCount() + 1) >> 1;
                    int b = getKeyCount() - at;
                    Object aKeys = createKeyStorage(at);
                    Object bKeys = createKeyStorage(b - 1);
                    System.arraycopy(keys, 0, aKeys, 0, at);
                    System.arraycopy(keys, at + 1, bKeys, 0, b - 1);
                    keys = aKeys;
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
                    if (isPersistent()) {
                        recalculateMemory();
                    }
                    return create(map, bKeys, null, bChildren, b - 1, t, 0);
                } else {
                    int at = (getKeyCount() + 1) >> 1;
                    int b = getKeyCount() - at;
                    ExtendedDataType type = map.getExtendedKeyType();
                    Object aKeys = createKeyStorage(at);
                    Object bKeys = createKeyStorage(b);
                    if(index < at) {
                        DataUtils.copyWithGap(keys, aKeys, at, index);
                        type.setValue(aKeys, index, key);
                        System.arraycopy(keys, at, bKeys, 0, b);
                    } else {
                        System.arraycopy(keys, 0, aKeys, 0, at);
                        if(index > at) {
                            System.arraycopy(keys, at, bKeys, 0, index - at);
                        }
                        if(index - at < b - 1) {
                            System.arraycopy(keys, index, bKeys, index - at + 1, b - index + at - 1);
                        }
                        type.setValue(bKeys, index - at, key);
                    }
                    keys = aKeys;
                    Object bValues = null;
                    if(values != null) {
                        Object aValues = createValueStorage(at);
                        bValues = createValueStorage(b);
                        if(index < at) {
                            DataUtils.copyWithGap(values, aValues, at, index);
                            map.getExtendedValueType().setValue(aValues, index, key);
                            System.arraycopy(value, at, bValues, 0, b);
                        } else {
                            System.arraycopy(values, 0, aValues, 0, at);
                            if(index > at) {
                                System.arraycopy(values, at, bValues, 0, index - at);
                            }
                            if(index - at < b - 1) {
                                System.arraycopy(values, index, bValues, index - at + 1, b - index + at - 1);
                            }
                            map.getExtendedValueType().setValue(bValues, index - at, key);
                        }
                        values = aValues;
                        totalCount = at;
                    }
                    if (isPersistent()) {
                        recalculateMemory();
                    }
                    return create(map, bKeys, bValues, null, b, b, 0);
                }
            }
        } else if(index == getKeyCapacity()) {
            // bulk mode split
            assert getKeyCount() == getKeyCapacity();
            Object bKeys = createKeyStorage(getKeyCapacity());
            map.getExtendedKeyType().setValue(bKeys, 0, key);
            Object bValues = null;
            PageReference[] bChildren = null;
            long t;
            if (!isLeaf()) {
                Page childPage = (Page) value;
                bChildren = new PageReference[] { children[getKeyCount() + 1], new PageReference(childPage) };
                t = children[getKeyCount() + 1].count;
                totalCount -= t;
                t += childPage.totalCount;
            } else {
                if(values != null) {
                    bValues = createValueStorage(getKeyCapacity());
                    map.getExtendedValueType().setValue(bValues, 0, value);
                }
                t = 1;
            }
            return create(map, bKeys, bValues, bChildren, 1, t, 0);
        } else {
            // growth without storage cloning
            keyCount++;
            map.getExtendedKeyType().setValue(keys, index, key);
            if (!isLeaf()) {
                Page childPage = (Page) value;
                children[index] = new PageReference(childPage);
                totalCount += childPage.totalCount;
            } else {
                if(values != null) {
                    map.getExtendedValueType().setValue(values, index, value);
                }
                totalCount++;
            }
            if (isPersistent()) {
                addMemory(map.getKeyType().getMemory(key) +
                        (isLeaf() ? map.getValueType().getMemory(value) : DataUtils.PAGE_MEMORY_CHILD));
            }
            return null;
        }
    }

    private void splitCopyInsert(ExtendedDataType type, Object source,
                                 Object tagetOne, int sizeOne,
                                 Object targetTwo, int sizeTwo,
                                 int index, Object value) {
        if(index < sizeOne) {
            DataUtils.copyWithGap(source, tagetOne, sizeOne, index);
            type.setValue(tagetOne, index, value);
            System.arraycopy(source, sizeOne, targetTwo, 0, sizeTwo);
        } else {
            System.arraycopy(source, 0, tagetOne, 0, sizeOne);
            if(index > sizeOne) {
                System.arraycopy(source, sizeOne, targetTwo, 0, index - sizeOne);
            }
            if(index - sizeOne < sizeTwo - 1) {
                System.arraycopy(source, index, targetTwo, index - sizeOne + 1, sizeTwo - index + sizeOne - 1);
            }
            type.setValue(targetTwo, index - sizeOne, value);
        }
    }
*/

    /**
     * Remove the key and value (or child) at the given index.
     *
     * @param index the index
     */
    public void remove(int index) {
        int keyLength = getKeyCount();
        assert keyLength == map.getExtendedKeyType().getLength(keys);
        int keyIndex = index >= keyLength ? index - 1 : index;
        if(isPersistent()) {
            Object old = getKey(keyIndex);
            addMemory(-map.getKeyType().getMemory(old));
        }
        Object newKeys = createKeyStorage(keyLength - 1);
        DataUtils.copyExcept(keys, newKeys, keyLength, keyIndex);
        keys = newKeys;
        keyCount--;
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
        map.getExtendedKeyType().read(buff, keys);
        if (isLeaf()) {
            readPayLoad(buff);
        }
        recalculateMemory();
    }

    protected abstract void readPayLoad(ByteBuffer buff);

    public final boolean isSaved() {
        return pos != 0;
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
        map.getExtendedKeyType().writeStorage(buff, keys);
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
        if (pos != 0) {
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
        return other == this || other instanceof Page && (pos != 0 && ((Page) other).pos == pos || this == other);
    }

    @Override
    public final int hashCode() {
        return pos != 0 ? (int) (pos | (pos >>> 32)) : super.hashCode();
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
        return getKeyCount();
    }

    protected final void addMemory(int mem) {
        memory += mem;
    }

    protected void recalculateMemory() {
        if(isPersistent()) {
            memory = Constants.MEMORY_ARRAY + map.getExtendedKeyType().getMemorySize(keys);
        }
    }

    /**
     * Remove the page.
     */
    public final void removePage() {
        removed = true;
        if(isPersistent()) {
            long p = pos;
            if (p == 0) {
                removedInMemory = true;
            }
            map.removePage(p, memory);
        }
    }

    public boolean isRemoved() {
        return removed;
    }

    public abstract void removeAllRecursive();

    protected Object createKeyStorage(int size)
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
        public static final PageReference SINGLE_EMPTY[] = {EMPTY};

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

    /**
     * Contains information about which other pages are referenced (directly or
     * indirectly) by the given page. This is a subset of the page data, for
     * pages of type node. This information is used for garbage collection (to
     * quickly find out which chunks are still in use).
     */
    public static final class PageChildren {

        /**
         * An empty array of type long.
         */
        private static final long[] EMPTY_ARRAY = new long[0];

        /**
         * The position of the page.
         */
        final long pos;

        /**
         * The page positions of (direct or indirect) children. Depending on the
         * use case, this can be the complete list, or only a subset of all
         * children, for example only only one reference to a child in another
         * chunk.
         */
        long[] children;

        /**
         * Whether this object only contains the list of chunks.
         */
        boolean chunkList;

        private PageChildren(long pos, long[] children) {
            this.pos = pos;
            this.children = children;
        }

        PageChildren(Page p) {
            this.pos = p.getPos();
            int count = p.getRawChildPageCount();
            this.children = new long[count];
            for (int i = 0; i < count; i++) {
                children[i] = p.getChildPagePos(i);
            }
        }

        int getMemory() {
            return 64 + 8 * children.length;
        }

        /**
         * Read an inner node page from the buffer, but ignore the keys and
         * values.
         *
         * @param fileStore the file store
         * @param pos the position
         * @param mapId the map id
         * @param filePos the position in the file
         * @param maxPos the maximum position (the end of the chunk)
         * @return the page children object
         */
        static PageChildren read(FileStore fileStore, long pos, int mapId,
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
            long[] children = new long[len + 1];
            for (int i = 0; i <= len; i++) {
                children[i] = buff.getLong();
            }
            return new PageChildren(pos, children);
        }

        /**
         * Only keep one reference to the same chunk. Only leaf references are
         * removed (references to inner nodes are not removed, as they could
         * indirectly point to other chunks).
         */
        void removeDuplicateChunkReferences() {
            HashSet<Integer> chunks = New.hashSet();
            // we don't need references to leaves in the same chunk
            chunks.add(DataUtils.getPageChunkId(pos));
            for (int i = 0; i < children.length; i++) {
                long p = children[i];
                int chunkId = DataUtils.getPageChunkId(p);
                boolean wasNew = chunks.add(chunkId);
                if (DataUtils.getPageType(p) == DataUtils.PAGE_TYPE_NODE) {
                    continue;
                }
                if (wasNew) {
                    continue;
                }
                removeChild(i--);
            }
        }

        private void removeChild(int index) {
            if (index == 0 && children.length == 1) {
                children = EMPTY_ARRAY;
                return;
            }
            long[] c2 = new long[children.length - 1];
            DataUtils.copyExcept(children, c2, children.length, index);
            children = c2;
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

        private Leaf(MVMap<?, ?> map, Object values) {
            super(map);
            this.values = values != null ? values :
                    map.getValueType() != map.getExtendedValueType() ? EMPTY_OBJECT_ARRAY :
                                                                   createValueStorage(0);
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
            int b = getKeyCount() - at;
            Object bKeys = splitKeys(at, b);
            Object bValues = createValueStorage(b);
            if(values != null) {
                Object aValues = createValueStorage(at);
                System.arraycopy(values, 0, aValues, 0, at);
                System.arraycopy(values, at, bValues, 0, b);
                values = aValues;
            }
            Page newPage = create(map, bKeys, bValues, null, b, b, 0);
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
        public void insertLeaf(int index, Object key, Object value) {
            int keyCount = getKeyCount();
            insertKey(index, key);

            if(values != null) {
                Object newValues = createValueStorage(keyCount + 1);
                DataUtils.copyWithGap(values, newValues, keyCount, index);
                values = newValues;
                setValueInternal(index, value);
            }
            if (isPersistent()) {
                addMemory(map.getValueType().getMemory(value));
            }
        }

        @Override
        public void insertNode(int index, Object key, Page childPage) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void remove(int index) {
            int keyLength = getKeyCount();
            super.remove(index);
            if (values != null) {
                if(isPersistent()) {
                    Object old = getValue(index);
                    addMemory(-map.getValueType().getMemory(old));
                }
                Object newValues = createValueStorage(keyLength - 1);
                DataUtils.copyExcept(values, newValues, keyLength, index);
                values = newValues;
            }
        }

        @Override
        public void removeAllRecursive() {
            removePage();
        }

        @Override
        protected void readPayLoad(ByteBuffer buff) {
            int keyCount = getKeyCount();
            values = createValueStorage(keyCount);
            map.getExtendedValueType().read(buff, values);
        }

        @Override
        protected void writeValues(WriteBuffer buff) {
            map.getExtendedValueType().writeStorage(buff, values);
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
                            map.getExtendedValueType().getMemorySize(values);
                addMemory(mem);
            }
        }

        @Override
        public void dump(StringBuilder buff) {
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
            this.children = source.children;
            this.totalCount = source.totalCount;
        }

        private NonLeaf(MVMap<?, ?> map, PageReference[] children, long totalCount) {
            super(map);
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
            Page newPage = create(map, bKeys, null, bChildren, b - 1, t, 0);
            if(isPersistent()) {
                recalculateMemory();
            }
            return newPage;
        }

        @Override
        public long getTotalCount() {
            if (MVStore.ASSERT) {
                long check = 0;
                for (int i = 0; i <= getKeyCount(); i++) {
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
            PageReference[] newChildren = new PageReference[childCount + 1];
            DataUtils.copyWithGap(children, newChildren, childCount, index);
            newChildren[index] = new PageReference(childPage);
            children = newChildren;

            totalCount += childPage.getTotalCount();
            if (isPersistent()) {
                addMemory(Constants.MEMORY_POINTER + DataUtils.PAGE_MEMORY_CHILD);
            }
        }

        @Override
        public void remove(int index) {
            super.remove(index);
            if(isPersistent()) {
                addMemory(-(Constants.MEMORY_POINTER + DataUtils.PAGE_MEMORY_CHILD));
            }
            long countOffset = children[index].count;

            int childCount = children.length;
            PageReference[] newChildren = new PageReference[childCount - 1];
            DataUtils.copyExcept(children, newChildren, childCount, index);
            children = newChildren;

            totalCount -= countOffset;
        }

        @Override
        public void removeAllRecursive() {
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
            removePage();
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
            for (int i = 0; i <= getKeyCount(); i++) {
                buff.putLong(children[i].pos);
            }
            if(withCounts) {
                for (int i = 0; i <= getKeyCount(); i++) {
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
                    if (ref.page.getPos() == 0) {
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
            assert children.length == getKeyCount() + 1;
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
