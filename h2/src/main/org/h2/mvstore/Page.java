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
import org.h2.mvstore.type.ExtendedDataType;
import org.h2.util.New;

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
public final class Page implements Cloneable {

    /**
     * An empty object array.
     */
    public static final Object[] EMPTY_OBJECT_ARRAY = new Object[0];
    private static final int IN_MEMORY = Integer.MIN_VALUE;

    public final MVMap<?, ?> map;
    private long pos;

    /**
     * The total entry count of this page and all children.
     */
    private long totalCount;

    /**
     * The last result of a find operation is cached.
     */
    private int cachedCompare;

    /**
     * The estimated memory used in persistent case, IN_MEMORY marker value otherwise.
     */
    private int memory;

    /**
     * The keys.
     */
    private Object keys;

    /**
     * The values.
     */
    private Object values;

    /**
     * The child page references.
     */
    private PageReference[] children;

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

    /**
     * Create a new, empty page.
     *
     * @param map the map
     * @return the new page
     */
    public static Page createEmpty(MVMap<?, ?> map) {
        return create(map,
                null, null,
                null,
                0, DataUtils.PAGE_MEMORY);
    }

    /**
     * Create a new page. The arrays are not cloned.
     *
     * @param map the map
     * @param keys the keys
     * @param values the values
     * @param children the child page positions
     * @param totalCount the total number of keys
     * @param memory the memory used in bytes
     * @return the page
     */
    public static Page create(MVMap<?, ?> map,
            Object keys, Object values, PageReference[] children,
            long totalCount, int memory) {
        Page p = new Page(map);
        // the position is 0
        p.keys = keys != null ? keys :
                map.getKeyType() != map.getExtendedKeyType() ? EMPTY_OBJECT_ARRAY :
                                                               p.createKeyStorage(0);
        p.values = values != null ? values :
                   children != null ? null :
                map.getValueType() != map.getExtendedValueType() ? EMPTY_OBJECT_ARRAY :
                                                               p.createValueStorage(0);
        p.children = children;
        p.totalCount = totalCount;
        MVStore store = map.store;
        if(store.getFileStore() == null) {
            p.memory = IN_MEMORY;
        } else if (memory == 0) {
            p.recalculateMemory();
        } else {
            p.addMemory(memory);
        }
        return p;
    }

    /**
     * Create a copy of a page.
     *
     * @param map the map
     * @param source the source page
     * @return the page
     */
    public static Page create(MVMap<?, ?> map, Page source) {
        PageReference[] children = source.children;
        long totalCount = source.totalCount;
        if(children != null) {
            // replace child pages with empty pages
            children = new PageReference[source.children.length];
            Arrays.fill(children, PageReference.EMPTY);
            totalCount = 0;
        }
        Page page = create(map, source.keys, source.values, children,
                           totalCount, source.memory);
        map.store.registerUnsavedPage(page.getMemory());
        return page;
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
        Page p = new Page(map);
        p.pos = pos;
        int chunkId = DataUtils.getPageChunkId(pos);
        int offset = DataUtils.getPageOffset(pos);
        p.read(buff, chunkId, offset, maxLength);
        return p;
    }

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
    public Page getChildPage(int index) {
        PageReference ref = children[index];
        Page page = ref.page;
        if(page == null) {
            page = map.readPage(ref.pos);
            assert ref.pos == page.pos;
            assert ref.count == page.totalCount;
        }
        return page;
    }

    /**
     * Get the position of the child.
     *
     * @param index the index
     * @return the position
     */
    public long getChildPagePos(int index) {
        return children[index].pos;
    }

    /**
     * Get the value at the given index.
     *
     * @param index the index
     * @return the value
     */
    public Object getValue(int index) {
        return map.getExtendedValueType().getValue(values, index);
    }

    /**
     * Get the number of keys in this page.
     *
     * @return the number of keys
     */
    public int getKeyCount() {
        return map.getExtendedKeyType().getLength(keys);
    }

    /**
     * Check whether this is a leaf page.
     *
     * @return true if it is a leaf
     */
    public boolean isLeaf() {
        return children == null;
    }

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
        int keyCount = getKeyCount();
        for (int i = 0; i <= keyCount; i++) {
            if (i > 0) {
                buff.append(" ");
            }
            if (children != null) {
                buff.append("[").append(Long.toHexString(children[i].pos)).append("] ");
            }
            if (i < keyCount) {
                buff.append(getKey(i));
                if (values != null) {
                    buff.append(':');
                    buff.append(getValue(i));
                }
            }
        }
        return buff.toString();
    }

    /**
     * Create a copy of this page.
     *
     * @return a mutable copy of this page
     */
    public Page copy() {
        return copy(false);
    }

    public Page copy(boolean countRemoval) {
//        Page newPage = create(map, keys, values, children, totalCount, memory);
        Page newPage = clone();
        // mark the old as deleted
        if(countRemoval) {
            removePage();
            if(isPersistent()) {
                map.store.registerUnsavedPage(newPage.getMemory());
            }
        }
//        newPage.cachedCompare = cachedCompare;
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
/*
        int keyCount = getKeyCount();
        if(keyCount == 0) return -1;
        Class<?> keyClass = key == null ? null : key.getClass();
        Object key0 = getKey(0);
        Class<?> arrayClass = key0.getClass();
        if(keyClass == ValueLong.class && arrayClass == ValueLong.class) return binarySearch(((ValueLong)key).getLong(), (Value[])keys);
        if(keyClass == Long.class && arrayClass == Long.class) return binarySearch2((Long)key, (long[])keys);
//        if(keyClass == ValueInt.class && arrayClass == ValueInt.class) return binarySearch(((ValueInt)key).getInt(), (Object[])keys);
//        if(keyClass == Integer.class && arrayClass == Integer.class) return binarySearch2((Integer)key, (int[])keys);
//        if(key instanceof ValueString && key0 instanceof ValueString) return binarySearch(((ValueString)key).getString(), (Object[])keys);
//        if(key instanceof String && key0 instanceof String) return binarySearch2((String)key, (Object[])keys);
*/

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
    Page split(int at) {
        Page page = isLeaf() ? splitLeaf(at) : splitNode(at);
        if(isPersistent()) {
            recalculateMemory();
        }
        return page;
    }

    @SuppressWarnings("SuspiciousSystemArraycopy")
    private Page splitLeaf(int at) {
        int b = getKeyCount() - at;
        Object aKeys = createKeyStorage(at);
        Object bKeys = createKeyStorage(b);
        System.arraycopy(keys, 0, aKeys, 0, at);
        System.arraycopy(keys, at, bKeys, 0, b);
        keys = aKeys;
        Object bValues = createValueStorage(b);
        if(values != null) {
            Object aValues = createValueStorage(at);
            System.arraycopy(values, 0, aValues, 0, at);
            System.arraycopy(values, at, bValues, 0, b);
            values = aValues;
        }
        totalCount = at;
        Page newPage = create(map,
                bKeys, bValues,
                null,
                b, 0);
        return newPage;
    }

    @SuppressWarnings("SuspiciousSystemArraycopy")
    private Page splitNode(int at) {
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
        Page newPage = create(map,
                bKeys, null,
                bChildren,
                t, 0);
        return newPage;
    }

    /**
     * Get the total number of key-value pairs, including child pages.
     *
     * @return the number of key-value pairs
     */
    public long getTotalCount() {
        if (MVStore.ASSERT) {
            long check = 0;
            if (isLeaf()) {
                check = getKeyCount();
            } else {
                for (PageReference x : children) {
                    check += x.count;
                }
            }
            if (check != totalCount) {
                throw DataUtils.newIllegalStateException(
                        DataUtils.ERROR_INTERNAL,
                        "Expected: {0} got: {1}", check, totalCount);
            }
        }
        return totalCount;
    }

    /**
     * Get the descendant counts for the given child.
     *
     * @param index the child index
     * @return the descendant count
     */
    long getCounts(int index) {
        return children[index].count;
    }

    /**
     * Replace the child page.
     *
     * @param index the index
     * @param c the new child page
     */
    public void setChild(int index, Page c) {
        long position;
        long totalCnt;
        if (c == null) {
            position = 0;
            totalCnt = 0;
        } else {
            position = c.pos;
            totalCnt = c.totalCount;
        }

        PageReference child = children[index];
        if (c != child.page || position != child.pos) {
            totalCount += totalCnt - child.count;
            // this is slightly slower:
            // children = Arrays.copyOf(children, children.length);
            children = children.clone();
            children[index] = new PageReference(c, position, totalCnt);
        }
    }

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
    public Object setValue(int index, Object value) {
        ExtendedDataType valueType = map.getExtendedValueType();
        Object old = valueType.getValue(values, index);
        values = valueType.clone(values);
        if(isPersistent()) {
            addMemory(valueType.getMemory(value) -
                      valueType.getMemory(old));
        }
        valueType.setValue(values, index, value);
        return old;
    }

    /**
     * Remove this page and all child pages.
     */
    void removeAllRecursive() {
        if (children != null) {
            for (int i = 0, size = map.getChildPageCount(this); i < size; i++) {
                PageReference ref = children[i];
                if (ref.page != null) {
                    ref.page.removeAllRecursive();
                } else {
                    long c = children[i].pos;
                    int type = DataUtils.getPageType(c);
                    if (type == DataUtils.PAGE_TYPE_LEAF) {
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

    /**
     * Insert a key-value pair into this leaf.
     *
     * @param index the index
     * @param key the key
     * @param value the value
     */
    public void insertLeaf(int index, Object key, Object value) {
        int len = getKeyCount() + 1;
        Object newKeys = createKeyStorage(len);
        DataUtils.copyWithGap(keys, newKeys, len - 1, index);
        keys = newKeys;
        map.getExtendedKeyType().setValue(keys, index, key);

        if(values != null) {
            Object newValues = createValueStorage(len);
            DataUtils.copyWithGap(values, newValues, len - 1, index);
            values = newValues;
            setValue(index, value);
        }
        totalCount++;
        addMemory(map.getKeyType().getMemory(key) +
                map.getValueType().getMemory(value));
    }

    /**
     * Insert a child page into this node.
     *
     * @param index the index
     * @param key the key
     * @param childPage the child page
     */
    public void insertNode(int index, Object key, Page childPage) {

        int keyCount = getKeyCount();
        Object newKeys = createKeyStorage(keyCount + 1);
        DataUtils.copyWithGap(keys, newKeys, keyCount, index);
        keys = newKeys;
        map.getExtendedKeyType().setValue(keys, index, key);

        int childCount = children.length;
        PageReference[] newChildren = new PageReference[childCount + 1];
        DataUtils.copyWithGap(children, newChildren, childCount, index);
        newChildren[index] = new PageReference(childPage);
        children = newChildren;

        totalCount += childPage.totalCount;
        addMemory(map.getKeyType().getMemory(key) +
                DataUtils.PAGE_MEMORY_CHILD);
    }

    /**
     * Remove the key and value (or child) at the given index.
     *
     * @param index the index
     */
    public void remove(int index) {
        int keyLength = getKeyCount();
        int keyIndex = index >= keyLength ? index - 1 : index;
        if(isPersistent()) {
            Object old = getKey(keyIndex);
            addMemory(-map.getKeyType().getMemory(old));
        }
        Object newKeys = createKeyStorage(keyLength - 1);
        DataUtils.copyExcept(keys, newKeys, keyLength, keyIndex);
        keys = newKeys;

        if (isLeaf()) {
            if (values != null) {
                if(isPersistent()) {
                    Object old = getValue(index);
                    addMemory(-map.getValueType().getMemory(old));
                }
                Object newValues = createValueStorage(keyLength - 1);
                DataUtils.copyExcept(values, newValues, keyLength, index);
                values = newValues;
            }
            totalCount--;
        } else {
            if(isPersistent()) {
                addMemory(-DataUtils.PAGE_MEMORY_CHILD);
            }
            long countOffset = children[index].count;

            int childCount = children.length;
            PageReference[] newChildren = new PageReference[childCount - 1];
            DataUtils.copyExcept(children, newChildren, childCount, index);
            children = newChildren;

            totalCount -= countOffset;
        }
    }

    /**
     * Read the page from the buffer.
     *
     * @param buff the buffer
     * @param chunkId the chunk id
     * @param offset the offset within the chunk
     * @param maxLength the maximum length
     */
    void read(ByteBuffer buff, int chunkId, int offset, int maxLength) {
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
        int len = DataUtils.readVarInt(buff);
        keys = createKeyStorage(len);
        int type = buff.get();
        boolean node = (type & 1) == DataUtils.PAGE_TYPE_NODE;
        if (node) {
            children = new PageReference[len + 1];
            long[] p = new long[len + 1];
            for (int i = 0; i <= len; i++) {
                p[i] = buff.getLong();
            }
            long total = 0;
            for (int i = 0; i <= len; i++) {
                long s = DataUtils.readVarLong(buff);
                long position = p[i];
                assert position == 0 ? s == 0 : s >= 0;
                total += s;
                children[i] = position == 0 ? PageReference.EMPTY : new PageReference(position, s);
            }
            totalCount = total;
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
        if (!node) {
            values = createValueStorage(len);
            map.getExtendedValueType().read(buff, values);
            totalCount = len;
        }
        recalculateMemory();
    }

    /**
     * Store the page and update the position.
     *
     * @param chunk the chunk
     * @param buff the target buffer
     * @return the position of the buffer just after the type
     */
    private int write(Chunk chunk, WriteBuffer buff) {
        int start = buff.position();
        int len = getKeyCount();
        int type = children != null ? DataUtils.PAGE_TYPE_NODE
                : DataUtils.PAGE_TYPE_LEAF;
        buff.putInt(0).
            putShort((byte) 0).
            putVarInt(map.getId()).
            putVarInt(len);
        int typePos = buff.position();
        buff.put((byte) type);
        if (type == DataUtils.PAGE_TYPE_NODE) {
            writeChildren(buff);
            for (int i = 0; i <= len; i++) {
                buff.putVarLong(children[i].count);
            }
        }
        int compressStart = buff.position();
        map.getExtendedKeyType().writeStorage(buff, keys);
        if (type == DataUtils.PAGE_TYPE_LEAF) {
            map.getExtendedValueType().writeStorage(buff, values);
        }
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
        store.cachePage(pos, this, getMemory());
        if (type == DataUtils.PAGE_TYPE_NODE) {
            // cache again - this will make sure nodes stays in the cache
            // for a longer time
            store.cachePage(pos, this, getMemory());
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

    private void writeChildren(WriteBuffer buff) {
        for (PageReference aChildren : children) {
            buff.putLong(aChildren.pos);
        }
    }

    /**
     * Store this page and all children that are changed, in reverse order, and
     * update the position and the children.
     *
     * @param chunk the chunk
     * @param buff the target buffer
     */
    void writeUnsavedRecursive(Chunk chunk, WriteBuffer buff) {
        if (pos != 0) {
            // already stored before
            return;
        }
        int patch = write(chunk, buff);
        if (!isLeaf()) {
            int len = children.length;
            for (int i = 0; i < len; i++) {
                Page p = children[i].page;
                if (p != null) {
                    p.writeUnsavedRecursive(chunk, buff);
                    children[i] = new PageReference(p);
                }
            }
            int old = buff.position();
            buff.position(patch);
            writeChildren(buff);
            buff.position(old);
        }
    }

    /**
     * Unlink the children recursively after all data is written.
     */
    void writeEnd() {
        if (isLeaf()) {
            return;
        }
        int len = children.length;
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

    public int getRawChildPageCount() {
        return children.length;
    }

    @Override
    public boolean equals(Object other) {
        return other == this || other instanceof Page && (pos != 0 && ((Page) other).pos == pos || this == other);
    }

    @Override
    public int hashCode() {
        return pos != 0 ? (int) (pos | (pos >>> 32)) : super.hashCode();
    }

    private boolean isPersistent() {
        return memory != IN_MEMORY;
    }

    public int getMemory() {
        if (isPersistent()) {
            if (MVStore.ASSERT) {
                int mem = memory;
                recalculateMemory();
                if (mem != memory) {
                    throw DataUtils.newIllegalStateException(
                            DataUtils.ERROR_INTERNAL, "Memory calculation error");
                }
            }
            return memory;
        }
        return getKeyCount();
    }

    private void addMemory(int mem) {
        memory += mem;
    }

    private void recalculateMemory() {
        if(isPersistent()) {
            int mem = DataUtils.PAGE_MEMORY + map.getExtendedKeyType().getMemorySize(keys);
            if (this.isLeaf()) {
                mem += map.getExtendedValueType().getMemorySize(values);
            } else {
                mem += this.getRawChildPageCount() * DataUtils.PAGE_MEMORY_CHILD;
            }
            addMemory(mem - memory);
        }
    }

    /**
     * Remove the page.
     */
    public void removePage() {
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

    private Object createKeyStorage(int size)
    {
        return map.getExtendedKeyType().createStorage(size);
    }

    private Object createValueStorage(int size)
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
            this(page, page.pos, page.totalCount);
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
}
