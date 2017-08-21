/*
 * Copyright 2004-2014 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.mvstore;

import java.lang.Thread.UncaughtExceptionHandler;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import org.h2.compress.CompressDeflate;
import org.h2.compress.CompressLZF;
import org.h2.compress.Compressor;
import org.h2.mvstore.Page.PageChildren;
import org.h2.mvstore.cache.CacheLongKeyLIRS;
import org.h2.mvstore.type.DataType;
import org.h2.mvstore.type.StringDataType;
import org.h2.util.MathUtils;
import org.h2.util.New;
import org.h2.util.Utils;

import static org.h2.mvstore.MVMap.INITIAL_VERSION;

/*

TODO:

Documentation
- rolling docs review: at "Metadata Map"
- better document that writes are in background thread
- better document how to do non-unique indexes
- document pluggable store and OffHeapStore

TransactionStore:
- ability to disable the transaction log,
    if there is only one connection

MVStore:
- better and clearer memory usage accounting rules
    (heap memory versus disk memory), so that even there is
    never an out of memory
    even for a small heap, and so that chunks
    are still relatively big on average
- make sure serialization / deserialization errors don't corrupt the file
- test and possibly improve compact operation (for large dbs)
- automated 'kill process' and 'power failure' test
- defragment (re-creating maps, specially those with small pages)
- store number of write operations per page (maybe defragment
    if much different than count)
- r-tree: nearest neighbor search
- use a small object value cache (StringCache), test on Android
    for default serialization
- MVStoreTool.dump should dump the data if possible;
    possibly using a callback for serialization
- implement a sharded map (in one store, multiple stores)
    to support concurrent updates and writes, and very large maps
- to save space when persisting very small transactions,
    use a transaction log where only the deltas are stored
- serialization for lists, sets, sets, sorted sets, maps, sorted maps
- maybe rename 'rollback' to 'revert' to distinguish from transactions
- support other compression algorithms (deflate, LZ4,...)
- remove features that are not really needed; simplify the code
    possibly using a separate layer or tools
    (retainVersion?)
- optional pluggable checksum mechanism (per page), which
    requires that everything is a page (including headers)
- rename "store" to "save", as "store" is used in "storeVersion"
- rename setStoreVersion to setDataVersion, setSchemaVersion or similar
- temporary file storage
- simple rollback method (rollback to last committed version)
- MVMap to implement SortedMap, then NavigableMap
- storage that splits database into multiple files,
    to speed up compact and allow using trim
    (by truncating / deleting empty files)
- add new feature to the file system API to avoid copying data
    (reads that returns a ByteBuffer instead of writing into one)
    for memory mapped files and off-heap storage
- support log structured merge style operations (blind writes)
    using one map per level plus bloom filter
- have a strict call order MVStore -> MVMap -> Page -> FileStore
- autocommit commits, stores, and compacts from time to time;
    the background thread should wait at least 90% of the
    configured write delay to store changes
- compact* should also store uncommitted changes (if there are any)
- write a LSM-tree (log structured merge tree) utility on top of the MVStore
    with blind writes and/or a bloom filter that
    internally uses regular maps and merge sort
- chunk metadata: maybe split into static and variable,
    or use a small page size for metadata
- data type "string": maybe use prefix compression for keys
- test chunk id rollover
- feature to auto-compact from time to time and on close
- compact very small chunks
- Page: to save memory, combine keys & values into one array
    (also children & counts). Maybe remove some other
    fields (childrenCount for example)
- Support SortedMap for MVMap
- compact: copy whole pages (without having to open all maps)
- maybe change the length code to have lower gaps
- test with very low limits (such as: short chunks, small pages)
- maybe allow to read beyond the retention time:
    when compacting, move live pages in old chunks
    to a map (possibly the metadata map) -
    this requires a change in the compaction code, plus
    a map lookup when reading old data; also, this
    old data map needs to be cleaned up somehow;
    maybe using an additional timeout
- rollback of removeMap should restore the data -
    which has big consequences, as the metadata map
    would probably need references to the root nodes of all maps

*/

/**
 * A persistent storage for maps.
 */
public final class MVStore {

    /**
     * Whether assertions are enabled.
     */
    public static final boolean ASSERT = false;

    /**
     * The block size (physical sector size) of the disk. The store header is
     * written twice, one copy in each block, to ensure it survives a crash.
     */
    static final int BLOCK_SIZE = 4 * 1024;

    private static final int FORMAT_WRITE = 1;
    private static final int FORMAT_READ = 1;

    /**
     * Used to mark a chunk as free, when it was detected that live bookkeeping
     * is incorrect.
     */
    private static final int MARKED_FREE = 10000000;
    private static final long NOT_SET = Long.MAX_VALUE;

    /**
     * The background thread, if any.
     */
    volatile BackgroundWriterThread backgroundWriterThread;

    private volatile boolean reuseSpace = true;

    private boolean closed;

    private final FileStore fileStore;
    private final boolean fileStoreIsProvided;

    private final int pageSplitSize;

    private final int keysPerPage;

    /**
     * The page cache. The default size is 16 MB, and the average size is 2 KB.
     * It is split in 16 segments. The stack move distance is 2% of the expected
     * number of entries.
     */
    private CacheLongKeyLIRS<Page> cache;

    /**
     * The page chunk references cache. The default size is 4 MB, and the
     * average size is 2 KB. It is split in 16 segments. The stack move distance
     * is 2% of the expected number of entries.
     */
    private CacheLongKeyLIRS<PageChildren> cacheChunkRef;

    /**
     * The newest chunk. If nothing was stored yet, this field is not set.
     */
    private Chunk lastChunk;

    /**
     * The map of chunks.
     */
    private final ConcurrentHashMap<Integer, Chunk> chunks =
            new ConcurrentHashMap<Integer, Chunk>();

    private long updateCounter = 0;
    private long updateAttemptCounter = 0;

    /**
     * The map of temporarily freed storage space caused by freed pages. The key
     * is the unsaved version, the value is the map of chunks. The maps contains
     * the number of freed entries per chunk. Access is synchronized.
     */
    private final ConcurrentHashMap<Long,
            HashMap<Integer, Chunk>> freedPageSpace =
            new ConcurrentHashMap<Long, HashMap<Integer, Chunk>>();

    /**
     * The metadata map. Write access to this map needs to be synchronized on
     * the store.
     */
    private final MVMap<String, String> meta;

    private final ConcurrentHashMap<Integer, MVMap<?, ?>> maps =
            new ConcurrentHashMap<Integer, MVMap<?, ?>>();

    private final HashMap<String, Object> storeHeader = New.hashMap();

    private WriteBuffer writeBuffer;

    private int lastMapId;

    private int versionsToKeep = 5;

    /**
     * The compression level for new pages (0 for disabled, 1 for fast, 2 for
     * high). Even if disabled, the store may contain (old) compressed pages.
     */
    private final int compressionLevel;

    private Compressor compressorFast;

    private Compressor compressorHigh;

    public final UncaughtExceptionHandler backgroundExceptionHandler;

    private long currentVersion; // = INITIAL_VERSION + 1;

    private VersionChangeListener versionChangeListener;

    /**
     * The version of the last stored chunk, or -1 if nothing was stored so far.
     */
    private long lastStoredVersion = INITIAL_VERSION;

    private final AtomicLong oldestVersionToKeep = new AtomicLong(NOT_SET);

    /**
     * The estimated memory used by unsaved pages. This number is not accurate,
     * also because it may be changed concurrently, and because temporary pages
     * are counted.
     */
    private int unsavedMemory;
    private int autoCommitMemory;
    private volatile boolean saveNeeded;
    private final AtomicBoolean commitIsInProgress = new AtomicBoolean();

    /**
     * The time the store was created, in milliseconds since 1970.
     */
    private long creationTime;

    /**
     * How long to retain old, persisted chunks, in milliseconds. For larger or
     * equal to zero, a chunk is never directly overwritten if unused, but
     * instead, the unused field is set. If smaller zero, chunks are directly
     * overwritten if unused.
     */
    private int retentionTime;

    private long lastCommitTime;

    /**
     * The earliest chunk to retain, if any.
     */
    private Chunk retainChunk;

    /**
     * The version of the current store operation (if any).
     */
    private volatile long currentStoreVersion = -1;

    private Thread currentStoreThread;

    private volatile boolean metaChanged;

    /**
     * The delay in milliseconds to automatically commit and write changes.
     */
    private int autoCommitDelay;

    private int autoCompactFillRate;
    private long autoCompactLastFileOpCount;

    private Object compactSync = new Object();

    private IllegalStateException panicException;

    private long lastTimeAbsolute;

    private long lastFreeUnusedChunks;

    /**
     * Create and open the store.
     *
     * @param config the configuration to use
     * @throws IllegalStateException if the file is corrupt, or an exception
     *             occurred while opening
     * @throws IllegalArgumentException if the directory does not exist
     */
    MVStore(HashMap<String, Object> config) {
        Object o = config.get("compress");
        this.compressionLevel = o == null ? 0 : (Integer) o;
        String fileName = (String) config.get("fileName");
        FileStore fileStore = (FileStore) config.get("fileStore");
        fileStoreIsProvided = fileStore != null;
        if(fileStore == null && fileName != null) {
            fileStore = new FileStore();
        }
        this.fileStore = fileStore;
        o = config.get("pageSplitSize");
        pageSplitSize = o != null         ? (Integer) o :
                        fileStore != null ? 16 * 1024 :
                                            4 * 1024;
        o = config.get("keysPerPage");
        keysPerPage = o != null ? (Integer)o : 48;
        o = config.get("backgroundExceptionHandler");
        this.backgroundExceptionHandler = (UncaughtExceptionHandler) o;
        meta = new MVMap<String, String>(this);
        meta.init();

        if (this.fileStore == null) {
            cache = null;
            cacheChunkRef = null;
        } else {
            retentionTime = this.fileStore.getDefaultRetentionTime();
            boolean readOnly = config.containsKey("readOnly");
            o = config.get("cacheSize");
            int mb = o == null ? 16 : (Integer) o;
            if (mb > 0) {
                CacheLongKeyLIRS.Config cc = new CacheLongKeyLIRS.Config();
                cc.maxMemory = mb * 1024L * 1024L;
                o = config.get("cacheConcurrency");
                if (o != null) {
                    cc.segmentCount = (Integer) o;
                }
                cache = new CacheLongKeyLIRS<Page>(cc);
                cc.maxMemory /= 4;
                cacheChunkRef = new CacheLongKeyLIRS<PageChildren>(cc);
            }
            o = config.get("autoCommitBufferSize");
            int kb = o == null ? 1024 : (Integer) o;
            // 19 KB memory is about 1 KB storage
            autoCommitMemory = kb * 1024 * 19;

            o = config.get("autoCompactFillRate");
            autoCompactFillRate = o == null ? 50 : (Integer) o;

            char[] encryptionKey = (char[]) config.get("encryptionKey");
            try {
                if (!fileStoreIsProvided) {
                    this.fileStore.open(fileName, readOnly, encryptionKey);
                }
                if (this.fileStore.size() == 0) {
                    creationTime = getTimeAbsolute();
                    lastCommitTime = creationTime;
                    storeHeader.put("H", 2);
                    storeHeader.put("blockSize", BLOCK_SIZE);
                    storeHeader.put("format", FORMAT_WRITE);
                    storeHeader.put("created", creationTime);
                    writeStoreHeader();
                } else {
                    readStoreHeader();
                }
            } catch (IllegalStateException e) {
                panic(e);
            } finally {
                if (encryptionKey != null) {
                    Arrays.fill(encryptionKey, (char) 0);
                }
            }
            lastCommitTime = getTimeSinceCreation();

            // setAutoCommitDelay starts the thread, but only if
            // the parameter is different from the old value
            o = config.get("autoCommitDelay");
            int delay = o == null ? 1000 : (Integer) o;
            setAutoCommitDelay(delay);
        }
    }

    private void panic(IllegalStateException e) {
        if (backgroundExceptionHandler != null) {
            backgroundExceptionHandler.uncaughtException(null, e);
        }
        panicException = e;
        closeImmediately();
        throw e;
    }

    public IllegalStateException getPanicException() {
        return panicException;
    }

    /**
     * Open a store in exclusive mode. For a file-based store, the parent
     * directory must already exist.
     *
     * @param fileName the file name (null for in-memory)
     * @return the store
     */
    public static MVStore open(String fileName) {
        HashMap<String, Object> config = New.hashMap();
        config.put("fileName", fileName);
        return new MVStore(config);
    }

    /**
     * Find position of the root page for historical version of the map.
     *
     * @param mapId to find the old version for
     * @param version the version
     * @return position of the root Page
     */
    long getRootPos(int mapId, long version) {
        MVMap<String, String> oldMeta = getMetaMap(version);
        return getRootPos(oldMeta, mapId);
    }

    /**
     * Open a map with the default settings. The map is automatically create if
     * it does not yet exist. If a map with this name is already open, this map
     * is returned.
     *
     * @param <K> the key type
     * @param <V> the value type
     * @param name the name of the map
     * @return the map
     */
    public <K, V> MVMap<K, V> openMap(String name) {
        return openMap(name, new MVMap.Builder<K, V>());
    }

    /**
     * Open a map with the given builder. The map is automatically create if it
     * does not yet exist. If a map with this name is already open, this map is
     * returned.
     *
     * @param <K> the key type
     * @param <V> the value type
     * @param name the name of the map
     * @param builder the map builder
     * @return the map
     */
    public synchronized <M extends MVMap<K, V>, K, V> M openMap(
            String name, MVMap.MapBuilder<M, K, V> builder) {
        checkOpen();
        String x = meta.get("name." + name);
        int id;
        HashMap<String, Object> c;
        M map;
        if (x != null) {
            id = DataUtils.parseHexInt(x);
            map = openMap(id, builder);
        } else {
            c = New.hashMap();
            id = ++lastMapId;
            c.put("id", id);
            c.put("createVersion", currentVersion);
            map = builder.create(this, c);
            map.init();
            markMetaChanged();
            x = Integer.toHexString(id);
            meta.put(MVMap.getMapKey(id), map.asString(name));
            meta.put("name." + name, x);
            map.setRootPos(0, lastStoredVersion);
            @SuppressWarnings("unchecked")
            M existingMap = (M)maps.putIfAbsent(id, map);
            if(existingMap != null) {
                map = existingMap;
            }
        }
        return map;
    }

    public synchronized <M extends MVMap<K, V>, K, V> M openMap(int id,
                                       MVMap.MapBuilder<M, K, V> builder) {
        @SuppressWarnings("unchecked")
        M map = (M) getMap(id);
        if (map == null) {
            String configAsString = meta.get(MVMap.getMapKey(id));
            if(configAsString != null) {
                HashMap<String, Object> config = New.hashMap();
                HashMap<String, String> cfg = DataUtils.parseMap(configAsString);
                config.putAll(cfg);
                config.put("id", id);
                map = builder.create(this, config);
                map.init();
                long root = getRootPos(meta, id);
                map.setRootPos(root, lastStoredVersion);
                maps.put(id, map);
            }
        }
        return map;
    }

    public <K,V> MVMap<K,V> getMap(int id) {
        checkOpen();
        @SuppressWarnings("unchecked")
        MVMap<K,V> map = (MVMap<K, V>) maps.get(id);
        return map;
    }

    /**
     * Get the set of all map names.
     *
     * @return the set of names
     */
    public synchronized Set<String> getMapNames() {
        HashSet<String> set = New.hashSet();
        checkOpen();
        for (Iterator<String> it = meta.keyIterator("name."); it.hasNext();) {
            String x = it.next();
            if (!x.startsWith("name.")) {
                break;
            }
            String mapName = x.substring("name.".length());
            set.add(mapName);
        }
        return set;
    }

    /**
     * Get the metadata map. This data is for informational purposes only. The
     * data is subject to change in future versions.
     * <p>
     * The data in this map should not be modified (changing system data may
     * corrupt the store). If modifications are needed, they need be
     * synchronized on the store.
     * <p>
     * The metadata map contains the following entries:
     * <pre>
     * chunk.{chunkId} = {chunk metadata}
     * name.{name} = {mapId}
     * map.{mapId} = {map metadata}
     * root.{mapId} = {root position}
     * setting.storeVersion = {version}
     * </pre>
     *
     * @return the metadata map
     */
    public MVMap<String, String> getMetaMap() {
        checkOpen();
        return meta;
    }

    private MVMap<String, String> getMetaMap(long version) {
        Chunk c = getChunkForVersion(version);
        if(c == null || c.block == Long.MAX_VALUE) {
            return meta;
        } else {
            c = readChunkHeader(c.block);
            MVMap<String, String> oldMeta = meta.openReadOnly(c.metaRootPos, version);
            return oldMeta;
        }
    }

    private Chunk getChunkForVersion(long version) {
        Chunk newest = null;
        for (Chunk c : chunks.values()) {
            if (c.version <= version) {
                if (newest == null || c.id > newest.id) {
                    newest = c;
                }
            }
        }
        return newest;
    }

    /**
     * Check whether a given map exists.
     *
     * @param name the map name
     * @return true if it exists
     */
    public boolean hasMap(String name) {
        return meta.containsKey("name." + name);
    }

    private void markMetaChanged() {
        // changes in the metadata alone are usually not detected, as the meta
        // map is changed after storing
        metaChanged = true;
    }

    private void readStoreHeader() {
        Chunk newest = null;
        boolean validStoreHeader = false;
        // find out which chunk and version are the newest
        // read the first two blocks
        ByteBuffer fileHeaderBlocks = fileStore.readFully(0, 2 * BLOCK_SIZE);
        byte[] buff = new byte[BLOCK_SIZE];
        for (int i = 0; i <= BLOCK_SIZE; i += BLOCK_SIZE) {
            fileHeaderBlocks.get(buff);
            // the following can fail for various reasons
            try {
                String s = new String(buff, 0, BLOCK_SIZE,
                        DataUtils.LATIN).trim();
                HashMap<String, String> m = DataUtils.parseMap(s);
                int blockSize = DataUtils.readHexInt(
                        m, "blockSize", BLOCK_SIZE);
                if (blockSize != BLOCK_SIZE) {
                    throw DataUtils.newIllegalStateException(
                            DataUtils.ERROR_UNSUPPORTED_FORMAT,
                            "Block size {0} is currently not supported",
                            blockSize);
                }
                int check = DataUtils.readHexInt(m, "fletcher", 0);
                m.remove("fletcher");
                s = s.substring(0, s.lastIndexOf("fletcher") - 1);
                byte[] bytes = s.getBytes(DataUtils.LATIN);
                int checksum = DataUtils.getFletcher32(bytes,
                        bytes.length);
                if (check != checksum) {
                    continue;
                }
                long version = DataUtils.readHexLong(m, "version", 0);
                if (newest == null || version > newest.version) {
                    validStoreHeader = true;
                    storeHeader.putAll(m);
                    creationTime = DataUtils.readHexLong(m, "created", 0);
                    int chunkId = DataUtils.readHexInt(m, "chunk", 0);
                    long block = DataUtils.readHexLong(m, "block", 0);
                    Chunk test = readChunkHeaderAndFooter(block);
                    if (test != null && test.id == chunkId) {
                        newest = test;
                    }
                }
            } catch (Exception ignore) {/**/}
        }
        if (!validStoreHeader) {
            throw DataUtils.newIllegalStateException(
                    DataUtils.ERROR_FILE_CORRUPT,
                    "Store header is corrupt: {0}", fileStore);
        }
        long format = DataUtils.readHexLong(storeHeader, "format", 1);
        if (format > FORMAT_WRITE && !fileStore.isReadOnly()) {
            throw DataUtils.newIllegalStateException(
                    DataUtils.ERROR_UNSUPPORTED_FORMAT,
                    "The write format {0} is larger " +
                    "than the supported format {1}, " +
                    "and the file was not opened in read-only mode",
                    format, FORMAT_WRITE);
        }
        format = DataUtils.readHexLong(storeHeader, "formatRead", format);
        if (format > FORMAT_READ) {
            throw DataUtils.newIllegalStateException(
                    DataUtils.ERROR_UNSUPPORTED_FORMAT,
                    "The read format {0} is larger " +
                    "than the supported format {1}",
                    format, FORMAT_READ);
        }
        lastStoredVersion = -1;
        chunks.clear();
        long now = System.currentTimeMillis();
        // calculate the year (doesn't have to be exact;
        // we assume 365.25 days per year, * 4 = 1461)
        int year =  1970 + (int) (now / (1000L * 60 * 60 * 6 * 1461));
        if (year < 2014) {
            // if the year is before 2014,
            // we assume the system doesn't have a real-time clock,
            // and we set the creationTime to the past, so that
            // existing chunks are overwritten
            creationTime = now - fileStore.getDefaultRetentionTime();
        } else if (now < creationTime) {
            // the system time was set to the past:
            // we change the creation time
            creationTime = now;
            storeHeader.put("created", creationTime);
        }
        Chunk test = readChunkFooter(fileStore.size());
        if (test != null) {
            test = readChunkHeaderAndFooter(test.block);
            if (test != null) {
                if (newest == null || test.version > newest.version) {
                    newest = test;
                }
            }
        }
        if (newest == null) {
            // no chunk
            return;
        }
        // read the chunk header and footer,
        // and follow the chain of next chunks
        while (true) {
            if (newest.next == 0 ||
                    newest.next >= fileStore.size() / BLOCK_SIZE) {
                // no (valid) next
                break;
            }
            test = readChunkHeaderAndFooter(newest.next);
            if (test == null || test.id <= newest.id) {
                break;
            }
            newest = test;
        }
        setLastChunk(newest);
        loadChunkMeta();
        fileStore.clear();
        // build the free space list
        for (Chunk c : chunks.values()) {
            if (c.pageCountLive == 0) {
                // remove this chunk in the next save operation
                registerFreePage(currentVersion, c.id, 0, 0);
            }
            long start = c.block * BLOCK_SIZE;
            int length = c.len * BLOCK_SIZE;
            fileStore.markUsed(start, length);
        }
        assert fileStore.getLast() == _getFileLengthInUse() : fileStore.getLast() + " != " + _getFileLengthInUse();
        // read all chunk headers and footers within the retention time,
        // to detect unwritten data after a power failure
        verifyLastChunks();
    }

    private void loadChunkMeta() {
        // load the chunk metadata: we can load in any order,
        // because loading chunk metadata might recursively load another chunk
        for (Iterator<String> it = meta.keyIterator("chunk."); it.hasNext();) {
            String s = it.next();
            if (!s.startsWith("chunk.")) {
                break;
            }
            s = meta.get(s);
            Chunk c = Chunk.fromString(s);
            if (chunks.putIfAbsent(c.id, c) == null) {
                if (c.block == Long.MAX_VALUE) {
                    throw DataUtils.newIllegalStateException(
                            DataUtils.ERROR_FILE_CORRUPT,
                            "Chunk {0} is invalid", c.id);
                }
            }
        }
    }

    private void setLastChunk(Chunk last) {
        lastChunk = last;
        if (last == null) {
            // no valid chunk
            lastMapId = 0;
            currentVersion = 0;
            meta.setRootPos(0, INITIAL_VERSION);
        } else {
            lastMapId = last.mapId;
            currentVersion = last.version;
            chunks.put(last.id, last);
            meta.setRootPos(last.metaRootPos, lastStoredVersion);
        }
        setWriteVersion(currentVersion);
        if(versionChangeListener != null) {
            versionChangeListener.onVersionChange(currentVersion);
        }
    }

    private void verifyLastChunks() {
        long time = getTimeSinceCreation();
        assert lastChunk == null || chunks.containsKey(lastChunk.id) : lastChunk;
        ArrayList<Integer> ids = new ArrayList<>(chunks.keySet());
        Collections.sort(ids);
        int newestValidChunk = -1;
        Chunk old = null;
        for (Integer chunkId : ids) {
            Chunk c = chunks.get(chunkId);
            if (old != null && c.time < old.time) {
                // old chunk (maybe leftover from a previous crash)
                break;
            }
            old = c;
            if (c.time + retentionTime < time) {
                // old chunk, no need to verify
                newestValidChunk = c.id;
                continue;
            }
            Chunk test = readChunkHeaderAndFooter(c.block);
            if (test == null || test.id != c.id) {
                break;
            }
            newestValidChunk = chunkId;
        }
        Chunk newest = chunks.get(newestValidChunk);
        if (newest != lastChunk) {
            // to avoid re-using newer chunks later on, we could clear
            // the headers and footers of those, but we might not know about all
            // of them, so that could be incomplete - but we check that newer
            // chunks are written after older chunks, so we are safe
            rollbackTo(newest == null ? 0 : newest.version);
        }
    }

    /**
     * Read a chunk header and footer, and verify the stored data is consistent.
     *
     * @param block the block
     * @return the chunk, or null if the header or footer don't match or are not
     *         consistent
     */
    private Chunk readChunkHeaderAndFooter(long block) {
        Chunk header;
        try {
            header = readChunkHeader(block);
        } catch (Exception e) {
            // invalid chunk header: ignore, but stop
            return null;
        }
        if (header == null) {
            return null;
        }
        Chunk footer = readChunkFooter((block + header.len) * BLOCK_SIZE);
        if (footer == null || footer.id != header.id) {
            return null;
        }
        return header;
    }

    /**
     * Try to read a chunk footer.
     *
     * @param end the end of the chunk
     * @return the chunk, or null if not successful
     */
    private Chunk readChunkFooter(long end) {
        // the following can fail for various reasons
        try {
            // read the chunk footer of the last block of the file
            long pos = end - Chunk.FOOTER_LENGTH;
            if(pos < 0) {
                return null;
            }
            ByteBuffer lastBlock = fileStore.readFully(pos, Chunk.FOOTER_LENGTH);
            byte[] buff = new byte[Chunk.FOOTER_LENGTH];
            lastBlock.get(buff);
            String s = new String(buff, DataUtils.LATIN).trim();
            HashMap<String, String> m = DataUtils.parseMap(s);
            int check = DataUtils.readHexInt(m, "fletcher", 0);
            m.remove("fletcher");
            s = s.substring(0, s.lastIndexOf("fletcher") - 1);
            byte[] bytes = s.getBytes(DataUtils.LATIN);
            int checksum = DataUtils.getFletcher32(bytes, bytes.length);
            if (check == checksum) {
                int chunk = DataUtils.readHexInt(m, "chunk", 0);
                Chunk c = new Chunk(chunk);
                c.version = DataUtils.readHexLong(m, "version", 0);
                c.block = DataUtils.readHexLong(m, "block", 0);
                return c;
            }
        } catch (Exception e) {
            // ignore
        }
        return null;
    }

    private void writeStoreHeader() {
        StringBuilder buff = new StringBuilder();
        if (lastChunk != null) {
            storeHeader.put("block", lastChunk.block);
            storeHeader.put("chunk", lastChunk.id);
            storeHeader.put("version", lastChunk.version);
        }
        DataUtils.appendMap(buff, storeHeader);
        byte[] bytes = buff.toString().getBytes(DataUtils.LATIN);
        int checksum = DataUtils.getFletcher32(bytes, bytes.length);
        DataUtils.appendMap(buff, "fletcher", checksum);
        buff.append("\n");
        bytes = buff.toString().getBytes(DataUtils.LATIN);
        ByteBuffer header = ByteBuffer.allocate(2 * BLOCK_SIZE);
        header.put(bytes);
        header.position(BLOCK_SIZE);
        header.put(bytes);
        header.rewind();
        write(0, header);
    }

    private void write(long pos, ByteBuffer buffer) {
        try {
            fileStore.writeFully(pos, buffer);
        } catch (IllegalStateException e) {
            panic(e);
            throw e;
        }
    }

    /**
     * Close the file and the store. Unsaved changes are written to disk first.
     */
    public void close() {
        if (closed) {
            return;
        }
        FileStore f = fileStore;
        if (f != null && !f.isReadOnly()) {
            stopBackgroundThread();
            if (hasUnsavedChanges()) {
                commit();
            }
        }
        closeStore(true);
    }

    /**
     * Close the file and the store, without writing anything. This will stop
     * the background thread. This method ignores all errors.
     */
    public void closeImmediately() {
        try {
            closeStore(false);
        } catch (Exception e) {
            if (backgroundExceptionHandler != null) {
                backgroundExceptionHandler.uncaughtException(null, e);
            }
        }
    }

    private void closeStore(boolean shrinkIfPossible) {
        if (closed) {
            return;
        }
        // can not synchronize on this yet, because
        // the thread also synchronized on this, which
        // could result in a deadlock
        stopBackgroundThread();
        synchronized (this) {
            closed = true;
            if (fileStore != null && shrinkIfPossible) {
                shrinkFileIfPossible(0);
            }
/*
            int updateFailureRatio = (int)(10000 * getUpdateFailureRatio());
            if(updateFailureRatio != 0) {
                System.out.println("UpdateFailurePercent: " + updateFailureRatio / 100 + "." + updateFailureRatio % 100 + "%");
            }
*/
            // release memory early - this is important when called
            // because of out of memory
            cache = null;
            cacheChunkRef = null;
            for (MVMap<?, ?> m : New.arrayList(maps.values())) {
                m.close();
            }
            chunks.clear();
            maps.clear();
            if (fileStore != null && !fileStoreIsProvided) {
                fileStore.close();
            }
        }
    }

    /**
     * Get the chunk for the given position.
     *
     * @param pos the position
     * @return the chunk
     */
    private Chunk getChunk(long pos) {
        Chunk c = getChunkIfFound(pos);
        if (c == null) {
            int chunkId = DataUtils.getPageChunkId(pos);
            throw DataUtils.newIllegalStateException(
                    DataUtils.ERROR_FILE_CORRUPT,
                    "Chunk {0} not found", chunkId);
        }
        return c;
    }

    private Chunk getChunkIfFound(long pos) {
        int chunkId = DataUtils.getPageChunkId(pos);
        Chunk c = chunks.get(chunkId);
        if (c == null) {
            checkOpen();
            String s = meta.get(Chunk.getMetaKey(chunkId));
            if (s == null) {
                return null;
            }
            c = Chunk.fromString(s);
            if (c.block == Long.MAX_VALUE) {
                throw DataUtils.newIllegalStateException(
                        DataUtils.ERROR_FILE_CORRUPT,
                        "Chunk {0} is invalid", chunkId);
            }
            chunks.put(c.id, c);
        }
        return c;
    }

    private void setWriteVersion(long version) {
        for (Iterator<MVMap<?, ?>> iter = maps.values().iterator(); iter.hasNext(); ) {
            MVMap<?, ?> map = iter.next();
            if (map.setWriteVersion(version) == null) {
                assert map.isClosed();
                assert map.getVersion() < getOldestVersionToKeep(null);
                iter.remove();
            }
        }
        meta.setWriteVersion(version);
    }

    /**
     * Commit the changes.
     * <p>
     * This method does nothing if there are no unsaved changes,
     * otherwise it increments the current version
     * and stores the data (for file based stores).
     * <p>
     * It is not necessary to call this method when auto-commit is enabled (the default
     * setting), as in this case it is automatically called from time to time or
     * when enough changes have accumulated. However, it may still be called to
     * flush all changes to disk.
     * <p>
     * At most one store operation may run at any time.
     *
     * @return the new version (incremented if there were changes)
     */
    public long commit() {
        // unlike synchronization, this will also prevent re-entrance,
        // which may be possible, if the meta map have changed
        if (commitIsInProgress.compareAndSet(false, true)) {
            synchronized (this) {
                try {
                    currentStoreVersion = currentVersion;
                    currentStoreThread = Thread.currentThread();
                    if (!closed && hasUnsavedChanges()) {
                        long v;
                        if (fileStore == null) {
                            lastStoredVersion = currentVersion;
                            v = ++currentVersion;
                            setWriteVersion(v);
                            metaChanged = false;
                            if(versionChangeListener != null) {
                                versionChangeListener.onVersionChange(currentVersion);
                            }
                        } else {
                            if (fileStore.isReadOnly()) {
                                throw DataUtils.newIllegalStateException(
                                        DataUtils.ERROR_WRITING_FAILED, "This store is read-only");
                            }
                            try {
                                v = storeNow();
                                assert v == currentVersion : v + " != " + currentVersion;
                            } catch (IllegalStateException e) {
                                panic(e);
                            } catch (Throwable e) {
                                panic(DataUtils.newIllegalStateException(DataUtils.ERROR_INTERNAL, e.toString(), e));
                            }
                        }
                    }
                } finally {
                    // in any case reset the current store version,
                    // to allow closing the store
                    currentStoreVersion = -1;
                    currentStoreThread = null;
                    commitIsInProgress.set(false);
                }
            }
        }
        return currentVersion;
    }

    private long storeNow() {
        assert Thread.holdsLock(this);
        long time = getTimeSinceCreation();
        int freeDelay = retentionTime / 10;
        if (time >= lastFreeUnusedChunks + freeDelay) {
            // set early in case it fails (out of memory or so)
            lastFreeUnusedChunks = time;
            freeUnusedChunks();
            // set it here as well, to avoid calling it often if it was slow
            lastFreeUnusedChunks = getTimeSinceCreation();
        }
        int currentUnsavedPageCount = unsavedMemory;
        long storeVersion = currentStoreVersion;
        long version = ++currentVersion;
        lastCommitTime = time;
        retainChunk = null;

        // the metadata of the last chunk was not stored so far, and needs to be
        // set now (it's better not to update right after storing, because that
        // would modify the meta map again)
        int lastChunkId;
        if (lastChunk == null) {
            lastChunkId = 0;
        } else {
            lastChunkId = lastChunk.id;
            meta.put(Chunk.getMetaKey(lastChunkId), lastChunk.asString());
            // never go backward in time
            time = Math.max(lastChunk.time, time);
        }
        int newChunkId = lastChunkId;
        while (true) {
            newChunkId = (newChunkId + 1) & Chunk.MAX_ID;
            Chunk old = chunks.get(newChunkId);
            if (old == null) {
                break;
            }
            if (old.block == Long.MAX_VALUE) {
                IllegalStateException e = DataUtils.newIllegalStateException(
                        DataUtils.ERROR_INTERNAL,
                        "Last block {0} not stored, possibly due to out-of-memory", old);
                panic(e);
            }
        }
        Chunk c = new Chunk(newChunkId);

        c.pageCount = Integer.MAX_VALUE;
        c.pageCountLive = Integer.MAX_VALUE;
        c.maxLen = Long.MAX_VALUE;
        c.maxLenLive = Long.MAX_VALUE;
        c.metaRootPos = Long.MAX_VALUE;
        c.block = Long.MAX_VALUE;
        c.len = Integer.MAX_VALUE;
        c.time = time;
        c.version = version;
        c.mapId = lastMapId;
        c.next = Long.MAX_VALUE;
        chunks.put(c.id, c);
        // force a metadata update
        meta.put(Chunk.getMetaKey(c.id), c.asString());
        meta.remove(Chunk.getMetaKey(c.id));
        ArrayList<MVMap<?, ?>> list = New.arrayList(maps.values());
        ArrayList<Page> changed = New.arrayList();
        for (Iterator<MVMap<?, ?>> iter = list.iterator(); iter.hasNext(); ) {
            MVMap<?, ?> map = iter.next();
            MVMap.RootReference rootReference = map.setWriteVersion(version);
            if (rootReference == null) {
                assert map.isClosed();
                assert map.getVersion() <getOldestVersionToKeep(null);
                iter.remove();
            } else if (map.getCreateVersion() <= storeVersion && // if map was created after storing started, skip it
                    !map.isVolatile() &&
                    map.hasChangesSince(lastStoredVersion)) {
                Page rootPage = rootReference.root;
                if (rootPage.getPos() == 0 ||
                        // after deletion previously saved leaf
                        // may pop up as a root, but we still need
                        // to save new root pos in meta
                        rootPage.isLeaf()) {
                    changed.add(rootPage);
                }
            }
        }
        applyFreedSpace(storeVersion);
        WriteBuffer buff = getWriteBuffer();
        // need to patch the header later
        c.writeChunkHeader(buff, 0);
        int headerLength = buff.position();
        c.pageCount = 0;
        c.pageCountLive = 0;
        c.maxLen = 0;
        c.maxLenLive = 0;
        for (Page p : changed) {
            String key = MVMap.getMapRootKey(p.getMapId());
            if (p.getTotalCount() == 0) {
                meta.put(key, "0");
            } else {
                p.writeUnsavedRecursive(c, buff);
                long root = p.getPos();
                meta.put(key, Long.toHexString(root));
            }
        }
        MVMap.RootReference metaRootReference = meta.setWriteVersion(version);

        if(versionChangeListener != null) {
            versionChangeListener.onVersionChange(currentVersion);
        }

//        Page metaRoot = meta.getRootPage();
        Page metaRoot = metaRootReference.root;
        metaRoot.writeUnsavedRecursive(c, buff);

        int chunkLength = buff.position();

        // add the store header and round to the next block
        int length = MathUtils.roundUpInt(chunkLength +
                Chunk.FOOTER_LENGTH, BLOCK_SIZE);
        buff.limit(length);

        // the length of the file that is still in use
        // (not necessarily the end of the file)
        long end = getFileLengthInUse();
        long filePos;
        if (reuseSpace) {
            filePos = fileStore.allocate(length);
        } else {
            filePos = end;
        }
        // end is not necessarily the end of the file
        boolean storeAtEndOfFile = filePos + length >= fileStore.size();

        if (!reuseSpace) {
            // we can not mark it earlier, because it
            // might have been allocated by one of the
            // removed chunks
            fileStore.markUsed(end, length);
        }

        c.block = filePos / BLOCK_SIZE;
        c.len = length / BLOCK_SIZE;
        assert fileStore.getLast() == _getFileLengthInUse() : fileStore.getLast() + " != " + _getFileLengthInUse() + " " + c;
        c.metaRootPos = metaRoot.getPos();
        // calculate and set the likely next position
        if (reuseSpace) {
            int predictBlocks = c.len;
            long predictedNextStart = fileStore.allocate(
                    predictBlocks * BLOCK_SIZE);
            fileStore.free(predictedNextStart, predictBlocks * BLOCK_SIZE);
            assert fileStore.getLast() == _getFileLengthInUse() : fileStore.getLast() + " != " + _getFileLengthInUse() + " " + c;
            c.next = predictedNextStart / BLOCK_SIZE;
        } else {
            // just after this chunk
            c.next = 0;
        }
        buff.position(0);
        c.writeChunkHeader(buff, headerLength);
        revertTemp(storeVersion);

        buff.position(buff.limit() - Chunk.FOOTER_LENGTH);
        buff.put(c.getFooterBytes());

        buff.position(0);
        write(filePos, buff.getBuffer());
        releaseWriteBuffer(buff);

        // whether we need to write the store header
        boolean writeStoreHeader = false;
        if (!storeAtEndOfFile) {
            if (lastChunk == null) {
                writeStoreHeader = true;
            } else if (lastChunk.next != c.block) {
                // the last prediction did not matched
                writeStoreHeader = true;
            } else {
                long headerVersion = DataUtils.readHexLong(
                        storeHeader, "version", 0);
                if (lastChunk.version - headerVersion > 20) {
                    // we write after at least 20 entries
                    writeStoreHeader = true;
                } else {
                    int chunkId = DataUtils.readHexInt(storeHeader, "chunk", 0);
                    while (true) {
                        Chunk old = chunks.get(chunkId);
                        if (old == null) {
                            // one of the chunks in between
                            // was removed
                            writeStoreHeader = true;
                            break;
                        }
                        if (chunkId == lastChunk.id) {
                            break;
                        }
                        chunkId++;
                    }
                }
            }
        }

        lastChunk = c;
        if (writeStoreHeader) {
            writeStoreHeader();
        }
        if (!storeAtEndOfFile) {
            // may only shrink after the store header was written
            shrinkFileIfPossible(1);
        }
        for (Page p : changed) {
            if (p.getTotalCount() > 0) {
                p.writeEnd();
            }
        }
        metaRoot.writeEnd();

        // some pages might have been changed in the meantime (in the newest
        // version)
        unsavedMemory = Math.max(0, unsavedMemory
                - currentUnsavedPageCount);

        metaChanged = false;
        lastStoredVersion = storeVersion;

        return version;
    }

    private synchronized void freeUnusedChunks() {
        if (lastChunk == null || !reuseSpace) {
            return;
        }
        Set<Integer> referenced = collectReferencedChunks();
        long time = getTimeSinceCreation();

        for (Iterator<Chunk> iterator = chunks.values().iterator(); iterator.hasNext(); ) {
            Chunk c = iterator.next();
            if (c.block != Long.MAX_VALUE && !referenced.contains(c.id)) {
                if (canOverwriteChunk(c, time)) {
                    iterator.remove();
                    markMetaChanged();
                    meta.remove(Chunk.getMetaKey(c.id));
                    long start = c.block * BLOCK_SIZE;
                    int length = c.len * BLOCK_SIZE;
                    fileStore.free(start, length);
                    assert fileStore.getLast() == _getFileLengthInUse() : fileStore.getLast() + " != " + _getFileLengthInUse();
                } else {
                    if (c.unused == 0) {
                        c.unused = time;
                        meta.put(Chunk.getMetaKey(c.id), c.asString());
                        markMetaChanged();
                    }
                }
            }
        }
    }

    private Set<Integer> collectReferencedChunks() {
        long testVersion = lastChunk.version;
        DataUtils.checkArgument(testVersion > 0, "Collect references on version 0");
        long readCount = getFileStore().readCount;
        Set<Integer> referenced = New.hashSet();
        long oldestVersionToKeep = getOldestVersionToKeep(null);

        collectReferencedChunks(referenced, meta.getId(), lastChunk.metaRootPos, 0);
/*
        MVMap.RootReference rootReference = meta.getRoot();
        do {
            if(rootReference.root.getPos() != 0) {
                collectReferencedChunks(referenced, meta.getId(), rootReference.root.getPos(), 0);
            }

            for (Cursor<String, String> c = new Cursor<>(meta, rootReference.root, "root.", true); c.hasNext(); ) {
                String key = c.next();
                assert key != null;
                if (!key.startsWith("root.")) {
                    break;
                }
                long pos = DataUtils.parseHexLong(c.getValue());
                if (pos != 0) {
                    int mapId = DataUtils.parseHexInt(key.substring("root.".length()));
                    collectReferencedChunks(referenced, mapId, pos, 0);
                }
            }
        } while(rootReference.version >= oldestVersionToKeep && (rootReference = rootReference.previous) != null);

/*/
        for (MVMap.RootReference rootReference = meta.getRoot();
                rootReference != null && (rootReference.version >= oldestVersionToKeep/* || rootReference.version == INITIAL_VERSION*/);
                rootReference = rootReference.previous) {

            long pos = rootReference.root.getPos();
            if(pos != 0) {
                collectReferencedChunks(referenced, meta.getId(), pos, 0);
            }

            for (Cursor<String, String> c = new Cursor<>(meta, rootReference.root, "root.", true); c.hasNext(); ) {
                String key = c.next();
                assert key != null;
                if (!key.startsWith("root.")) {
                    break;
                }
                pos = DataUtils.parseHexLong(c.getValue());
                if (pos != 0) {
                    int mapId = DataUtils.parseHexInt(key.substring("root.".length()));
                    collectReferencedChunks(referenced, mapId, pos, 0);
                }
            }

//            if(rootReference.version < oldestVersionToKeep) {
//                break;
//            }
            assert !referenced.isEmpty();
        }
//*/
        readCount = fileStore.readCount - readCount;
        return referenced;
    }

    private void collectReferencedChunks(Set<Integer> targetChunkSet,
            int mapId, long pos, int level) {
        int c = DataUtils.getPageChunkId(pos);
        targetChunkSet.add(c);
        if (DataUtils.getPageType(pos) == DataUtils.PAGE_TYPE_LEAF) {
            return;
        }
        PageChildren refs = readPageChunkReferences(mapId, pos);
        if (!refs.chunkList) {
            Set<Integer> target = New.hashSet();
            for (long p : refs.children) {
                collectReferencedChunks(target, mapId, p, level + 1);
            }
            // we don't need a reference to this chunk
            target.remove(c);
            long[] children = new long[target.size()];
            int i = 0;
            for (Integer p : target) {
                children[i++] = DataUtils.getPagePos(p, 0, 0,
                        DataUtils.PAGE_TYPE_LEAF);
            }
            refs.children = children;
            refs.chunkList = true;
            if (cacheChunkRef != null) {
                cacheChunkRef.put(refs.pos, refs, refs.getMemory());
            }
        }
        for (long p : refs.children) {
            targetChunkSet.add(DataUtils.getPageChunkId(p));
        }
    }

    private PageChildren readPageChunkReferences(int mapId, long pos) {
        PageChildren r;
        if (cacheChunkRef != null) {
            r = cacheChunkRef.get(pos);
        } else {
            r = null;
        }
        if (r == null) {
            // if possible, create it from the cached page
            if (cache != null) {
                Page p = cache.get(pos);
                if (p != null) {
                    r = new PageChildren(p);
                }
            }
            if (r == null) {
                // page was not cached: read the data
                Chunk c = getChunk(pos);
                long filePos = c.block * BLOCK_SIZE;
                filePos += DataUtils.getPageOffset(pos);
                if (filePos < 0) {
                    throw DataUtils.newIllegalStateException(
                            DataUtils.ERROR_FILE_CORRUPT,
                            "Negative position {0}; p={1}, c={2}", filePos, pos, c.toString());
                }
                long maxPos = (c.block + c.len) * BLOCK_SIZE;
                r = PageChildren.read(fileStore, pos, mapId, filePos, maxPos);
            }
            r.removeDuplicateChunkReferences();
            if (cacheChunkRef != null) {
                cacheChunkRef.put(pos, r, r.getMemory());
            }
        }
//        assert r.children.length > 0;
        return r;
    }

    /**
     * Get a buffer for writing. This caller must synchronize on the store
     * before calling the method and until after using the buffer.
     *
     * @return the buffer
     */
    private WriteBuffer getWriteBuffer() {
        WriteBuffer buff;
        if (writeBuffer != null) {
            buff = writeBuffer;
            buff.clear();
        } else {
            buff = new WriteBuffer();
        }
        return buff;
    }

    /**
     * Release a buffer for writing. This caller must synchronize on the store
     * before calling the method and until after using the buffer.
     *
     * @param buff the buffer than can be re-used
     */
    private void releaseWriteBuffer(WriteBuffer buff) {
        if (buff.capacity() <= 4 * 1024 * 1024) {
            writeBuffer = buff;
        }
    }

    private boolean canOverwriteChunk(Chunk c, long time) {
        if (retentionTime >= 0) {
            if (c.time + retentionTime > time) {
                return false;
            }
            if (c.unused == 0 || c.unused + retentionTime / 2 > time) {
                return false;
            }
        }
        Chunk r = retainChunk;
        if (r != null && c.version > r.version) {
            return false;
        }
        return true;
    }

    private long getTimeSinceCreation() {
        return Math.max(0, getTimeAbsolute() - creationTime);
    }

    private long getTimeAbsolute() {
        long now = System.currentTimeMillis();
        if (lastTimeAbsolute != 0 && now < lastTimeAbsolute) {
            // time seems to have run backwards - this can happen
            // when the system time is adjusted, for example
            // on a leap second
            now = lastTimeAbsolute;
        } else {
            lastTimeAbsolute = now;
        }
        return now;
    }

    /**
     * Apply the freed space to the chunk metadata. The metadata is updated, but
     * completely free chunks are not removed from the set of chunks, and the
     * disk space is not yet marked as free.
     *
     * @param storeVersion apply up to the given version
     */
    private void applyFreedSpace(long storeVersion) {
        while (true) {
            ArrayList<Chunk> modified = New.arrayList();
            Iterator<Entry<Long, HashMap<Integer, Chunk>>> it;
            it = freedPageSpace.entrySet().iterator();
            while (it.hasNext()) {
                Entry<Long, HashMap<Integer, Chunk>> e = it.next();
                long v = e.getKey();
                if (v > storeVersion) {
                    continue;
                }
                HashMap<Integer, Chunk> freed = e.getValue();
                synchronized (freed) {
                    for (Chunk f : freed.values()) {
                        Chunk c = chunks.get(f.id);
                        if (c == null) {
                            // already removed
                            continue;
                        }
                        // no need to synchronize, as old entries
                        // are not concurrently modified
                        c.maxLenLive += f.maxLenLive;
                        c.pageCountLive += f.pageCountLive;
                        if (c.pageCountLive < 0 && c.pageCountLive > -MARKED_FREE) {
                            // can happen after a rollback
                            c.pageCountLive = 0;
                        }
                        if (c.maxLenLive < 0 && c.maxLenLive > -MARKED_FREE) {
                            // can happen after a rollback
                            c.maxLenLive = 0;
                        }
                        modified.add(c);
                    }
                }
                it.remove();
            }
            for (Chunk c : modified) {
                meta.put(Chunk.getMetaKey(c.id), c.asString());
            }
            if (modified.size() == 0) {
                break;
            }
        }
    }

    /**
     * Shrink the file if possible, and if at least a given percentage can be
     * saved.
     *
     * @param minPercent the minimum percentage to save
     */
    private void shrinkFileIfPossible(int minPercent) {
        if (fileStore.isReadOnly()) {
            return;
        }
        long end = getFileLengthInUse();
        long fileSize = fileStore.size();
        if (end >= fileSize) {
            return;
        }
        if (minPercent > 0 && fileSize - end < BLOCK_SIZE) {
            return;
        }
        int savedPercent = (int) (100 - (end * 100 / fileSize));
        if (savedPercent < minPercent) {
            return;
        }
        if (!closed) {
            sync();
        }
        fileStore.truncate(end);
    }

    /**
     * Get the position of the last used byte.
     *
     * @return the position
     */
    private long getFileLengthInUse() {
        long result = fileStore.getLast();
        assert result == _getFileLengthInUse() : result + " != " + _getFileLengthInUse();
        return result;
    }

    private long _getFileLengthInUse() {
        long size = 2;
        for (Chunk c : chunks.values()) {
            if (c.len != Integer.MAX_VALUE) {
                size = Math.max(size, c.block + c.len);
            }
        }
        return size * BLOCK_SIZE;
    }

    /**
     * Check whether there are any unsaved changes.
     *
     * @return if there are any changes
     */
    public boolean hasUnsavedChanges() {
        checkOpen();
        if (metaChanged) {
            return true;
        }
        for (MVMap<?, ?> m : maps.values()) {
            if (!m.isClosed()) {
                if(m.hasChangesSince(lastStoredVersion)) {
                    return true;
                }
            }
        }
        return false;
    }

    private Chunk readChunkHeader(long block) {
        long p = block * BLOCK_SIZE;
        ByteBuffer buff = fileStore.readFully(p, Chunk.MAX_HEADER_LENGTH);
        return Chunk.readChunkHeader(buff, p);
    }

    /**
     * Compact the store by moving all live pages to new chunks.
     *
     * @return if anything was written
     */
    public synchronized boolean compactRewriteFully() {
        checkOpen();
        if (lastChunk == null) {
            // nothing to do
            return false;
        }
        for (MVMap<?, ?> m : maps.values()) {
            @SuppressWarnings("unchecked")
            MVMap<Object, Object> map = (MVMap<Object, Object>) m;
            Cursor<Object, Object> cursor = map.cursor(null);
            Page lastPage = null;
            while (cursor.hasNext()) {
                cursor.next();
                Page p = cursor.getPage();
                if (p == lastPage) {
                    continue;
                }
                Object k = p.getKey(0);
                Object v = p.getValue(0);
                map.put(k, v);
                lastPage = p;
            }
        }
        commit();
        return true;
    }

    /**
     * Compact by moving all chunks next to each other.
     *
     * @return if anything was written
     */
    public synchronized boolean compactMoveChunks() {
        return compactMoveChunks(100, Long.MAX_VALUE);
    }

    /**
     * Compact the store by moving all chunks next to each other, if there is
     * free space between chunks. This might temporarily increase the file size.
     * Chunks are overwritten irrespective of the current retention time. Before
     * overwriting chunks and before resizing the file, syncFile() is called.
     *
     * @param targetFillRate do nothing if the file store fill rate is higher
     *            than this
     * @param moveSize the number of bytes to move
     * @return if anything was written
     */
    public synchronized boolean compactMoveChunks(int targetFillRate, long moveSize) {
        checkOpen();
        if (lastChunk == null || !reuseSpace) {
            // nothing to do
            return false;
        }
        int oldRetentionTime = retentionTime;
        boolean oldReuse = reuseSpace;
        try {
            retentionTime = -1;
            freeUnusedChunks();
            if (fileStore.getFillRate() > targetFillRate) {
                return false;
            }
            long start = fileStore.getFirstFree() / BLOCK_SIZE;
            ArrayList<Chunk> move = compactGetMoveBlocks(start, moveSize);
            compactMoveChunks(move);
            freeUnusedChunks();
            storeNow();
        } finally {
            reuseSpace = oldReuse;
            retentionTime = oldRetentionTime;
        }
        return true;
    }

    private ArrayList<Chunk> compactGetMoveBlocks(long startBlock, long moveSize) {
        ArrayList<Chunk> move = New.arrayList();
        for (Chunk c : chunks.values()) {
            if (c.block > startBlock) {
                move.add(c);
            }
        }
        // sort by block
        Collections.sort(move, new Comparator<Chunk>() {
            @Override
            public int compare(Chunk o1, Chunk o2) {
                return Long.signum(o1.block - o2.block);
            }
        });
        // find which is the last block to keep
        int count = 0;
        long size = 0;
        for (Chunk c : move) {
            long chunkSize = c.len * (long) BLOCK_SIZE;
            size += chunkSize;
            if (size > moveSize) {
                break;
            }
            count++;
        }
        // move the first block (so the first gap is moved),
        // and the one at the end (so the file shrinks)
        while (move.size() > count && move.size() > 1) {
            move.remove(1);
        }

        return move;
    }

    private void compactMoveChunks(ArrayList<Chunk> move) {
        for (Chunk c : move) {
            WriteBuffer buff = getWriteBuffer();
            long start = c.block * BLOCK_SIZE;
            int length = c.len * BLOCK_SIZE;
            buff.limit(length);
            ByteBuffer readBuff = fileStore.readFully(start, length);
            Chunk.readChunkHeader(readBuff, start);
            int chunkHeaderLen = readBuff.position();
            buff.position(chunkHeaderLen);
            buff.put(readBuff);
            long pos = getFileLengthInUse();
            fileStore.markUsed(pos, length);
            fileStore.free(start, length);
            c.block = pos / BLOCK_SIZE;
            c.next = 0;
            buff.position(0);
            c.writeChunkHeader(buff, chunkHeaderLen);
            buff.position(length - Chunk.FOOTER_LENGTH);
            buff.put(c.getFooterBytes());
            buff.position(0);
            write(pos, buff.getBuffer());
            releaseWriteBuffer(buff);
            markMetaChanged();
            meta.put(Chunk.getMetaKey(c.id), c.asString());
        }

        // update the metadata (store at the end of the file)
        reuseSpace = false;
        commit();
        sync();

        // now re-use the empty space
        reuseSpace = true;
        compactRewrite(Collections.singleton(lastChunk));
        for (Chunk c : move) {
            if (!chunks.containsKey(c.id)) {
                // already removed during the
                // previous store operation
                continue;
            }
            WriteBuffer buff = getWriteBuffer();
            long start = c.block * BLOCK_SIZE;
            int length = c.len * BLOCK_SIZE;
            buff.limit(length);
            ByteBuffer readBuff = fileStore.readFully(start, length);
            Chunk.readChunkHeader(readBuff, start);
            int chunkHeaderLen = readBuff.position();
            buff.position(chunkHeaderLen);
            buff.put(readBuff);
            long pos = fileStore.allocate(length);
            assert fileStore.getLast() == _getFileLengthInUse() : fileStore.getLast() + " != " + _getFileLengthInUse();
            fileStore.free(start, length);
            assert fileStore.getLast() == _getFileLengthInUse() : fileStore.getLast() + " != " + _getFileLengthInUse();
            c.block = pos / BLOCK_SIZE;
            c.next = 0;
            buff.position(0);
            c.writeChunkHeader(buff, chunkHeaderLen);
            buff.position(length - Chunk.FOOTER_LENGTH);
            buff.put(c.getFooterBytes());
            buff.position(0);
            write(pos, buff.getBuffer());
            releaseWriteBuffer(buff);
            markMetaChanged();
            meta.put(Chunk.getMetaKey(c.id), c.asString());
        }

        // update the metadata (within the file)
        commit();
        sync();
        shrinkFileIfPossible(0);
    }

    /**
     * Force all stored changes to be written to the storage. The default
     * implementation calls FileChannel.force(true).
     */
    public void sync() {
        checkOpen();
        FileStore f = fileStore;
        if (f != null) {
            f.sync();
        }
    }

    /**
     * Try to increase the fill rate by re-writing partially full chunks. Chunks
     * with a low number of live items are re-written.
     * <p>
     * If the current fill rate is higher than the target fill rate, nothing is
     * done.
     * <p>
     * Please note this method will not necessarily reduce the file size, as
     * empty chunks are not overwritten.
     * <p>
     * Only data of open maps can be moved. For maps that are not open, the old
     * chunk is still referenced. Therefore, it is recommended to open all maps
     * before calling this method.
     *
     * @param targetFillRate the minimum percentage of live entries
     * @param write the minimum number of bytes to write
     * @return if a chunk was re-written
     */
    public boolean compact(int targetFillRate, int write) {
        if (!reuseSpace) {
            return false;
        }
        synchronized (compactSync) {
            checkOpen();
            ArrayList<Chunk> old;
            synchronized (this) {
                old = compactGetOldChunks(targetFillRate, write);
            }
            if (old == null || old.isEmpty()) {
                return false;
            }
            compactRewrite(old);
            return true;
        }
    }

    private ArrayList<Chunk> compactGetOldChunks(int targetFillRate, int write) {
        if (lastChunk == null) {
            // nothing to do
            return null;
        }

        // calculate the fill rate
        long maxLengthSum = 0;
        long maxLengthLiveSum = 0;

        long time = getTimeSinceCreation();

        for (Chunk c : chunks.values()) {
            // ignore young chunks, because we don't optimize those
            if (c.time + retentionTime <= time) {
                maxLengthSum += c.maxLen;
                maxLengthLiveSum += c.maxLenLive;
            }
        }
        if (maxLengthLiveSum < 0) {
            // no old data
            return null;
        }
        // the fill rate of all chunks combined
        if (maxLengthSum <= 0) {
            // avoid division by 0
            maxLengthSum = 1;
        }
        int fillRate = (int) (100 * maxLengthLiveSum / maxLengthSum);
        if (fillRate >= targetFillRate) {
            return null;
        }

        // the 'old' list contains the chunks we want to free up
        ArrayList<Chunk> old = New.arrayList();
        Chunk last = chunks.get(lastChunk.id);
        for (Chunk c : chunks.values()) {
            // only look at chunk older than the retention time
            // (it's possible to compact chunks earlier, but right
            // now we don't do that)
            if (c.time + retentionTime <= time) {
                long age = last.version - c.version + 1;
                c.collectPriority = (int) (c.getFillRate() * 1000 / Math.max(1,age));
                old.add(c);
            }
        }
        if (old.isEmpty()) {
            return null;
        }

        // sort the list, so the first entry should be collected first
        Collections.sort(old, new Comparator<Chunk>() {
            @Override
            public int compare(Chunk o1, Chunk o2) {
                int comp = Integer.compare(o1.collectPriority, o2.collectPriority);
                if (comp == 0) {
                    comp = Long.compare(o1.maxLenLive, o2.maxLenLive);
                }
                return comp;
            }
        });
        // find out up to were in the old list we need to move
        long written = 0;
        int chunkCount = 0;
        Chunk move = null;
        for (Chunk c : old) {
            if (move != null) {
                if (c.collectPriority > 0 && written > write) {
                    break;
                }
            }
            written += c.maxLenLive;
            chunkCount++;
            move = c;
        }
        if (chunkCount < 1) {
            return null;
        }
        // remove the chunks we want to keep from this list
        boolean remove = false;
        for (Iterator<Chunk> it = old.iterator(); it.hasNext();) {
            Chunk c = it.next();
            if (move == c) {
                remove = true;
            } else if (remove) {
                it.remove();
            }
        }
        return old;
    }

    private void compactRewrite(Iterable<Chunk> old) {
        HashSet<Integer> set = New.hashSet();
        for (Chunk c : old) {
            set.add(c.id);
        }
        for (MVMap<?, ?> m : maps.values()) {
            @SuppressWarnings("unchecked")
            MVMap<Object, Object> map = (MVMap<Object, Object>) m;
            if (!map.rewrite(set)) {
                return;
            }
        }
        if (!meta.rewrite(set)) {
            return;
        }
        freeUnusedChunks();
        commit();
    }

    /**
     * Read a page.
     *
     * @param map the map
     * @param pos the page position
     * @return the page
     */
    Page readPage(MVMap<?, ?> map, long pos) {
        if (pos == 0) {
            throw DataUtils.newIllegalStateException(
                    DataUtils.ERROR_FILE_CORRUPT, "Position 0");
        }
        Page p = cache == null ? null : cache.get(pos);
        if (p == null) {
            Chunk c = getChunk(pos);
            long filePos = c.block * BLOCK_SIZE;
            filePos += DataUtils.getPageOffset(pos);
            if (filePos < 0) {
                throw DataUtils.newIllegalStateException(
                        DataUtils.ERROR_FILE_CORRUPT,
                        "Negative position {0}", filePos);
            }
            long maxPos = (c.block + c.len) * BLOCK_SIZE;
            p = Page.read(fileStore, pos, map, filePos, maxPos);
            cachePage(p);
        }
        return p;
    }

    /**
     * Remove a page.
     *
     * @param map the map the page belongs to
     * @param pos the position of the page
     * @param memory the memory usage
     */
    void removePage(MVMap<?, ?> map, long pos, int memory) {
        // we need to keep temporary pages,
        // to support reading old versions and rollback
        if (pos == 0) {
            // the page was not yet stored:
            // just using "unsavedMemory -= memory" could result in negative
            // values, because in some cases a page is allocated, but never
            // stored, so we need to use max
            unsavedMemory = Math.max(0, unsavedMemory - memory);
            return;
        }

        // This could result in a cache miss if the operation is rolled back,
        // but we don't optimize for rollback.
        // We could also keep the page in the cache, as somebody
        // could still read it (reading the old version).
        if (cache != null) {
            if (DataUtils.getPageType(pos) == DataUtils.PAGE_TYPE_LEAF) {
                // keep nodes in the cache, because they are still used for
                // garbage collection
                cache.remove(pos);
            }
        }

        Chunk c = getChunkIfFound(pos);
        if(c != null) {
            long version = currentVersion;
            if (map == meta && currentStoreVersion >= 0) {
                if (Thread.currentThread() == currentStoreThread) {
                    // if the meta map is modified while storing,
                    // then this freed page needs to be registered
                    // with the stored chunk, so that the old chunk
                    // can be re-used
                    version = currentStoreVersion;
                }
            }
            registerFreePage(version, c.id,
                    DataUtils.getPageMaxLength(pos), 1);
        }
    }

    private void registerFreePage(long version, int chunkId,
            long maxLengthLive, int pageCount) {
        HashMap<Integer, Chunk> freed = freedPageSpace.get(version);
        if (freed == null) {
            freed = New.hashMap();
            HashMap<Integer, Chunk> f2 = freedPageSpace.putIfAbsent(version,
                    freed);
            if (f2 != null) {
                freed = f2;
            }
        }
        // synchronize, because pages could be freed concurrently
        synchronized (freed) {
            Chunk f = freed.get(chunkId);
            if (f == null) {
                f = new Chunk(chunkId);
                freed.put(chunkId, f);
            }
            f.maxLenLive -= maxLengthLive;
            f.pageCountLive -= pageCount;
        }
    }

    Compressor getCompressorFast() {
        if (compressorFast == null) {
            compressorFast = new CompressLZF();
        }
        return compressorFast;
    }

    Compressor getCompressorHigh() {
        if (compressorHigh == null) {
            compressorHigh = new CompressDeflate();
        }
        return compressorHigh;
    }

    int getCompressionLevel() {
        return compressionLevel;
    }

    public int getPageSplitSize() {
        return pageSplitSize;
    }

    public int getKeysPerPage() {
        return keysPerPage;
    }

    public long getMaxPageSize() {
        return cache == null ? Long.MAX_VALUE : cache.getMaxItemSize() >> 4;
    }

    public boolean getReuseSpace() {
        return reuseSpace;
    }

    /**
     * Whether empty space in the file should be re-used. If enabled, old data
     * is overwritten (default). If disabled, writes are appended at the end of
     * the file.
     * <p>
     * This setting is specially useful for online backup. To create an online
     * backup, disable this setting, then copy the file (starting at the
     * beginning of the file). In this case, concurrent backup and write
     * operations are possible (obviously the backup process needs to be faster
     * than the write operations).
     *
     * @param reuseSpace the new value
     */
    public void setReuseSpace(boolean reuseSpace) {
        this.reuseSpace = reuseSpace;
    }

    public int getRetentionTime() {
        return retentionTime;
    }

    /**
     * How long to retain old, persisted chunks, in milliseconds. Chunks that
     * are older may be overwritten once they contain no live data.
     * <p>
     * The default value is 45000 (45 seconds) when using the default file
     * store. It is assumed that a file system and hard disk will flush all
     * write buffers within this time. Using a lower value might be dangerous,
     * unless the file system and hard disk flush the buffers earlier. To
     * manually flush the buffers, use
     * <code>MVStore.getFile().force(true)</code>, however please note that
     * according to various tests this does not always work as expected
     * depending on the operating system and hardware.
     * <p>
     * The retention time needs to be long enough to allow reading old chunks
     * while traversing over the entries of a map.
     * <p>
     * This setting is not persisted.
     *
     * @param ms how many milliseconds to retain old chunks (0 to overwrite them
     *            as early as possible)
     */
    public void setRetentionTime(int ms) {
        this.retentionTime = ms;
    }

    /**
     * How many versions to retain for in-memory stores. If not set, 5 old
     * versions are retained.
     *
     * @param count the number of versions to keep
     */
    public void setVersionsToKeep(int count) {
        this.versionsToKeep = count;
    }

    /**
     * Get the oldest version to retain in memory (for in-memory stores).
     *
     * @return the version
     */
    public long getVersionsToKeep() {
        return versionsToKeep;
    }

    /**
     * Get the oldest version to retain in memory, which is the manually set
     * retain version, or the current store version (whatever is older).
     *
     * @return the version
     */
    public long getOldestVersionToKeep(MVMap map) {
        long v = oldestVersionToKeep.get();
        if(v == NOT_SET) {
            v = map == null ? currentVersion : map.getVersion();
            if (fileStore == null) {
                v = Math.max(v - versionsToKeep, INITIAL_VERSION);
                return v;
            }
        }

        long storeVersion = lastStoredVersion;
        if (storeVersion < v && storeVersion != INITIAL_VERSION) {
            v = storeVersion;
        }
        return v;
    }

    public void setOldestVersionToKeep(long oldestVersionToKeep) {
        boolean success;
        do {
            long current = this.oldestVersionToKeep.get();
//            assert oldestVersionToKeep >= current || current == NOT_SET : oldestVersionToKeep + " < " + current;
            success = oldestVersionToKeep <= current && current != NOT_SET ||
                      this.oldestVersionToKeep.compareAndSet(current, oldestVersionToKeep);
        } while (!success);
    }

    /**
     * Check whether all data can be read from this version. This requires that
     * all chunks referenced by this version are still available (not
     * overwritten).
     *
     * @param version the version
     * @return true if all data can be read
     */
    private boolean isKnownVersion(long version) {
        if (version > currentVersion || version < 0) {
            return false;
        }
        if (version == currentVersion || chunks.isEmpty()) {
            // no stored data
            return true;
        }
        // need to check if a chunk for this version exists
        Chunk c = getChunkForVersion(version);
        if (c == null) {
            return false;
        }
        // also, all chunks referenced by this version
        // need to be available in the file
        MVMap<String, String> oldMeta = getMetaMap(version);
        if (oldMeta == null) {
            return false;
        }
        try {
            for (Iterator<String> it = oldMeta.keyIterator("chunk.");
                    it.hasNext();) {
                String chunkKey = it.next();
                if (!chunkKey.startsWith("chunk.")) {
                    break;
                }
                if (!meta.containsKey(chunkKey)) {
                    String s = oldMeta.get(chunkKey);
                    Chunk c2 = Chunk.fromString(s);
                    Chunk test = readChunkHeaderAndFooter(c2.block);
                    if (test == null || test.id != c2.id) {
                        return false;
                    }
                    // we store this chunk
                    chunks.put(c2.id, c2);
                }
            }
        } catch (IllegalStateException e) {
            // the chunk missing where the metadata is stored
            return false;
        }
        return true;
    }

    /**
     * Increment the number of unsaved pages.
     *
     * @param memory the memory usage of the page
     */
    public void registerUnsavedPage(int memory) {
        unsavedMemory += memory;
        int newValue = unsavedMemory;
        if (newValue > autoCommitMemory && autoCommitMemory > 0) {
            saveNeeded = true;
        }
    }

    public boolean isSaveNeeded() {
        return saveNeeded;
    }

    /**
     * This method is called before writing to a map.
     *
     * @param map the map
     */
    void beforeWrite(MVMap<?, ?> map) {
        if (saveNeeded && fileStore != null && !closed && autoCommitDelay > 0) {
/*
            if (map == meta) {
                // to, don't save while the metadata map is locked
                // this is to avoid deadlocks that could occur when we
                // synchronize on the store and then on the metadata map
                // TODO there should be no deadlocks possible
                return;
            }
*/
            saveNeeded = false;
            // check again, because it could have been written by now
            if (unsavedMemory > autoCommitMemory && autoCommitMemory > 0) {
                commit();
            }
        }
    }

    /**
     * Get the store version. The store version is usually used to upgrade the
     * structure of the store after upgrading the application. Initially the
     * store version is 0, until it is changed.
     *
     * @return the store version
     */
    public int getStoreVersion() {
        checkOpen();
        String x = meta.get("setting.storeVersion");
        return x == null ? 0 : DataUtils.parseHexInt(x);
    }

    /**
     * Update the store version.
     *
     * @param version the new store version
     */
    public synchronized void setStoreVersion(int version) {
        checkOpen();
        markMetaChanged();
        meta.put("setting.storeVersion", Integer.toHexString(version));
    }

    /**
     * Revert to the beginning of the current version, reverting all uncommitted
     * changes.
     */
    public void rollback() {
        rollbackTo(currentVersion);
    }

    /**
     * Revert to the beginning of the given version. All later changes (stored
     * or not) are forgotten. All maps that were created later are closed. A
     * rollback to a version before the last stored version is immediately
     * persisted. Rollback to version 0 means all data is removed.
     *
     * @param version the version to revert to
     */
    public synchronized void rollbackTo(long version) {
        checkOpen();
        if (version == 0) {
            // special case: remove all data
            for (MVMap<?, ?> m : maps.values()) {
                m.close();
            }
            meta.setInitialRoot(Page.createEmpty(meta), INITIAL_VERSION);

            chunks.clear();
            if (fileStore != null) {
                fileStore.clear();
            }
            maps.clear();
            freedPageSpace.clear();
            currentVersion = version;
            setWriteVersion(version);
            metaChanged = false;
            return;
        }
        DataUtils.checkArgument(
                isKnownVersion(version),
                "Unknown version {0}", version);
        for (MVMap<?, ?> m : maps.values()) {
            m.rollbackTo(version);
        }
        for (long v = currentVersion; v >= version && !freedPageSpace.isEmpty(); v--) {
            freedPageSpace.remove(v);
        }
        meta.rollbackTo(version);
        metaChanged = false;
        boolean loadFromFile = false;
        // find out which chunks to remove,
        // and which is the newest chunk to keep
        // (the chunk list can have gaps)
        ArrayList<Integer> remove = new ArrayList<Integer>();
        Chunk keep = null;
        for (Chunk c : chunks.values()) {
            if (c.version > version) {
                remove.add(c.id);
            } else if (keep == null || keep.id < c.id) {
                keep = c;
            }
        }
        if (remove.size() > 0) {
            // remove the youngest first, so we don't create gaps
            // (in case we remove many chunks)
            Collections.sort(remove, Collections.reverseOrder());
            revertTemp(version);
            loadFromFile = true;
            for (int id : remove) {
                Chunk c = chunks.remove(id);
                long start = c.block * BLOCK_SIZE;
                int length = c.len * BLOCK_SIZE;
                fileStore.free(start, length);
                assert fileStore.getLast() == _getFileLengthInUse() : fileStore.getLast() + " != " + _getFileLengthInUse();
                // overwrite the chunk,
                // so it is not be used later on
                WriteBuffer buff = getWriteBuffer();
                buff.limit(length);
                // buff.clear() does not set the data
                Arrays.fill(buff.getBuffer().array(), (byte) 0);
                write(start, buff.getBuffer());
                releaseWriteBuffer(buff);
                // only really needed if we remove many chunks, when writes are
                // re-ordered - but we do it always, because rollback is not
                // performance critical
                sync();
            }
            lastChunk = keep;
            writeStoreHeader();
            readStoreHeader();
        }
        for (MVMap<?, ?> m : New.arrayList(maps.values())) {
            int id = m.getId();
            if (m.getCreateVersion() >= version) {
                m.close();
//                maps.remove(id);
            } else {
                if (loadFromFile) {
                    m.setRootPos(getRootPos(meta, id), version);
                }
            }
        }
        // rollback might have rolled back the stored chunk metadata as well
        if (lastChunk != null) {
//            chunks.put(lastChunk.id, lastChunk);
            for (Chunk c : chunks.values()) {
                meta.put(Chunk.getMetaKey(c.id), c.asString());
            }
        }
        currentVersion = version;
//        setWriteVersion(version);       // TODO: remove this as redundant
    }

    private static long getRootPos(MVMap<String, String> map, int mapId) {
        String root = map.get(MVMap.getMapRootKey(mapId));
        return root == null ? 0 : DataUtils.parseHexLong(root);
    }

    private void revertTemp(long storeVersion) {
        Set<Long> keys = freedPageSpace.keySet();
        for (Iterator<Long> it = keys.iterator();
             it.hasNext();) {
            long v = it.next();
            if (v <= storeVersion) {
                it.remove();
            }
        }
        for (Iterator<MVMap<?, ?>> iter = maps.values().iterator(); iter.hasNext(); ) {
            MVMap<?, ?> map = iter.next();
            if (map.removeUnusedOldVersions()) {
                assert map.isClosed();
                assert map.getVersion() < getOldestVersionToKeep(null);
                iter.remove();
            }
        }
    }

    /**
     * Get the current version of the data. When a new store is created, the
     * version is 0.
     *
     * @return the version
     */
    public long getCurrentVersion() {
        return currentVersion;
    }

    public long getLastStoredVersion() {
        return lastStoredVersion;
    }

    /**
     * Get the file store.
     *
     * @return the file store
     */
    public FileStore getFileStore() {
        return fileStore;
    }

    /**
     * Get the store header. This data is for informational purposes only. The
     * data is subject to change in future versions. The data should not be
     * modified (doing so may corrupt the store).
     *
     * @return the store header
     */
    public Map<String, Object> getStoreHeader() {
        return storeHeader;
    }

    private void checkOpen() {
        if (closed) {
            throw DataUtils.newIllegalStateException(DataUtils.ERROR_CLOSED,
                    "This store is closed", panicException);
        }
    }

    /**
     * Rename a map.
     *
     * @param map the map
     * @param newName the new name
     */
    public synchronized void renameMap(MVMap<?, ?> map, String newName) {
        checkOpen();
        DataUtils.checkArgument(map != meta,
                "Renaming the meta map is not allowed");
        int id = map.getId();
        String oldName = getMapName(id);
        if (oldName.equals(newName)) {
            return;
        }
        DataUtils.checkArgument(
                !meta.containsKey("name." + newName),
                "A map named {0} already exists", newName);
        markMetaChanged();
        String x = Integer.toHexString(id);
        meta.remove("name." + oldName);
        meta.put(MVMap.getMapKey(id), map.asString(newName));
        meta.put("name." + newName, x);
    }

    /**
     * Remove a map. Please note rolling back this operation does not restore
     * the data; if you need this ability, use Map.clear().
     *
     * @param map the map to remove
     */
    public void removeMap(MVMap<?, ?> map) {
        removeMap(map, false);
    }

    public synchronized void removeMap(MVMap<?, ?> map, boolean delayed) {
        checkOpen();
        DataUtils.checkArgument(map != meta,
                "Removing the meta map is not allowed");
        map.close();
        MVMap.RootReference rootReference = map.getRoot();
        updateCounter += rootReference.updateCounter;
        updateAttemptCounter += rootReference.updateAttemptCounter;

        int id = map.getId();
        String name = getMapName(id);
        removeMap(name, id, delayed);
    }

    private void removeMap(String name, int id, boolean delayed) {
        markMetaChanged();
        meta.remove(MVMap.getMapKey(id));
        meta.remove("name." + name);
        meta.remove(MVMap.getMapRootKey(id));
        if (!delayed) {
            maps.remove(id);
        }
    }

    public void removeMap(String name) {
        int id = getMapId(name);
        if(id > 0) {
            removeMap(name, id, false);
        }
    }

    /**
     * Get the name of the given map.
     *
     * @param id the map id
     * @return the name, or null if not found
     */
    public synchronized String getMapName(int id) {
        checkOpen();
        String m = meta.get(MVMap.getMapKey(id));
        return m == null ? null : DataUtils.parseMap(m).get("name");
    }

    public synchronized int getMapId(String name) {
        checkOpen();
        String m = meta.get("name." + name);
        return m == null ? -1 : DataUtils.parseHexInt(m);
    }

    /**
     * Commit and save all changes, if there are any, and compact the store if
     * needed.
     */
    void writeInBackground() {
        if (closed) {
            return;
        }

        // could also commit when there are many unsaved pages,
        // but according to a test it doesn't really help

        long time = getTimeSinceCreation();
        if (time <= lastCommitTime + autoCommitDelay) {
            return;
        }
        try {
            commit();
            if (autoCompactFillRate > 0) {
                // whether there were file read or write operations since
                // the last time
                boolean fileOps;
                long fileOpCount = fileStore.getWriteCount() + fileStore.getReadCount();
                if (autoCompactLastFileOpCount != fileOpCount) {
                    fileOps = true;
                } else {
                    fileOps = false;
                }
                // use a lower fill rate if there were any file operations
                int fillRate = fileOps ? autoCompactFillRate / 3 : autoCompactFillRate;
                // TODO how to avoid endless compaction if there is a bug
                // in the bookkeeping?
                compact(fillRate, autoCommitMemory);
                autoCompactLastFileOpCount = fileStore.getWriteCount() + fileStore.getReadCount();
            }
        } catch (Throwable e) {
            if (backgroundExceptionHandler != null) {
                backgroundExceptionHandler.uncaughtException(null, e);
            }
        }
    }

    /**
     * Set the read cache size in MB.
     *
     * @param mb the cache size in MB.
     */
    public void setCacheSize(int mb) {
        final long bytes = (long) mb * 1024 * 1024;
        if (cache != null) {
            cache.setMaxMemory(bytes);
            cache.clear();
        }
        if (cacheChunkRef != null) {
            cacheChunkRef.setMaxMemory(bytes / 4);
            cacheChunkRef.clear();
        }
    }

    public boolean isClosed() {
        return closed;
    }

    private void stopBackgroundThread() {
        BackgroundWriterThread t = backgroundWriterThread;
        if (t == null) {
            return;
        }
        backgroundWriterThread = null;
        if (Thread.currentThread() == t) {
            // within the thread itself - can not join
            return;
        }
        synchronized (t.sync) {
            t.sync.notifyAll();
        }
        if (Thread.holdsLock(this)) {
            // called from storeNow: can not join,
            // because that could result in a deadlock
            return;
        }
        try {
            t.join();
        } catch (Exception e) {
            // ignore
        }
    }

    /**
     * Set the maximum delay in milliseconds to auto-commit changes.
     * <p>
     * To disable auto-commit, set the value to 0. In this case, changes are
     * only committed when explicitly calling commit.
     * <p>
     * The default is 1000, meaning all changes are committed after at most one
     * second.
     *
     * @param millis the maximum delay
     */
    public void setAutoCommitDelay(int millis) {
        if (autoCommitDelay == millis) {
            return;
        }
        autoCommitDelay = millis;
        if (fileStore == null || fileStore.isReadOnly()) {
            return;
        }
        stopBackgroundThread();
        // start the background thread if needed
        if (millis > 0) {
            int sleep = Math.max(1, millis / 10);
            BackgroundWriterThread t =
                    new BackgroundWriterThread(this, sleep,
                            fileStore.toString());
            t.start();
            backgroundWriterThread = t;
        }
    }

    /**
     * Get the auto-commit delay.
     *
     * @return the delay in milliseconds, or 0 if auto-commit is disabled.
     */
    public int getAutoCommitDelay() {
        return autoCommitDelay;
    }

    /**
     * Get the maximum memory (in bytes) used for unsaved pages. If this number
     * is exceeded, unsaved changes are stored to disk.
     *
     * @return the memory in bytes
     */
    public int getAutoCommitMemory() {
        return autoCommitMemory;
    }

    /**
     * Get the estimated memory (in bytes) of unsaved data. If the value exceeds
     * the auto-commit memory, the changes are committed.
     * <p>
     * The returned value is an estimation only.
     *
     * @return the memory in bytes
     */
    public int getUnsavedMemory() {
        return unsavedMemory;
    }

    /**
     * Put the page in the cache.
     * @param page the page
     */
    void cachePage(Page page) {
        if (cache != null) {
            cache.put(page.getPos(), page, page.getMemory());
        }
    }

    /**
     * Get the amount of memory used for caching, in MB.
     * Note that this does not include the page chunk references cache, which is
     * 25% of the size of the page cache.
     *
     * @return the amount of memory used for caching
     */
    public int getCacheSizeUsed() {
        if (cache == null) {
            return 0;
        }
        return (int) (cache.getUsedMemory() / 1024 / 1024);
    }

    /**
     * Get the maximum cache size, in MB.
     * Note that this does not include the page chunk references cache, which is
     * 25% of the size of the page cache.
     *
     * @return the cache size
     */
    public int getCacheSize() {
        if (cache == null) {
            return 0;
        }
        return (int) (cache.getMaxMemory() / 1024 / 1024);
    }

    /**
     * Get the cache.
     *
     * @return the cache
     */
    public CacheLongKeyLIRS<Page> getCache() {
        return cache;
    }

    /**
     * Whether the store is read-only.
     *
     * @return true if it is
     */
    public boolean isReadOnly() {
        return fileStore != null && fileStore.isReadOnly();
    }

    public synchronized double getUpdateFailureRatio() {
        long updateCounter = this.updateCounter;
        long updateAttemptCounter = this.updateAttemptCounter;
        MVMap.RootReference rootReference = meta.getRoot();
        updateCounter += rootReference.updateCounter;
        updateAttemptCounter += rootReference.updateAttemptCounter;
        for (MVMap<?, ?> map : maps.values()) {
            MVMap.RootReference root = map.getRoot();
            updateCounter += root.updateCounter;
            updateAttemptCounter += root.updateAttemptCounter;
        }
        return updateAttemptCounter == 0 ? 0 : 1 - ((double)updateCounter / updateAttemptCounter);
    }

    public synchronized void setVersionChangeListener(VersionChangeListener versionChangeListener) {
        this.versionChangeListener = versionChangeListener;
    }

    public interface VersionChangeListener {
        void onVersionChange(long version);
    }

    /**
     * A background writer thread to automatically store changes from time to
     * time.
     */
    private static class BackgroundWriterThread extends Thread {

        public final Object sync = new Object();
        private final MVStore store;
        private final int sleep;

        private BackgroundWriterThread(MVStore store, int sleep, String fileStoreName) {
            super("MVStore background writer " + fileStoreName);
            this.store = store;
            this.sleep = sleep;
            setDaemon(true);
        }

        @Override
        public void run() {
            while (true) {
                Thread t = store.backgroundWriterThread;
                if (t == null) {
                    break;
                }
                synchronized (sync) {
                    try {
                        sync.wait(sleep);
                    } catch (InterruptedException e) {
                        continue;
                    }
                }
                store.writeInBackground();
            }
        }

    }

    /**
     * A builder for an MVStore.
     */
    public static final class Builder {

        private final HashMap<String, Object> config = New.hashMap();

        private Builder set(String key, Object value) {
            config.put(key, value);
            return this;
        }

        /**
         * Disable auto-commit, by setting the auto-commit delay and auto-commit
         * buffer size to 0.
         *
         * @return this
         */
        public Builder autoCommitDisabled() {
            // we have a separate config option so that
            // no thread is started if the write delay is 0
            // (if we only had a setter in the MVStore,
            // the thread would need to be started in any case)
            set("autoCommitBufferSize", 0);
            return set("autoCommitDelay", 0);
        }

        /**
         * Set the size of the write buffer, in KB disk space (for file-based
         * stores). Unless auto-commit is disabled, changes are automatically
         * saved if there are more than this amount of changes.
         * <p>
         * The default is 1024 KB.
         * <p>
         * When the value is set to 0 or lower, data is not automatically
         * stored.
         *
         * @param kb the write buffer size, in kilobytes
         * @return this
         */
        public Builder autoCommitBufferSize(int kb) {
            return set("autoCommitBufferSize", kb);
        }

        /**
         * Set the auto-compact target fill rate. If the average fill rate (the
         * percentage of the storage space that contains active data) of the
         * chunks is lower, then the chunks with a low fill rate are re-written.
         * Also, if the percentage of empty space between chunks is higher than
         * this value, then chunks at the end of the file are moved. Compaction
         * stops if the target fill rate is reached.
         * <p>
         * The default value is 50 (50%). The value 0 disables auto-compacting.
         * <p>
         *
         * @param percent the target fill rate
         * @return this
         */
        public Builder autoCompactFillRate(int percent) {
            return set("autoCompactFillRate", percent);
        }

        /**
         * Use the following file name. If the file does not exist, it is
         * automatically created. The parent directory already must exist.
         *
         * @param fileName the file name
         * @return this
         */
        public Builder fileName(String fileName) {
            return set("fileName", fileName);
        }

        /**
         * Encrypt / decrypt the file using the given password. This method has
         * no effect for in-memory stores. The password is passed as a
         * char array so that it can be cleared as soon as possible. Please note
         * there is still a small risk that password stays in memory (due to
         * Java garbage collection). Also, the hashed encryption key is kept in
         * memory as long as the file is open.
         *
         * @param password the password
         * @return this
         */
        public Builder encryptionKey(char[] password) {
            return set("encryptionKey", password);
        }

        /**
         * Open the file in read-only mode. In this case, a shared lock will be
         * acquired to ensure the file is not concurrently opened in write mode.
         * <p>
         * If this option is not used, the file is locked exclusively.
         * <p>
         * Please note a store may only be opened once in every JVM (no matter
         * whether it is opened in read-only or read-write mode), because each
         * file may be locked only once in a process.
         *
         * @return this
         */
        public Builder readOnly() {
            return set("readOnly", 1);
        }

        /**
         * Set the read cache size in MB. The default is 16 MB.
         *
         * @param mb the cache size in megabytes
         * @return this
         */
        public Builder cacheSize(int mb) {
            return set("cacheSize", mb);
        }

        /**
         * Set the read cache concurrency. The default is 16, meaning 16
         * segments are used.
         *
         * @param concurrency the cache concurrency
         * @return this
         */
        public Builder cacheConcurrency(int concurrency) {
            return set("cacheConcurrency", concurrency);
        }

        /**
         * Compress data before writing using the LZF algorithm. This will save
         * about 50% of the disk space, but will slow down read and write
         * operations slightly.
         * <p>
         * This setting only affects writes; it is not necessary to enable
         * compression when reading, even if compression was enabled when
         * writing.
         *
         * @return this
         */
        public Builder compress() {
            return set("compress", 1);
        }

        /**
         * Compress data before writing using the Deflate algorithm. This will
         * save more disk space, but will slow down read and write operations
         * quite a bit.
         * <p>
         * This setting only affects writes; it is not necessary to enable
         * compression when reading, even if compression was enabled when
         * writing.
         *
         * @return this
         */
        public Builder compressHigh() {
            return set("compress", 2);
        }

        /**
         * Set the amount of memory a page should contain at most, in bytes,
         * before it is split. The default is 16 KB for persistent stores and 4
         * KB for in-memory stores. This is not a limit in the page size, as
         * pages with one entry can get larger. It is just the point where pages
         * that contain more than one entry are split.
         *
         * @param pageSplitSize the page size
         * @return this
         */
        public Builder pageSplitSize(int pageSplitSize) {
            return set("pageSplitSize", pageSplitSize);
        }

        /**
         * Set the listener to be used for exceptions that occur when writing in
         * the background thread.
         *
         * @param exceptionHandler the handler
         * @return this
         */
        public Builder backgroundExceptionHandler(
                Thread.UncaughtExceptionHandler exceptionHandler) {
            return set("backgroundExceptionHandler", exceptionHandler);
        }

        /**
         * Use the provided file store instead of the default one.
         * <p>
         * File stores passed in this way need to be open. They are not closed
         * when closing the store.
         * <p>
         * Please note that any kind of store (including an off-heap store) is
         * considered a "persistence", while an "in-memory store" means objects
         * are not persisted and fully kept in the JVM heap.
         *
         * @param store the file store
         * @return this
         */
        public Builder fileStore(FileStore store) {
            return set("fileStore", store);
        }

        /**
         * Open the store.
         *
         * @return the opened store
         */
        public MVStore open() {
            return new MVStore(config);
        }

        @Override
        public String toString() {
            return DataUtils.appendMap(new StringBuilder(), config).toString();
        }

        /**
         * Read the configuration from a string.
         *
         * @param s the string representation
         * @return the builder
         */
        public static Builder fromString(String s) {
            HashMap<String, String> config = DataUtils.parseMap(s);
            Builder builder = new Builder();
            builder.config.putAll(config);
            return builder;
        }

    }

}
