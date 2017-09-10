/*
 * Copyright 2004-2014 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.mvstore.db;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.BitSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.atomic.AtomicReferenceArray;

import org.h2.engine.Constants;
import org.h2.engine.Database;
import org.h2.mvstore.Cursor;
import org.h2.mvstore.DataUtils;
import org.h2.mvstore.MVMap;
import org.h2.mvstore.MVStore;
import org.h2.mvstore.Page;
import org.h2.mvstore.WriteBuffer;
import org.h2.mvstore.rtree.MVRTreeMap;
import org.h2.mvstore.type.DataType;
import org.h2.mvstore.type.LongDataType;
import org.h2.mvstore.type.ObjectDataType;
import org.h2.mvstore.type.StringDataType;
import org.h2.util.New;
import org.h2.util.Utils;

/**
 * A store that supports concurrent MVCC read-committed transactions.
 */
public final class TransactionStore implements MVStore.VersionChangeListener {

    private static final int AVG_OPEN_TRANSACTIONS = 0x100;
    public static final String TYPE_REGISTRY_NAME = "_";

    /**
     * The store.
     */
    final MVStore store;

    private final long timeoutMillis;

    /**
     * The persisted map of prepared transactions.
     * Key: transactionId, value: [ status, name ].
     */
    private final MVMap<Integer, Object[]> preparedTransactions;

    /**
     * The undo log.
     * <p>
     * If the first entry for a transaction doesn't have a logId
     * of 0, then the transaction is partially committed (which means rollback
     * is not possible). Log entries are written before the data is changed
     * (write-ahead).
     * <p>
     * Key: opId, value: [ mapId, key, oldValue ].
     */
    private final MVMap<Long, Record> undoLog;

    private final MVMap<String, DataType> typeRegistry;

    /**
     * The map of maps.
     */
//    private final ConcurrentHashMap<Integer, MVMap<Object, VersionedValue>> maps = new ConcurrentHashMap<>();

    private final DataType metaDataType;

    private final DataType dataType;


    private boolean init;

    private int maxTransactionId = 0xffff;
    private final AtomicReferenceArray<Transaction> transactions = new AtomicReferenceArray<>(AVG_OPEN_TRANSACTIONS);
    private volatile Transaction[] spilledTransactions = new Transaction[AVG_OPEN_TRANSACTIONS];
    private final AtomicReference<BitSet> openTransactions = new AtomicReference<>(new BitSet());
    private final AtomicReference<BitSet> committingTransactions = new AtomicReference<>(new BitSet());
    private final ConcurrentLinkedQueue<TxCounter> versions = new ConcurrentLinkedQueue<>();
    private volatile TxCounter currentTxCounter = new TxCounter(MVMap.INITIAL_VERSION);


    /**
     * The next id of a temporary map.
     */
    private int nextTempMapId;

    /**
     * Create a new transaction store.
     *
     * @param store the store
     */
    public TransactionStore(MVStore store) {
        this(store, new DBMetaType(null, store.backgroundExceptionHandler), new ObjectDataType(), 0);
//        this(store, new MetaDataType(store.backgroundExceptionHandler), new ObjectDataType(), 0);
    }

    /**
     * Create a new transaction store.
     * @param store the store
     * @param dataType the data type for map keys and values
     * @param timeoutMillis lock aquisition timeout in milliseconds, 0 means no wait
     */
    public TransactionStore(MVStore store, DataType metaDataType, DataType dataType, long timeoutMillis) {
        this.store = store;
        this.currentTxCounter = new TxCounter(store.getOldestVersionToKeep(null));
        this.metaDataType = metaDataType;
        this.dataType = dataType;
        this.timeoutMillis = timeoutMillis;
        MVMap.Builder<String, DataType> builder2 = new MVMap.Builder<String, DataType>()
                                            .keyType(StringDataType.INSTANCE)
                                            .valueType(metaDataType);

        typeRegistry = store.openMap(TYPE_REGISTRY_NAME, builder2);

        store.stashMapData("openTransactions");
        store.stashMapData("undoLog");

        preparedTransactions = store.openMap("openTransactions",
                                             new MVMap.Builder<Integer, Object[]>());

        RecordType undoLogValueType = new RecordType(this);
        MVMap.Builder<Long, Record> builder = new MVMap.Builder<Long, Record>()
                        .keyType(LongDataType.INSTANCE)
                        .valueType(undoLogValueType);
        undoLog = store.openMap("undoLog", builder);

        this.store.setVersionChangeListener(this);
    }

    public DataType getMetaDataType() {
        return metaDataType;
    }

    public MVMap<Long, Record> getUndoLog() {
        return undoLog;
    }

    /**
     * Initialize the store. This is needed before a transaction can be opened.
     * If the transaction store is corrupt, this method can throw an exception,
     * in which case the store can only be used for reading.
     */
    public void init() {
        init(RollbackListener.NONE);
    }

    public synchronized void init(RollbackListener listener) {
        if(!init) {
            // remove all temporary maps
            for (String mapName : store.getMapNames()) {
                if (mapName.startsWith("temp.")) {
                    MVMap<?,?> temp = store.openMap(mapName);
                    store.removeMap(temp);
                }
            }

            assert undoLog.isEmpty();
            assert openTransactions.get().cardinality() == 0;
            store.unstashMapData(preparedTransactions);
            store.unstashMapData(undoLog);

            Long key = undoLog.firstKey();
            while (key != null) {
                int transactionId = getTransactionId(key);
                key = undoLog.lowerKey(getOperationId(transactionId + 1, 0));
                long logId = getLogId(key) + 1;
                Object[] data = preparedTransactions.get(transactionId);
                int status;
                String name;
                if (data == null) {
                    if (undoLog.containsKey(getOperationId(transactionId, 0))) {
                        status = Transaction.STATUS_OPEN;
                    } else {
                        status = Transaction.STATUS_COMMITTING;
                    }
                    name = null;
                } else {
                    status = (Integer) data[0];
                    name = (String) data[1];
                }
                boolean success;
                do {
                    BitSet original = openTransactions.get();
                    BitSet clone = (BitSet) original.clone();
                    assert !clone.get(transactionId);
                    clone.set(transactionId);
                    success = openTransactions.compareAndSet(original, clone);
                } while(!success);
                registerTransaction(transactionId, status, name, logId, timeoutMillis, listener);

                key = undoLog.ceilingKey(getOperationId(transactionId + 1, 0));
            }
            init = true;
        }
    }

    /**
     * Commit all transactions that are in the committing state, and
     * rollback all open transactions.
     */
    public void endLeftoverTransactions() {
        List<Transaction> list = getOpenTransactions();
        for (Transaction t : list) {
            if(t.getStatus() == Transaction.STATUS_COMMITTING) {
                t.commit();
            } else if(t.getStatus() != Transaction.STATUS_PREPARED) {
                t.rollback();
            }
        }
    }

    /**
     * Set the maximum transaction id, after which ids are re-used. If the old
     * transaction is still in use when re-using an old id, the new transaction
     * fails.
     *
     * @param max the maximum id
     */
    public void setMaxTransactionId(int max) {
        this.maxTransactionId = max;
    }

    /**
     * Combine the transaction id and the log id to an operation id.
     *
     * @param transactionId the transaction id
     * @param logId the log id
     * @return the operation id
     */
    static long getOperationId(int transactionId, long logId) {
        DataUtils.checkArgument(transactionId >= 0 && transactionId < (1 << 24),
                "Transaction id out of range: {0}", transactionId);
        DataUtils.checkArgument(logId >= 0 && logId < (1L << 40),
                "Transaction log id out of range: {0}", logId);
        return ((long) transactionId << 40) | logId;
    }

    /**
     * Get the transaction id for the given operation id.
     *
     * @param operationId the operation id
     * @return the transaction id
     */
    public static int getTransactionId(long operationId) {
        return (int) (operationId >>> 40);
    }

    /**
     * Get the log id for the given operation id.
     *
     * @param operationId the operation id
     * @return the log id
     */
    static long getLogId(long operationId) {
        return operationId & ((1L << 40) - 1);
    }

    /**
     * Get the list of unclosed transactions that have pending writes.
     *
     * @return the list of transactions (sorted by id)
     */
    public List<Transaction> getOpenTransactions() {
        ArrayList<Transaction> list = New.arrayList();
        if(!init) {
            init();
        }
        int transactionId = 0;
        BitSet bitSet = openTransactions.get();
        while((transactionId = bitSet.nextSetBit(transactionId + 1)) > 0) {
            Transaction transaction = getTransaction(transactionId);
            if(transaction != null) {
                transaction = new Transaction(transaction);
                if(transaction.getStatus() != Transaction.STATUS_CLOSED) {
                    list.add(transaction);
                }
            }
        }
        return list;
    }

    /**
     * Close the transaction store.
     */
    public synchronized void close() {
        store.commit();
    }

    /**
     * Begin a new transaction.
     *
     * @return the transaction
     */
    public Transaction begin() {
        return begin(RollbackListener.NONE, timeoutMillis);
    }

    public Transaction begin(RollbackListener listener, long timeoutMillis) {
//        if (!init) {
//            throw DataUtils.newIllegalStateException(
//                    DataUtils.ERROR_TRANSACTION_ILLEGAL_STATE,
//                    "Not initialized");
//        }
        int transactionId;
        boolean success;
        do {
            BitSet original = openTransactions.get();
            transactionId = original.nextClearBit(1);
            if (transactionId > maxTransactionId) {
                throw DataUtils.newIllegalStateException(
                        DataUtils.ERROR_TOO_MANY_OPEN_TRANSACTIONS,
                        "There are {0} open transactions",
                        transactionId - 1);
            }
            BitSet clone = (BitSet) original.clone();
            clone.set(transactionId);
            success = openTransactions.compareAndSet(original, clone);
        } while(!success);
        int status = Transaction.STATUS_OPEN;
        if(timeoutMillis <= 0) {
            timeoutMillis = this.timeoutMillis;
        }
        Transaction transaction = registerTransaction(transactionId, status, null, 0, timeoutMillis, listener);
        return transaction;
    }

    private Transaction registerTransaction(int transactionId, int status, String name,
                                     long logId, long timeoutMillis, RollbackListener listener) {
        Transaction transaction = new Transaction(this, transactionId, status, name, logId, timeoutMillis, listener);
        if(transactionId < AVG_OPEN_TRANSACTIONS) {
            transactions.set(transactionId, transaction);
        } else {
            transactionId -= AVG_OPEN_TRANSACTIONS;
            if(spilledTransactions.length <= transactionId) {
                int newLength = Math.max(spilledTransactions.length + AVG_OPEN_TRANSACTIONS, transactionId + 1);
                spilledTransactions = Arrays.copyOf(spilledTransactions, newLength);
            }
            spilledTransactions[transactionId] = transaction;
        }
        return transaction;
    }

    /**
     * Store a transaction.
     *
     * @param t the transaction
     */
    void storeTransaction(Transaction t) {
        if (t.getStatus() == Transaction.STATUS_PREPARED ||
                t.getName() != null) {
            Object[] v = { t.getStatus(), t.getName() };
            preparedTransactions.put(t.getId(), v);
        }
    }

    /**
     * Log a log entry.
     * @param undoKey transactionId/LogId
     * @param record Record to add
     *
     * @return true if success, false otherwise
     */
    boolean addUndoLogRecord(Long undoKey, Record record) {
        return undoLog.putIfAbsent(undoKey, record) == null;
    }

    /**
     * Remove a log entry.
     * @param undoKey transactionId/LogId
     *
     * @return true if success, false otherwise
     */
    boolean removeUndoLogRecord(Long undoKey) {
        return undoLog.remove(undoKey) != null;
    }

    /**
     * Remove the given map.
     *
     * @param <K> the key type
     * @param <V> the value type
     * @param map the map
     */
    <K, V> void removeMap(TransactionMap<K, V> map) {
//        maps.remove(map.map.getId());
        store.removeMap(map.map, true);
    }

    /**
     * Commit a transaction.
     *  @param t the transaction
     *
     */
    private void commit(Transaction t) {
        if (!store.isClosed()) {
            boolean success;
            int transactionId = t.transactionId;
            do {
                BitSet original = committingTransactions.get();
                assert !original.get(transactionId) : "Double commit";
                BitSet clone = (BitSet) original.clone();
                clone.set(transactionId);
                success = committingTransactions.compareAndSet(original, clone);
            } while(!success);
            try {
                long state = t.setStatus(Transaction.STATUS_COMMITTED);
                long maxLogId = Transaction.getTxLogId(state);
                CommitDecisionMaker decisionMaker = new CommitDecisionMaker(this);
                for (long logId = 0; logId < maxLogId; logId++) {
                    long undoKey = getOperationId(t.getId(), logId);
                    decisionMaker.setOperationId(undoKey);
                    undoLog.operate(undoKey, null, decisionMaker);
                    if (decisionMaker.decision == MVMap.Decision.ABORT) // entry was not there
                    {
                        // partially committed: load next
                        Long nextKey = undoLog.ceilingKey(undoKey);
                        if (nextKey == null || getTransactionId(nextKey) != transactionId) {
                            break;
                        }
                        logId = getLogId(nextKey) - 1;
                    }
                }
            } finally {
                do {
                    BitSet original = committingTransactions.get();
                    assert original.get(transactionId) : "Mysterious bit's disappearance";
                    BitSet clone = (BitSet) original.clone();
                    clone.clear(transactionId);
                    success = committingTransactions.compareAndSet(original, clone);
                } while(!success);
            }
        }
    }

    /**
     * Open the map with the given name.
     *
     * @param <K> the key type
     * @param name the map name
     * @param keyType the key type
     * @param valueType the value type
     * @return the map
     */
    <K> MVMap<K, VersionedValue> openMap(String name,
            DataType keyType, DataType valueType) {
        VersionedValueType vt = valueType == null ? null : new VersionedValueType(valueType);
        MVMap.Builder<K, VersionedValue> builder = new TxMapBuilder<K,VersionedValue>(typeRegistry, dataType)
                .keyType(keyType).valueType(vt);
        MVMap<K, VersionedValue> map = store.openMap(name, builder);
        return map;
    }

    /**
     * Open the map with the given id.
     *
     * @param mapId the id
     * @return the map
     */
    private MVMap<Object, VersionedValue> openMap(int mapId) {
        MVMap<Object, VersionedValue> map = store.getMap(mapId);
        if (map == null) {
            MVMap.Builder<Object, VersionedValue> builder = new TxMapBuilder<Object,VersionedValue>(typeRegistry, dataType);

//            VersionedValueType vt = new VersionedValueType(dataType);
//            MVMap.Builder<Object, VersionedValue> builder =
//                    new MVMap.Builder<Object, VersionedValue>().
//                            keyType(dataType).valueType(vt);
            map = store.openMap(mapId, builder);
//            if(map != null) {
//                MVMap<Object, VersionedValue> existingMap = maps.putIfAbsent(mapId, map);
//                if (existingMap != null) {
//                    map = existingMap;
//                }
//            }
        }
        return map;
    }

    private MVMap<Object,VersionedValue> getMap(int mapId) {
        MVMap<Object, VersionedValue> map = store.getMap(mapId);
        if (map == null && !init) {
            map = openMap(mapId);
        }
        assert map != null : "map with id " + mapId + " is missing" +
                                (init ? "" : " during initialization");
        return map;
    }

    /**
     * Create a temporary map. Such maps are removed when opening the store.
     *
     * @return the map
     */
    synchronized <K,V> MVMap<K,V> createTempMap(DataType keyType, DataType valueType) {
        String mapName = "temp." + nextTempMapId++;
        MVMap.Builder<K, V> builder = new MVMap.Builder<K,V>()
                .keyType(keyType)
                .valueType(valueType);

        return store.openMap(mapName, builder);
    }

    /**
     * Open a temporary map.
     *
     * @param mapName the map name
     * @return the map
     */
    private MVMap<Object, Integer> openTempMap(String mapName) {
        MVMap.Builder<Object, Integer> mapBuilder =
                new MVMap.Builder<Object, Integer>().
                keyType(dataType);
        return store.openMap(mapName, mapBuilder);
    }

    /**
     * End this transaction
     *
     * @param t the transaction
     */
    void endTransaction(Transaction t) {
        int txId = t.transactionId;

        int status = t.getStatus();
        t.closeIt();
        clearTxSavePoint(t.txCounter);

        assert verifyUndoIsEmptyForTx(txId);

        if(t.transactionId < AVG_OPEN_TRANSACTIONS) {
            transactions.set(t.transactionId, null);
        } else {
            spilledTransactions[t.transactionId - AVG_OPEN_TRANSACTIONS] = null;
            int prospectiveLength = spilledTransactions.length >> 1;
            if(prospectiveLength > AVG_OPEN_TRANSACTIONS && openTransactions.get().length() < prospectiveLength) {
                spilledTransactions = Arrays.copyOf(spilledTransactions, prospectiveLength);
            }
        }

        boolean success;
        do {
            BitSet original = openTransactions.get();
            assert original.get(t.transactionId);
            BitSet clone = (BitSet) original.clone();
            clone.clear(t.transactionId);
            success = openTransactions.compareAndSet(original, clone);
        } while(!success);

        assert verifyUndoIsEmptyWhenIdle();

        if (status == Transaction.STATUS_PREPARED
                || status == Transaction.STATUS_COMMITTING
                || status == Transaction.STATUS_COMMITTED) {
            if(!preparedTransactions.isClosed()) {
                preparedTransactions.remove(txId);
            }
        }

        if (store.getAutoCommitDelay() == 0) {
            store.commit();
            return;
        }
        // to avoid having to store the transaction log,
        // if there is no open transaction,
        // and if there have been many changes, store them now
        if (undoLog.isEmpty()) {
            int unsaved = store.getUnsavedMemory();
            int max = store.getAutoCommitMemory();
            // save at 3/4 capacity
            if (unsaved * 4 > max * 3) {
                store.commit();
            }
        }
    }

    private boolean verifyUndoIsEmptyForTx(int txId) {
        Long ceilingKey = undoLog.ceilingKey(getOperationId(txId, 0));
        assert ceilingKey == null || getTransactionId(ceilingKey) != txId :
                "undoLog has leftovers for " + txId + " : " + getTransactionId(ceilingKey) +
                        "/" + getLogId(ceilingKey) + " -> " + undoLog.get(ceilingKey);
        return true;
    }

    private boolean verifyUndoIsEmptyWhenIdle() {
        // if there is no open transaction, undo log suppose to be empty
        Page undLogRootPage = undoLog.getRootPage();
        long count = undLogRootPage.getTotalCount();
        assert count == 0
            || openTransactions.get().cardinality() > 0
            || undLogRootPage != undoLog.getRootPage() : count + " orphan undoLog entries";
        return true;
    }

    private Transaction getTransaction(int transactionId) {
        return  transactionId < AVG_OPEN_TRANSACTIONS ?
                transactions.get(transactionId) :
                spilledTransactions[transactionId - AVG_OPEN_TRANSACTIONS];
    }

    /**
     * Rollback to an old savepoint.
     *
     * @param t the transaction
     * @param toLogId the log id to roll back to
     */
    void rollbackTo(final Transaction t, long fromLogId, long toLogId) {
        int transactionId = t.getId();
        RollbackDecisionMaker decisionMaker = new RollbackDecisionMaker(this, transactionId, toLogId, t.listener);
        for (long logId = fromLogId - 1; logId >= toLogId; logId--) {
            Long undoKey = getOperationId(transactionId, logId);
            undoLog.operate(undoKey, null, decisionMaker);
            decisionMaker.reset();
        }
    }

    /**
     * Get the changes of the given transaction, starting from the latest log id
     * back to the given log id.
     *
     * @param t the transaction
     * @param maxLogId the maximum log id
     * @param toLogId the minimum log id
     * @return the changes
     */
    Iterator<Change> getChanges(final Transaction t, final long maxLogId,
            final long toLogId) {
        return new Iterator<Change>() {

            private long logId = maxLogId - 1;
            private Change current;

            private void fetchNext() {
                while (logId >= toLogId) {
                    Long undoKey = getOperationId(t.getId(), logId);
                    Record op = undoLog.get(undoKey);
                    logId--;
                    if (op == null) {
                        // partially rolled back: load previous
                        undoKey = undoLog.floorKey(undoKey);
                        if (undoKey == null || getTransactionId(undoKey) != t.getId()) {
                            break;
                        }
                        logId = getLogId(undoKey);
                        continue;
                    }
                    int mapId = op.mapId;
                    MVMap<Object, VersionedValue> m = openMap(mapId);
                    if (m != null) { // could be null if map was removed later on
                        current = new Change(m.getName(), op.key, op.oldValue == null ? null : op.oldValue.value);
                        return;
                    }
                }
                current = null;
            }

            @Override
            public boolean hasNext() {
                if(current == null) {
                    fetchNext();
                }
                return current != null;
            }

            @Override
            public Change next() {
                if(!hasNext()) {
                    throw DataUtils.newUnsupportedOperationException("no data");
                }
                Change result = current;
                current = null;
                return result;
            }

            @Override
            public void remove() {
                throw DataUtils.newUnsupportedOperationException("remove");
            }

        };
    }

    /**
     * Register opened transaction.
     * This would increment usage counter for the current version.
     * This version (and all after it) should not be dropped until all
     * transactions involved are closed and usage counter goes to zero.
     * @return TxCounter to be decremented when transaction closed.
     */
    private TxCounter registerTxSavePoint() {
        TxCounter txCounter;
        while(true) {
            txCounter = currentTxCounter;
            if(txCounter.counter.getAndIncrement() >= 0) {
                break;
            }
            // The only way for counter to be negative
            // if it was retrieved right before onVersionChange()
            // and now onVersionChange() is done.
            // Thit version is available for reclamation now
            // and should not be used here, so restore count
            // not to upset accounting and try again with a new
            // version (currentTxCounter should have changed).
            assert txCounter != currentTxCounter : txCounter;
            txCounter.counter.decrementAndGet();
        }
        return txCounter;
    }

    private void clearTxSavePoint(TxCounter txCounter) {
        if(txCounter != null) {
            if(txCounter.counter.decrementAndGet() <= 0) {
                dropUnusedVersions();
            }
        }
    }

    @Override
    public void onVersionChange(long version) {
        TxCounter txCounter = this.currentTxCounter;
        versions.add(txCounter);
        currentTxCounter = new TxCounter(version);
        txCounter.counter.decrementAndGet();
        dropUnusedVersions();
    }

    private void dropUnusedVersions() {
        TxCounter txCounter;
        while ((txCounter = versions.peek()) != null
                && txCounter.counter.get() < 0
                && versions.remove(txCounter)) {/**/}
        store.setOldestVersionToKeep(txCounter == null ? currentTxCounter.version : txCounter.version);
    }

    public static final class TxCounter {
        private final long version;
        private final AtomicInteger counter = new AtomicInteger();

        private TxCounter(long version) {
            this.version = version;
        }

        @Override
        public String toString() {
            return "v=" + version + " / cnt=" + counter;
        }
    }

    public static final class TxMapBuilder<K,V> extends MVMap.Builder<K,V> {

        private final MVMap<String, DataType> typeRegistry;
        private final DataType defaultDataType;

        public TxMapBuilder(MVMap<String, DataType> typeRegistry, DataType defaultDataType) {
            this.typeRegistry = typeRegistry;
            this.defaultDataType = defaultDataType;
        }

        private String registerDataType(DataType dataType) {
            String key = getDataTypeRegistrationKey(dataType);
            DataType registeredDataType = typeRegistry.putIfAbsent(key, dataType);
            if(registeredDataType != null) {
                // TODO: ensure type consistency
            }
            return key;
        }

        private static String getDataTypeRegistrationKey(DataType dataType) {
            return Integer.toHexString(Utils.hashCode(dataType));
        }

        @Override
        public MVMap<K,V> create(MVStore store, Map<String, Object> config) {
            DataType keyType = getKeyType();
            if (keyType == null) {
                String keyTypeKey = (String) config.remove("key");
                if (keyTypeKey != null) {
                    keyType = typeRegistry.get(keyTypeKey);
                    if (keyType == null) {
                        throw DataUtils.newIllegalStateException(DataUtils.ERROR_UNKNOWN_DATA_TYPE,
                                "Data type with hash {0} can not be found", keyTypeKey);
                    }
                    setKeyType(keyType);
                }
            } else {
                registerDataType(keyType);
            }

            DataType valueType = getValueType();
            if (valueType == null) {
                String valueTypeKey = (String) config.remove("val");
                if (valueTypeKey != null) {
                    valueType = typeRegistry.get(valueTypeKey);
                    if (valueType == null) {
                        throw DataUtils.newIllegalStateException(DataUtils.ERROR_UNKNOWN_DATA_TYPE,
                                "Data type with hash {0} can not be found", valueTypeKey);
                    }
                    setValueType(valueType);
                }
            } else {
                registerDataType(valueType);
            }

            if (getKeyType() == null) {
                setKeyType(defaultDataType);
                registerDataType(getKeyType());
            }
            if (getValueType() == null) {
                setValueType(new VersionedValueType(defaultDataType));
                registerDataType(getValueType());
            }

            config.put("store", store);
            config.put("key", getKeyType());
            config.put("val", getValueType());
            return create(config);
        }


        @Override
        protected MVMap<K,V> create(Map<String,Object> config) {
            if("rtree".equals(config.get("type"))) {
                MVMap<K, V> map = (MVMap<K, V>) new MVRTreeMap<V>(config);
                return map;
            }
            return new TMVMap<K,V>(config);
        }

        private static final class TMVMap<K,V> extends MVMap<K,V> {
            private final String type;

            private TMVMap(Map<String, Object> config) {
                super(config);
                type = (String)config.get("type");
            }

            private TMVMap(MVMap<K, V> source) {
                super(source);
                type = source.getType();
            }

            @Override
            protected MVMap<K, V> cloneIt() {
                return new TMVMap<K, V>(this);
            }

            @Override
            public String getType() {
                return type;
            }

            @Override
            protected String asString(String name) {
                StringBuilder buff = new StringBuilder();
                buff.append(super.asString(name));
                DataUtils.appendMap(buff, "key", getDataTypeRegistrationKey(getKeyType()));
                DataUtils.appendMap(buff, "val", getDataTypeRegistrationKey(getValueType()));
                return buff.toString();
            }
        }
    }

    /**
     * A change in a map.
     */
    public static final class Change {

        /**
         * The name of the map where the change occurred.
         */
        public final String mapName;

        /**
         * The key.
         */
        public final Object key;

        /**
         * The value.
         */
        public final Object value;

        public Change(String mapName, Object key, Object value) {
            this.mapName = mapName;
            this.key = key;
            this.value = value;
        }
    }

    /**
     * A transaction.
     */
    public static final class Transaction {

        /**
         * The status of a closed transaction (committed or rolled back).
         */
        public static final int STATUS_CLOSED = 0;

        /**
         * The status of an open transaction.
         */
        public static final int STATUS_OPEN = 1;

        /**
         * The status of a prepared transaction.
         */
        public static final int STATUS_PREPARED = 2;

        /**
         * The status of a transaction that is being committed, but possibly not
         * yet finished. A transactions can go into this state when the store is
         * closed while the transaction is committing. When opening a store,
         * such transactions should be committed.
         */
        public static final int STATUS_COMMITTING   = 3;
        public static final int STATUS_COMMITTED    = 4;
        public static final int STATUS_ROLLING_BACK = 5;
        public static final int STATUS_ROLLED_BACK  = 6;

        public static final String STATUS_NAMES[] = {
                "CLOSED", "OPEN", "PREPARED", "COMMITTING",
                "COMMITTED", "ROLLING_BACK", "ROLLED_BACK"
        };

        /**
         * The transaction store.
         */
        final TransactionStore store;

        final RollbackListener listener;

        /**
         * The transaction id.
         */
        public final int transactionId;

        /*
         * Transation state is an atomic composite field:
         * bit 44       : flag whether transaction had rollback(s)
         * bits 42-40   : status
         * bits 39-0    : log id
         */
        private final AtomicLong statusAndLogId;

        private String name;

        private final long timeoutMillis;

        private int blockingTransactionId;

        private int ownerId;

        private TxCounter txCounter;

        private Transaction(TransactionStore store, int transactionId, int status,
                            String name, long logId, long timeoutMillis, RollbackListener listener) {
            this.store = store;
            this.transactionId = transactionId;
            this.statusAndLogId = new AtomicLong(composeState(status, logId, false));
            this.name = name;
            this.timeoutMillis = timeoutMillis;
            this.listener = listener;
        }

        private Transaction(Transaction tx) {
            this.store = tx.store;
            this.transactionId = tx.transactionId;
            this.statusAndLogId = new AtomicLong(tx.statusAndLogId.get());
            this.name = tx.name;
            this.timeoutMillis = tx.timeoutMillis;
            this.blockingTransactionId = tx.blockingTransactionId;
            this.ownerId = tx.ownerId;
            this.txCounter = tx.txCounter;
            this.listener = RollbackListener.NONE;
        }

        public int getId() {
            return transactionId;
        }

        public static int getStatus(long state) {
            return (int)(state >>> 40) & 15;
        }

        public static long getTxLogId(long state) {
            return state & ((1L << 40) - 1);
        }

        public static boolean hasRollback(long state) {
            return (state & (1L << 44)) != 0;
        }

        public static boolean hasChanges(long state) {
            return getTxLogId(state) != 0;
        }

        public static long composeState(int status, long logId, boolean hasRollback) {
            if (hasRollback) {
                status |= 16;
            }
            return ((long)status << 40) | logId;
        }

        public int getStatus() {
            return getStatus(statusAndLogId.get());
        }

        private long setStatus(int status) {
            while (true) {
                long currentState = statusAndLogId.get();
                long logId = getTxLogId(currentState);
                int currentStatus = getStatus(currentState);
                validateStatusTransition(currentStatus, status);
                long newState = composeState(status, logId, hasRollback(currentState));
                if (statusAndLogId.compareAndSet(currentState, newState)) {
                    return currentState;
                }
            }
        }

        private static void validateStatusTransition(int currentStatus, int newStatus) {
            boolean valid;
            switch (newStatus) {
                case STATUS_OPEN:
                    valid = currentStatus == STATUS_CLOSED ||
                            currentStatus == STATUS_ROLLING_BACK;
                    break;
                case STATUS_ROLLING_BACK:
                    valid = currentStatus == STATUS_OPEN;
                    break;
                case STATUS_PREPARED:
                    valid = currentStatus == STATUS_OPEN;
                    break;
                case STATUS_COMMITTING:
                    valid = currentStatus == STATUS_OPEN ||
                            currentStatus == STATUS_PREPARED ||
                            // this case is only possible if called
                            // from endLeftoverTransactions()
                            currentStatus == STATUS_COMMITTING;
                    break;
                case STATUS_COMMITTED:
                    valid = currentStatus == STATUS_COMMITTING;
                    break;
                case STATUS_ROLLED_BACK:
                    valid = currentStatus == STATUS_OPEN ||
                            currentStatus == STATUS_PREPARED;
                    break;
                case STATUS_CLOSED:
                    valid = currentStatus == STATUS_COMMITTING ||
                            currentStatus == STATUS_COMMITTED ||
                            currentStatus == STATUS_ROLLED_BACK;
                    break;
               default:
                   valid = false;
                   break;
            }
            if (!valid) {
                throw DataUtils.newIllegalStateException(
                        DataUtils.ERROR_TRANSACTION_ILLEGAL_STATE,
                        "Transaction was illegally transitioned from {0} to {1}",
                        STATUS_NAMES[currentStatus], STATUS_NAMES[newStatus]);
            }
        }

        public long getLogId() {
            return getTxLogId(statusAndLogId.get());
        }

        public boolean hasChanges() {
            return hasChanges(statusAndLogId.get());
        }

        public int getOwnerId() {
            return ownerId;
        }

        public void setOwnerId(int ownerId) {
            assert this.ownerId == 0 : this.ownerId;
            this.ownerId = ownerId;
        }

        public void setName(String name) {
            checkNotClosed();
            this.name = name;
            store.storeTransaction(this);
        }

        public String getName() {
            return name;
        }

        public int getBlockerId() {
            if(blockingTransactionId != 0) {
                Transaction tx = store.getTransaction(blockingTransactionId);
                if(tx != null) {
                    return tx.getOwnerId();
                }
            }
            return 0;
        }

        /**
         * Create a new savepoint.
         *
         * @return the savepoint id
         */
        public long setSavepoint() {
            return getLogId();
        }

        public void markStatementStart() {
            markStatementEnd();
            txCounter = store.registerTxSavePoint();
        }

        public void markStatementEnd() {
            TxCounter counter = txCounter;
            txCounter = null;
            if(counter != null) {
                store.clearTxSavePoint(counter);
            }
        }

        /**
         * Add a log entry.
         *
         * @param mapId the map id
         * @param key the key
         * @param oldValue the old value
         */
        void log(int mapId, Object key, VersionedValue oldValue) {
            long currentState = statusAndLogId.getAndIncrement();
            long logId = getTxLogId(currentState);
            int currentStatus = getStatus(currentState);
            checkOpen(currentStatus);
            Long undoKey = getOperationId(transactionId, logId);
            Record log = new Record(mapId, key, oldValue);
            if(!store.addUndoLogRecord(undoKey, log)) {
                if (logId == 0) {
                    throw DataUtils.newIllegalStateException(
                            DataUtils.ERROR_TOO_MANY_OPEN_TRANSACTIONS,
                            "An old transaction with the same id " +
                            "is still open: {0}",
                            transactionId);
                }
                throw DataUtils.newIllegalStateException(
                        DataUtils.ERROR_TRANSACTION_CORRUPT,
                        "Duplicate keys in transaction log {0}/{1}",
                        transactionId, logId);
            }
        }

        /**
         * Remove the last log entry.
         */
        void logUndo() {
            long currentState = statusAndLogId.decrementAndGet();
            long logId = getTxLogId(currentState);
            int currentStatus = getStatus(currentState);
            checkOpen(currentStatus);
            Long undoKey = getOperationId(transactionId, logId);
            if (!store.removeUndoLogRecord(undoKey)) {
                throw DataUtils.newIllegalStateException(
                        DataUtils.ERROR_TRANSACTION_ILLEGAL_STATE,
                        "Transaction {0} was concurrently rolled back",
                        transactionId);
            }
            assert logId > 0 || store.verifyUndoIsEmptyForTx(transactionId);
        }

        /**
         * Open a data map.
         *
         * @param <K> the key type
         * @param <V> the value type
         * @param name the name of the map
         * @return the transaction map
         */
        public <K, V> TransactionMap<K, V> openMap(String name) {
            return openMap(name, null, null);
        }

        /**
         * Open the map to store the data.
         *
         * @param <K> the key type
         * @param <V> the value type
         * @param name the name of the map
         * @param keyType the key data type
         * @param valueType the value data type
         * @return the transaction map
         */
        public <K, V> TransactionMap<K, V> openMap(String name,
                DataType keyType, DataType valueType) {
            checkNotClosed();
            MVMap<K, VersionedValue> map = store.openMap(name, keyType,
                    valueType);
            return new TransactionMap<>(this, map);
        }

        /**
         * Open the transactional version of the given map.
         *
         * @param <K> the key type
         * @param <V> the value type
         * @param map the base map
         * @return the transactional map
         */
        public <K, V> TransactionMap<K, V> openMap(
                MVMap<K, VersionedValue> map) {
            checkNotClosed();
            return new TransactionMap<>(this, map);
        }

        /**
         * Prepare the transaction. Afterwards, the transaction can only be
         * committed or completely rolled back.
         */
        public void prepare() {
            setStatus(STATUS_PREPARED);
            store.storeTransaction(this);
        }

        /**
         * Commit the transaction. Afterwards, this transaction is closed.
         */
        public void commit() {
            assert store.openTransactions.get().get(transactionId);
            long state = setStatus(STATUS_COMMITTING);
            Throwable ex = null;
            try {
                if (hasChanges(state)) {
//                    int status = getStatus(state);
//                    if (!store.committingTransactions.get().get(transactionId) /*|| status != STATUS_COMMITTING*/) {
//                        assert store.openTransactions.get().get(transactionId) : this + " " + store.openTransactions;
//                        assert status == Transaction.STATUS_OPEN ||
//                                status == Transaction.STATUS_PREPARED && store.preparedTransactions.get(transactionId) != null ||
//                                // this case is only possible if called from initTransactions()
//                                status == Transaction.STATUS_COMMITTING : status;
                    store.commit(this);
//                    }
                }
            } catch (Throwable e) {
                ex = e;
                throw e;
            } finally {
                try {
                    store.endTransaction(this);
                } catch (Throwable e) {
                    if (ex == null) {
                        throw e;
                    } else {
                        ex.addSuppressed(e);
                    }
                }
            }
        }

        /**
         * Roll back to the given savepoint. This is only allowed if the
         * transaction is open.
         *
         * @param savepointId the savepoint id
         */
        public void rollbackToSavepoint(long savepointId) {
            long lastState = setStatus(STATUS_ROLLING_BACK);
            long logId = getTxLogId(lastState);
            store.rollbackTo(this, logId, savepointId);
            assert savepointId > 0 || store.verifyUndoIsEmptyForTx(transactionId);

            long expectedState = composeState(STATUS_ROLLING_BACK, logId, hasRollback(lastState));
            long newState = composeState(STATUS_OPEN, savepointId, true);
            if (!statusAndLogId.compareAndSet(expectedState, newState)) {
                throw DataUtils.newIllegalStateException(
                        DataUtils.ERROR_TRANSACTION_ILLEGAL_STATE,
                        "Transaction {0} concurrently modified " +
                                "while rollback to savepoint was in progress",
                        transactionId);
            }
        }

        /**
         * Roll the transaction back. Afterwards, this transaction is closed.
         */
        public void rollback() {
            long lastState = setStatus(STATUS_ROLLED_BACK);
            long logId = getTxLogId(lastState);
            if(logId > 0) {
                store.rollbackTo(this, logId, 0);
            }
            store.endTransaction(this);
        }

        /**
         * Get the list of changes, starting with the latest change, up to the
         * given savepoint (in reverse order than they occurred). The value of
         * the change is the value before the change was applied.
         *
         * @param savepointId the savepoint id, 0 meaning the beginning of the
         *            transaction
         * @return the changes
         */
        public Iterator<Change> getChanges(long savepointId) {
            return store.getChanges(this, getLogId(), savepointId);
        }

        /**
         * Check whether this transaction is open.
         */
        private void checkOpen(int status) {
            if (status != STATUS_OPEN) {
                throw DataUtils.newIllegalStateException(
                        DataUtils.ERROR_TRANSACTION_ILLEGAL_STATE,
                        "Transaction {0} has status {1}, not open", transactionId, status);
            }
        }

        /**
         * Check whether this transaction is open or prepared.
         */
        void checkNotClosed(int status) {
            if (status == STATUS_CLOSED) {
                throw DataUtils.newIllegalStateException(
                        DataUtils.ERROR_CLOSED, "Transaction {0} is closed", transactionId);
            }
        }

        void checkNotClosed() {
            checkNotClosed(getStatus());
        }

        private void closeIt() {
            long lastState = setStatus(STATUS_CLOSED);
            if(hasChanges(lastState) || hasRollback(lastState)) {
                _closeIt();
            }
        }

        private synchronized void _closeIt() {
            notifyAll();
        }

        public boolean waitFor(Transaction toWaitFor) {
            synchronized (this) {
                blockingTransactionId = toWaitFor.transactionId;
            }
            return toWaitFor.waitForThisToEnd(timeoutMillis);
        }

        private synchronized boolean waitForThisToEnd(long millis) {
            long until = System.currentTimeMillis() + millis;
            while(getStatus() != STATUS_CLOSED) {
                long dur = until - System.currentTimeMillis();
                if(dur <= 0) {
                    return false;
                }
                try {
                    wait(dur);
                } catch (InterruptedException ex) {
                    return false;
                }
            }
            return true;
        }

        /**
         * Remove the map.
         *
         * @param map the map
         */
        public <K, V> void removeMap(TransactionMap<K, V> map) {
            store.removeMap(map);
        }

        @Override
        public String toString() {
            return "" + transactionId;
        }

    }

    /**
     * A map that supports transactions.
     *
     * @param <K> the key type
     * @param <V> the value type
     */
    public static final class TransactionMap<K, V> {

        /**
         * If a record was read that was updated by this transaction, and the
         * update occurred before this log id, the older version is read. This
         * is so that changes are not immediately visible, to support statement
         * processing (for example "update test set id = id + 1").
         */
        private long readLogId = Long.MAX_VALUE;

        /**
         * The map used for writing (the latest version).
         * <p>
         * Key: key the key of the data.
         * Value: { transactionId, oldVersion, value }
         */
        final MVMap<K, VersionedValue> map;

        private final Transaction transaction;

        TransactionMap(Transaction transaction, MVMap<K, VersionedValue> map) {
            this.transaction = transaction;
            this.map = map;
        }

        /**
         * Set the savepoint. Afterwards, reads are based on the specified
         * savepoint.
         *
         * @param savepoint the savepoint
         */
        public void setSavepoint(long savepoint) {
            this.readLogId = savepoint;
        }

        /**
         * Get a clone of this map for the given transaction.
         *
         * @param transaction the transaction
         * @param savepoint the savepoint
         * @return the map
         */
        public TransactionMap<K, V> getInstance(Transaction transaction,
                long savepoint) {
            TransactionMap<K, V> m = new TransactionMap<>(transaction, map);
            m.setSavepoint(savepoint);
            return m;
        }

        /**
         * Get the size of the raw map. This includes uncommitted entries, and
         * transiently removed entries, so it is the maximum number of entries.
         *
         * @return the maximum size
         */
        public long sizeAsLongMax() {
            return map.sizeAsLong();
        }

        /**
         * Get the size of the map as seen by this transaction.
         *
         * @return the size
         */
        public long sizeAsLong() {
            BitSet committingTransactions;
            MVMap.RootReference mapRootReference;
            MVMap.RootReference undoLogRootReference;
            do {
                committingTransactions = transaction.store.committingTransactions.get();
                mapRootReference = map.getRoot();
                undoLogRootReference = transaction.store.undoLog.getRoot();
            } while(mapRootReference != map.getRoot() || committingTransactions != transaction.store.committingTransactions.get());
            long size = mapRootReference.root.getTotalCount();
            long undoLogSize = undoLogRootReference.root.getTotalCount();
            if (undoLogSize == 0) {
                return size;
            }
            if (undoLogSize > size) {
                // the undo log is larger than the map -
                // count the entries of the map
                size = 0;
                Cursor<K, VersionedValue> cursor = new Cursor<>(map, mapRootReference.root, null, true);
                while (cursor.hasNext()) {
                    K key = cursor.next();
                    VersionedValue data = getValue(key, readLogId, undoLogRootReference.root, committingTransactions, cursor.getValue());
                    if (data != null && data.value != null) {
                        size++;
                    }
                }
                return size;
            }
            // the undo log is smaller than the map -
            // scan the undo log and subtract invisible entries
            MVMap<Object,Object> temp = transaction.store.createTempMap(map.getKeyType(),ObjectDataType.NoneType.INSTANCE);
            try {
                Cursor<Long, Record> cursor = new Cursor<>(transaction.store.undoLog, undoLogRootReference.root, null, true);
                while (cursor.hasNext()) {
                    /*Long undoKey = */cursor.next();
                    Record op = cursor.getValue();
                    int mapId = op.mapId;
                    if (mapId == map.getId()) {
                        @SuppressWarnings("unchecked")
                        K key = (K) op.key;
                        if (get(key) == null) {
                            // count each key only once (there might be multiple
                            // changes for the same key)
                            if (temp.putIfAbsent(key, ObjectDataType.NoneType.INSTANCE) == null) {
                                size--;
                            }
                        }
                    }
                }
            } finally {
                transaction.store.store.removeMap(temp);
            }
            return size;
        }

        /**
         * Remove an entry.
         * <p>
         * If the row is locked, this method will retry until the row could be
         * updated or until a lock timeout.
         *
         * @param key the key
         * @throws IllegalStateException if a lock timeout occurs
         */
        public V remove(K key) {
            return set(key, null);
        }

        /**
         * Update the value for the given key.
         * <p>
         * If the row is locked, this method will retry until the row could be
         * updated or until a lock timeout.
         *
         * @param key the key
         * @param value the new value (not null)
         * @return the old value
         * @throws IllegalStateException if a lock timeout occurs
         */
        public V put(K key, V value) {
            DataUtils.checkArgument(value != null, "The value may not be null");
            return set(key, value);
        }

        /**
         * Update the value for the given key, without adding an undo log entry.
         *
         * @param key the key
         * @param value the value
         * @return the old value
         */
        @SuppressWarnings("unchecked")
        public V putCommitted(K key, V value) {
            DataUtils.checkArgument(value != null, "The value may not be null");
            VersionedValue newValue = new VersionedValue(value);
            VersionedValue oldValue = map.put(key, newValue);
            return (V) (oldValue == null ? null : oldValue.value);
        }

        private V set(K key, V value) {
            TxDecisionMaker decisionMaker = new TxDecisionMaker(map.getId(), key, transaction);
            Transaction blockingTransaction;
            do {
                long txState = transaction.statusAndLogId.get();
                transaction.checkNotClosed(Transaction.getStatus(txState));
                transaction.blockingTransactionId = 0;

                long operationId = getOperationId(transaction.transactionId, Transaction.getTxLogId(txState));
                VersionedValue newValue = new VersionedValue(operationId, value);
                VersionedValue result = map.put(key, newValue, decisionMaker);
                assert decisionMaker.decision != null;
                if (decisionMaker.decision != MVMap.Decision.ABORT) {
                    //noinspection unchecked
                    return result == null ? null : (V) result.value;
                }
                blockingTransaction = decisionMaker.blockingTransaction;
                assert blockingTransaction != null : "Tx " + decisionMaker.blockingId +
                        " is missing, open: " + transaction.store.openTransactions.get().get(decisionMaker.blockingId) +
                        ", committing: " + transaction.store.committingTransactions.get().get(decisionMaker.blockingId);
                decisionMaker.reset();
            } while (transaction.waitFor(blockingTransaction));
            throw DataUtils.newIllegalStateException(DataUtils.ERROR_TRANSACTION_LOCKED,
                    "Map entry <{0}> with key <{1}> is locked", map.getName(), key);
        }

        /**
         * Try to remove the value for the given key.
         * <p>
         * This will fail if the row is locked by another transaction (that
         * means, if another open transaction changed the row).
         *
         * @param key the key
         * @return whether the entry could be removed
         */
        public boolean tryRemove(K key) {
            return trySet(key, null, false);
        }

        /**
         * Try to update the value for the given key.
         * <p>
         * This will fail if the row is locked by another transaction (that
         * means, if another open transaction changed the row).
         *
         * @param key the key
         * @param value the new value
         * @return whether the entry could be updated
         */
        public boolean tryPut(K key, V value) {
            DataUtils.checkArgument(value != null, "The value may not be null");
            return trySet(key, value, false);
        }

        /**
         * Try to set or remove the value. When updating only unchanged entries,
         * then the value is only changed if it was not changed after opening
         * the map.
         *
         * @param key the key
         * @param value the new value (null to remove the value)
         * @param onlyIfUnchanged only set the value if it was not changed (by
         *            this or another transaction) since the map was opened
         * @return true if the value was set, false if there was a concurrent
         *         update
         */
        public boolean trySet(K key, V value, boolean onlyIfUnchanged) {

            if (onlyIfUnchanged) {
                BitSet committingTransactions;
                MVMap.RootReference mapRootReference;
                MVMap.RootReference undoLogRootReference;
                do {
                    committingTransactions = transaction.store.committingTransactions.get();
                    mapRootReference = map.getRoot();
                    undoLogRootReference = transaction.store.undoLog.getRoot();
                } while(mapRootReference != map.getRoot() || committingTransactions != transaction.store.committingTransactions.get());

                VersionedValue current = (VersionedValue) Page.get(mapRootReference.root, key);
                VersionedValue old = getValue(key, readLogId, undoLogRootReference.root, committingTransactions, current);
                if (!map.areValuesEqual(old, current)) {
                    long tx = getTransactionId(current.operationId);
                    if (tx == transaction.transactionId) {
                        if (value == null) {
                            // ignore removing an entry
                            // if it was added or changed
                            // in the same statement
                            return true;
                        } else if (current.value == null) {
                            // add an entry that was removed
                            // in the same statement
                        } else {
                            return false;
                        }
                    } else {
                        return false;
                    }
                }
            }
            try {
                set(key, value);
                return true;
            } catch (IllegalStateException e) {
                return false;
            }
        }

        /**
         * Get the value for the given key at the time when this map was opened.
         *
         * @param key the key
         * @return the value or null
         */
        public V get(K key) {
            return get(key, readLogId);
        }

        /**
         * Whether the map contains the key.
         *
         * @param key the key
         * @return true if the map contains an entry for this key
         */
        public boolean containsKey(K key) {
            return get(key) != null;
        }

        /**
         * Get the value for the given key.
         *
         * @param key the key
         * @param maxLogId the maximum log id
         * @return the value or null
         */
        @SuppressWarnings("unchecked")
        public V get(K key, long maxLogId) {
            VersionedValue data = getValue(key, maxLogId);
            return data == null ? null : (V) data.value;
        }

        /**
         * Whether the entry for this key was added or removed from this
         * session.
         *
         * @param key the key
         * @return true if yes
         */
        public boolean isSameTransaction(K key) {
            VersionedValue data = map.get(key);
            if (data == null) {
                // doesn't exist or deleted by a committed transaction
                return false;
            }
            int tx = getTransactionId(data.operationId);
            return tx == transaction.transactionId;
        }

        private VersionedValue getValue(K key, long maxLog) {
            BitSet committingTransactions;
            MVMap.RootReference mapRootReference;
            MVMap.RootReference undoLogRootReference;
            do {
                committingTransactions = transaction.store.committingTransactions.get();
                mapRootReference = map.getRoot();
                undoLogRootReference = transaction.store.undoLog.getRoot();
            } while(mapRootReference != map.getRoot() || committingTransactions != transaction.store.committingTransactions.get());

            VersionedValue data = (VersionedValue) Page.get(mapRootReference.root, key);
            return getValue(key, maxLog, undoLogRootReference.root, committingTransactions, data);
        }

        /**
         * Get the versioned value for the given key.
         *
         * @param key the key
         * @param maxLog the maximum log id of the entry
         * @param data the value stored in the main map
         * @return the value
         */
        private VersionedValue getValue(K key, long maxLog, VersionedValue data) {
            return getValue(key, maxLog, transaction.store.undoLog.getRootPage(),
                            transaction.store.committingTransactions.get(), data);
        }

        private VersionedValue getValue(K key, long maxLog, Page undoLogRootPage,
                                        BitSet committingTransactions, VersionedValue data) {
            boolean first = true;
            while (true) {
                if (data == null) {
                    // doesn't exist or deleted by a committed transaction
                    return null;
                }
                long id = data.operationId;
                if (id == 0) {
                    // it is committed
                    return data;
                }
                int tx = getTransactionId(id);
                if (tx == transaction.transactionId) {
                    // added by this transaction
                    if (getLogId(id) < maxLog) {
                        return data;
                    }
                } else if(first && committingTransactions.get(tx)) {
                    return data;
                }
                first = false;
                // get the value before the uncommitted transaction
                Record d = (Record) Page.get(undoLogRootPage, id);
                assert d != null : getTransactionId(id)+"/"+getLogId(id);
                assert d.mapId == map.getId() : d.mapId + " != " + map.getId();
                assert MVMap.areValuesEqual(map.getKeyType(), d.key, key) : d.key + " <> " + key;
                data = d.oldValue;
            }
        }

        /**
         * Check whether this map is closed.
         *
         * @return true if closed
         */
        public boolean isClosed() {
            return map.isClosed();
        }

        /**
         * Clear the map.
         */
        public void clear() {
            // TODO truncate transactionally?
            map.clear();
        }

        /**
         * Get the first key.
         *
         * @return the first key, or null if empty
         */
        public K firstKey() {
            Iterator<K> it = keyIterator(null);
            return it.hasNext() ? it.next() : null;
        }

        /**
         * Get the last key.
         *
         * @return the last key, or null if empty
         */
        public K lastKey() {
            K k = map.lastKey();
            while (true) {
                if (k == null) {
                    return null;
                }
                if (get(k) != null) {
                    return k;
                }
                k = map.lowerKey(k);
            }
        }

        /**
         * Get the smallest key that is larger than the given key, or null if no
         * such key exists.
         *
         * @param key the key (may not be null)
         * @return the result
         */
        public K higherKey(K key) {
            while (true) {
                K k = map.higherKey(key);
                if (k == null || get(k) != null) {
                    return k;
                }
                key = k;
            }
        }

        /**
         * Get one of the previous or next keys. There might be no value
         * available for the returned key.
         *
         * @param key the key (may not be null)
         * @param offset how many keys to skip (-1 for previous, 1 for next)
         * @return the key
         */
        public K relativeKey(K key, long offset) {
            K k = offset > 0 ? map.ceilingKey(key) : map.floorKey(key);
            if (k == null) {
                return null;
            }
            long index = map.getKeyIndex(k);
            return map.getKey(index + offset);
        }

        /**
         * Get the largest key that is smaller than the given key, or null if no
         * such key exists.
         *
         * @param key the key (may not be null)
         * @return the result
         */
        public K lowerKey(K key) {
            while (true) {
                K k = map.lowerKey(key);
                if (k == null || get(k) != null) {
                    return k;
                }
                key = k;
            }
        }

        /**
         * Iterate over keys.
         *
         * @param from the first key to return
         * @return the iterator
         */
        public Iterator<K> keyIterator(K from) {
            return keyIterator(from, false);
        }

        /**
         * Iterate over keys.
         *
         * @param from the first key to return
         * @param includeUncommitted whether uncommitted entries should be
         *            included
         * @return the iterator
         */
        public Iterator<K> keyIterator(K from, boolean includeUncommitted) {
            return new KeyIterator<K>(this, from, includeUncommitted);
        }

        /**
         * Iterate over entries.
         *
         * @param from the first key to return
         * @return the iterator
         */
        public Iterator<Entry<K, V>> entryIterator(final K from) {
            return new EntryIterator<K,V>(this, from);
        }

        /**
         * Iterate over keys.
         *
         * @param iterator the iterator to wrap
         * @param includeUncommitted whether uncommitted entries should be
         *            included
         * @return the iterator
         */
        public Iterator<K> wrapIterator(final Iterator<K> iterator,
                final boolean includeUncommitted) {
            // TODO duplicate code for wrapIterator and entryIterator
            return new Iterator<K>() {
                private K current;

                private void fetchNext() {
                    while (iterator.hasNext()) {
                        current = iterator.next();
                        if (includeUncommitted) {
                            return;
                        }
                        if (containsKey(current)) {
                            return;
                        }
                    }
                    current = null;
                }

                @Override
                public boolean hasNext() {
                    if(current == null) {
                        fetchNext();
                    }
                    return current != null;
                }

                @Override
                public K next() {
                    if(!hasNext()) {
                        return null;
                    }
                    K result = current;
                    current = null;
                    return result;
                }


                @Override
                public void remove() {
                    throw DataUtils.newUnsupportedOperationException(
                            "Removing is not supported");
                }
            };
        }

        public Transaction getTransaction() {
            return transaction;
        }

        public String getName() {
            return map.getName();
        }

        public DataType getKeyType() {
            return map.getKeyType();
        }

        public static final class TxDecisionMaker extends MVMap.DecisionMaker<VersionedValue> {
            private final int            mapId;
            private final Object         key;
            public  final Transaction    transaction;
            private       int            blockingId;
            private       Transaction    blockingTransaction;
            private       MVMap.Decision decision;

            private TxDecisionMaker(int mapId, Object key, Transaction transaction) {
                this.mapId = mapId;
                this.key = key;
                this.transaction = transaction;
            }

            @Override
            public MVMap.Decision decide(VersionedValue existingValue, VersionedValue providedValue) {
                assert decision == null;
                assert providedValue != null;
                assert getTransactionId(providedValue.operationId) == transaction.transactionId;
                assert getLogId(providedValue.operationId) == transaction.getLogId();
                long id;
                // if map does not have that entry yet
                if(existingValue == null ||
                        // or entry is a committed one
                        (id = existingValue.operationId) == 0 ||
                        // or it came from the same transaction
                        (blockingId = getTransactionId(id)) == transaction.transactionId) {
                    decision = MVMap.Decision.PUT;
                    transaction.log(mapId, key, existingValue);
                } else if(transaction.store.committingTransactions.get().get(blockingId)) {
                    // entry belongs to a committing transaction and therefore will be committed soon
                    // we assume that we are looking at final value for this transaction
                    // and if it's not the case, then it will fail later
                    // because a tree root has definitely changed
                    decision = MVMap.Decision.PUT;
                    transaction.log(mapId, key, new VersionedValue(existingValue.value));
                } else {
                    // this entry comes from a different transaction and it's not committed yet
                    blockingTransaction = transaction.store.getTransaction(blockingId);
                    decision = MVMap.Decision.ABORT;
                }
                return decision;
            }

            @Override
            public void reset() {
                if(decision != MVMap.Decision.ABORT) {
                    // map was updated after our decision has been made
                    transaction.logUndo();
                }
                blockingTransaction = null;
                decision = null;
            }

            @Override
            public String toString() {
                return "tx "+transaction.transactionId;
            }
        }
    }

    /**
     * A versioned value (possibly null). It contains a pointer to the old
     * value, and the value itself.
     */
    public static final class VersionedValue {

        /**
         * The operation id.
         */
        public final long operationId;

        /**
         * The value.
         */
        public final Object value;

        public VersionedValue(Object value) {
            this(0, value);
        }

        public VersionedValue(long operationId, Object value) {
            this.operationId = operationId;
            this.value = value;
        }

        @Override
        public String toString() {
            return value + (operationId == 0 ? "" : (
                    " " +
                    getTransactionId(operationId) + "/" +
                    getLogId(operationId)));
        }

    }

    /**
     * The value type for a versioned value.
     */
    public static final class VersionedValueType implements DataType, StatefulDataType {

        private final DataType valueType;

        VersionedValueType(DataType valueType) {
            this.valueType = valueType;
        }

        @Override
        public int getMemory(Object obj) {
            if(obj == null) return 0;
            VersionedValue v = (VersionedValue) obj;
            Object value = v.value;
            return Constants.MEMORY_OBJECT + 8 + Constants.MEMORY_POINTER +
                    (value == null ? 0 : valueType.getMemory(value));
        }

        @Override
        public int compare(Object aObj, Object bObj) {
            if (aObj == bObj) {
                return 0;
            }
            VersionedValue a = (VersionedValue) aObj;
            VersionedValue b = (VersionedValue) bObj;
            int comp = Long.compare(a.operationId, b.operationId);
            if (comp == 0) {
                comp = valueType.compare(a.value, b.value);
            }
            return comp;
        }

        @Override
        public void read(ByteBuffer buff, Object[] obj, int len, boolean key) {
            if (buff.get() == 0) {
                // fast path (no op ids or null entries)
                for (int i = 0; i < len; i++) {
                    obj[i] = new VersionedValue(valueType.read(buff));
                }
            } else {
                // slow path (some entries may be null)
                for (int i = 0; i < len; i++) {
                    obj[i] = read(buff);
                }
            }
        }

        @Override
        public Object read(ByteBuffer buff) {
            long operationId = DataUtils.readVarLong(buff);
            Object value = buff.get() == 1 ? valueType.read(buff) : null;
            return new VersionedValue(operationId, value);
        }

        @Override
        public void write(WriteBuffer buff, Object[] obj, int len, boolean key) {
            boolean fastPath = true;
            for (int i = 0; i < len; i++) {
                VersionedValue v = (VersionedValue) obj[i];
                if (v.operationId != 0 || v.value == null) {
                    fastPath = false;
                }
            }
            if (fastPath) {
                buff.put((byte) 0);
                for (int i = 0; i < len; i++) {
                    VersionedValue v = (VersionedValue) obj[i];
                    valueType.write(buff, v.value);
                }
            } else {
                // slow path:
                // store op ids, and some entries may be null
                buff.put((byte) 1);
                for (int i = 0; i < len; i++) {
                    write(buff, obj[i]);
                }
            }
        }

        @Override
        public void write(WriteBuffer buff, Object obj) {
            VersionedValue v = (VersionedValue) obj;
            buff.putVarLong(v.operationId);
            if (v.value == null) {
                buff.put((byte) 0);
            } else {
                buff.put((byte) 1);
                valueType.write(buff, v.value);
            }
        }

        @Override
        public int hashCode() {
            return getClass().hashCode() + valueType.hashCode();
        }

        @Override
        public void save(WriteBuffer buff, DataType metaDataType, Database database) {
            metaDataType.write(buff, valueType);
        }

        @Override
        public void load(ByteBuffer buff, DataType metaDataType, Database database) {
            throw DataUtils.newUnsupportedOperationException("load()");
        }

        @Override
        public Factory getFactory() {
            return FACTORY;
        }

        private static final Factory FACTORY = new Factory();

        public static final class Factory implements StatefulDataType.Factory {

            @Override
            public DataType create(ByteBuffer buff, DataType metaDataType, Database database) {
                DataType valueType = (DataType) metaDataType.read(buff);
                return new VersionedValueType(valueType);
            }
        }
    }

    private static final class Record {
        private final int mapId;
        private final Object key;
        private final VersionedValue oldValue;

        public Record(int mapId, Object key, VersionedValue oldValue) {
            this.mapId = mapId;
            this.key = key;
            this.oldValue = oldValue;
        }

        @Override
        public String toString() {
            return "mapId=" + mapId + ", key=" + key + ", value=" + oldValue;
        }
    }

    /**
     * A data type for undo log values
     */
    public static final class RecordType implements DataType {
        private final TransactionStore transactionStore;

        public RecordType(TransactionStore transactionStore) {
            this.transactionStore = transactionStore;
        }

        @Override
        public int getMemory(Object obj) {
            Record record = (Record) obj;
            MVMap<Object, VersionedValue> map = transactionStore.getMap(record.mapId);
            return Constants.MEMORY_OBJECT + 4 + 3 * Constants.MEMORY_POINTER +
                    map.getKeyType().getMemory(record.key) +
                    map.getValueType().getMemory(record.oldValue);
        }

        @Override
        public int compare(Object aObj, Object bObj) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void read(ByteBuffer buff, Object[] obj,
                int len, boolean key) {
            for (int i = 0; i < len; i++) {
                obj[i] = read(buff);
            }
        }

        @Override
        public void write(WriteBuffer buff, Object[] obj,
                int len, boolean key) {
            for (int i = 0; i < len; i++) {
                write(buff, obj[i]);
            }
        }

        @Override
        public void write(WriteBuffer buff, Object obj) {
            Record record = (Record) obj;
            MVMap<Object, VersionedValue> map = transactionStore.getMap(record.mapId);
            buff.putVarInt(record.mapId);
            map.getKeyType().write(buff, record.key);
            VersionedValue oldValue = record.oldValue;
            if(oldValue == null) {
                buff.put((byte) 0);
            } else {
                buff.put((byte) 1);
                map.getValueType().write(buff, oldValue);
            }
        }

        @Override
        public Object read(ByteBuffer buff) {
            int mapId = DataUtils.readVarInt(buff);
            MVMap<Object, VersionedValue> map = transactionStore.getMap(mapId);
            Object key = map.getKeyType().read(buff);
            VersionedValue oldValue = null;
            if (buff.get() == 1) {
                oldValue = (VersionedValue)map.getValueType().read(buff);
            }
            return new Record(mapId, key, oldValue);
        }

    }

    private static final class CommitDecisionMaker extends MVMap.DecisionMaker<Record> {
        private final TransactionStore store;
        private final FinalCommitDecisionMaker finalCommitDecisionMaker;
        private       MVMap.Decision decision;

        private CommitDecisionMaker(TransactionStore store) {
            this.store = store;
            finalCommitDecisionMaker = new FinalCommitDecisionMaker();
        }

        private void setOperationId(long undoKey) {
            finalCommitDecisionMaker.setOperationId(undoKey);
            reset();
        }

        @Override
        public MVMap.Decision decide(Record existingValue, Record providedValue) {
            assert decision == null;
            if (existingValue == null) {
                decision = MVMap.Decision.ABORT;
            } else {
                int mapId = existingValue.mapId;
                MVMap<Object, VersionedValue> map = store.openMap(mapId);
                if (map != null && !map.isClosed()) { // could be null if map was later removed
                    Object key = existingValue.key;
                    VersionedValue prev = existingValue.oldValue;
                    assert prev == null || prev.operationId == 0 || getTransactionId(prev.operationId) == getTransactionId(finalCommitDecisionMaker.undoKey) ;
                    map.operate(key, null, finalCommitDecisionMaker);
                }
                decision = MVMap.Decision.REMOVE;
            }
            return decision;
        }

        @Override
        public void reset() {
            decision = null;
            finalCommitDecisionMaker.reset();
        }

        @Override
        public String toString() {
            return "commit " + getTransactionId(finalCommitDecisionMaker.undoKey);
        }
    }

    private static final class FinalCommitDecisionMaker extends MVMap.DecisionMaker<VersionedValue> {
        private long undoKey;
        private MVMap.Decision decision;
        private VersionedValue value;

        private FinalCommitDecisionMaker() {}

        private void setOperationId(long undoKey) {
            this.undoKey = undoKey;
        }

        @Override
        public MVMap.Decision decide(VersionedValue existingValue, VersionedValue providedValue) {
            assert decision == null;
            if (existingValue == null) {
                decision = MVMap.Decision.ABORT;
            } else {
                if (existingValue.operationId == undoKey) {
                    if (existingValue.value == null) {
                        // remove the value only if it comes from the same transaction
                        decision = MVMap.Decision.REMOVE;
                    } else {
                        // put the value only if it comes from the same transaction
                        decision = MVMap.Decision.PUT;
                        value = new VersionedValue(existingValue.value);
                    }
                } else {
                    decision = MVMap.Decision.ABORT;
                }
            }
            return decision;
        }

        @Override
        public VersionedValue selectValue(VersionedValue existingValue, VersionedValue providedValue) {
            assert decision == MVMap.Decision.PUT;
            assert value != null;
            assert value.operationId == 0;
            assert existingValue != null;
            assert value.value == existingValue.value;
            return value;
        }

        @Override
        public void reset() {
            value = null;
            decision = null;
        }

        @Override
        public String toString() {
            return "final_commit " + getTransactionId(undoKey);
        }
    }

    public interface RollbackListener {
        RollbackListener NONE = new RollbackListener() {
            @Override
            public void onRollback(MVMap<Object, VersionedValue> map, Object key, VersionedValue existingValue, VersionedValue restoredValue) {

            }
        };
        void onRollback(MVMap<Object,VersionedValue> map, Object key,
                        VersionedValue existingValue, VersionedValue restoredValue);
    }

    private static final class RollbackDecisionMaker extends MVMap.DecisionMaker<Record> {
        private final TransactionStore store;
        private final long             transactionId;
        private final long             toLogId;
        private final RollbackListener listener;
        private       MVMap.Decision   decision;

        private RollbackDecisionMaker(TransactionStore store, long transactionId, long toLogId, RollbackListener listener) {
            this.store = store;
            this.transactionId = transactionId;
            this.toLogId = toLogId;
            this.listener = listener;
        }

        @Override
        public MVMap.Decision decide(Record existingValue, Record providedValue) {
            assert decision == null;
            assert existingValue != null;
//            if (existingValue != null) {
                VersionedValue valueToRestore = existingValue.oldValue;
                long operationId;
                if(valueToRestore == null ||
                        (operationId = valueToRestore.operationId) == 0 ||
                        getTransactionId(operationId) == transactionId
                                && getLogId(operationId) < toLogId) {
                    int mapId = existingValue.mapId;
                    MVMap<Object, VersionedValue> map = store.openMap(mapId);
                    if(map != null && !map.isClosed()) {
                        Object key = existingValue.key;
                        VersionedValue previousValue = map.operate(key, valueToRestore, MVMap.DecisionMaker.DEFAULT);
                        listener.onRollback(map, key, previousValue, valueToRestore);
                    }
                }
                decision = MVMap.Decision.REMOVE;
//            } else {
//                decision = MVMap.Decision.ABORT;
//            }
            return decision;
        }

        @Override
        public void reset() {
            decision = null;
        }

        @Override
        public String toString() {
            return "rollback ";
        }
    }

    private static final class KeyIterator<K> extends TMIterator<K,K> {
        private final boolean includeUncommitted;

        public KeyIterator(TransactionMap<K, ?> transactionMap, K from, boolean includeUncommitted) {
            super(transactionMap, from);
            this.includeUncommitted = includeUncommitted;
        }

        @Override
        protected void fetchNext() {
            while (cursor.hasNext()) {
                K key = cursor.next();
                current = key;
                VersionedValue data = cursor.getValue();
                if (!includeUncommitted) {
                    data = getCommittedValue(key, data);
                }
                if(data != null && data.value != null) {
                    return;
                }
            }
            current = null;
        }
    }

    private static final class EntryIterator<K,V> extends TMIterator<K,Entry<K,V>> {

        public EntryIterator(TransactionMap<K, ?> transactionMap, K from) {
            super(transactionMap, from);
        }

        @Override
        protected void fetchNext() {
            while (cursor.hasNext()) {
                K key = cursor.next();
                VersionedValue data = cursor.getValue();
                data = getCommittedValue(key, data);
                if (data != null && data.value != null) {
                    @SuppressWarnings("unchecked")
                    V value = (V) data.value;
                    current = new DataUtils.MapEntry<K, V>(key, value);
                    return;
                }
            }
            current = null;
        }
    }

    private abstract static class TMIterator<K,X> implements Iterator<X> {
        private   final TransactionMap<K,?>       transactionMap;
        private   final BitSet                    committingTransactions;
        private   final Page                      undoLogRootPage;
        protected final Cursor<K, VersionedValue> cursor;
        protected       X                         current;

        protected TMIterator(TransactionMap<K,?> transactionMap, K from)
        {
            this.transactionMap = transactionMap;
            TransactionStore store = transactionMap.transaction.store;
            MVMap<K, VersionedValue> map = transactionMap.map;
            BitSet committingTransactions;
            MVMap.RootReference mapRootReference;
            MVMap.RootReference undoLogRootReference;
            do {
                committingTransactions = store.committingTransactions.get();
                mapRootReference = map.getRoot();
                undoLogRootReference = store.undoLog.getRoot();
            } while(mapRootReference != map.getRoot() || committingTransactions != store.committingTransactions.get());
            this.committingTransactions = committingTransactions;
            this.undoLogRootPage = undoLogRootReference.root;
            this.cursor = new Cursor<K, VersionedValue>(map, mapRootReference.root, from, true);
        }

        protected abstract void fetchNext();

        @Override
        public final boolean hasNext() {
            if(current == null) {
                fetchNext();
            }
            return current != null;
        }

        @Override
        public final X next() {
            if(!hasNext()) {
                return null;
            }
            X result = current;
            current = null;
            return result;
        }

        @Override
        public final void remove() {
            throw DataUtils.newUnsupportedOperationException(
                    "Removing is not supported");
        }

        protected final VersionedValue getCommittedValue(K key, VersionedValue data) {
            return transactionMap.getValue(key, transactionMap.readLogId, undoLogRootPage,
                                           committingTransactions, data);
        }
    }
}
