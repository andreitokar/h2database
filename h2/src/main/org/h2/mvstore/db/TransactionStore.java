/*
 * Copyright 2004-2014 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.mvstore.db;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.atomic.AtomicReferenceArray;

import org.h2.engine.Constants;
import org.h2.engine.Database;
import org.h2.mvstore.BasicDataType;
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

/**
 * A store that supports concurrent MVCC read-committed transactions.
 */
public final class TransactionStore {

    private static final int MAX_OPEN_TRANSACTIONS = 0x100;
    private static final String TYPE_REGISTRY_NAME = "_";
    private static final String UNDO_LOG_NAME_PEFIX = "undoLog-";

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
//    private final MVMap<Long, Record> undoLog;

    private final MVMap<String, DataType> typeRegistry;

    private final DataType dataType;


    private boolean init;

    private int maxTransactionId = MAX_OPEN_TRANSACTIONS;
    private final AtomicReferenceArray<Transaction> transactions = new AtomicReferenceArray<>(MAX_OPEN_TRANSACTIONS);
    private final MVMap<Long,Record> undoLogs[] = (MVMap<Long,Record>[])new MVMap[MAX_OPEN_TRANSACTIONS];

    private final AtomicReference<BitSet> openTransactions = new AtomicReference<>(new BitSet());
    private final AtomicReference<BitSet> committingTransactions = new AtomicReference<>(new BitSet());


    /**
     * The next id of a temporary map.
     */
    private int nextTempMapId;

    private final MVMap.Builder<Long, Record> builder;

    /**
     * Create a new transaction store.
     *
     * @param store the store
     */
    public TransactionStore(MVStore store) {
        this(store, new DBMetaType(null, store.backgroundExceptionHandler), new ObjectDataType(), 0);
    }

    /**
     * Create a new transaction store.
     * @param store the store
     * @param dataType the data type for map keys and values
     * @param timeoutMillis lock aquisition timeout in milliseconds, 0 means no wait
     */
    public TransactionStore(MVStore store, DataType metaDataType, DataType dataType, long timeoutMillis) {
        this.store = store;
        this.dataType = dataType;
        this.timeoutMillis = timeoutMillis;
        MVMap.Builder<String, DataType> typeRegistryBuilder =
                                    new MVMap.Builder<String, DataType>()
                                                .keyType(StringDataType.INSTANCE)
                                                .valueType(metaDataType);
        typeRegistry = store.openMap(TYPE_REGISTRY_NAME, typeRegistryBuilder);
        preparedTransactions = store.openMap("openTransactions",
                                             new MVMap.Builder<Integer, Object[]>());
        RecordType undoLogValueType = new RecordType(this);
        builder = new MVMap.Builder<Long, Record>()
                        .singleWriter()
                        .keyType(LongDataType.INSTANCE)
                        .valueType(undoLogValueType);
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
            for (String mapName : store.getMapNames()) {
                if (mapName.startsWith(UNDO_LOG_NAME_PEFIX)) {
                    int transactionId = Integer.parseInt(mapName.substring(UNDO_LOG_NAME_PEFIX.length()));
                    if (store.hasData(mapName)) {
                        MVMap<Long,Record> undoLog = store.openMap(mapName, builder);
                        undoLogs[transactionId] = undoLog;
                        Object[] data = preparedTransactions.get(transactionId);
                        int status;
                        String name;
                        if (data == null) {
                            if (getLogId(undoLog.firstKey()) == 0) {
                                status = Transaction.STATUS_OPEN;
                            } else {
                                status = Transaction.STATUS_COMMITTING;
                            }
                            name = null;
                        } else {
                            status = (Integer) data[0];
                            name = (String) data[1];
                        }
                        long logId = getLogId(undoLog.lastKey()) + 1;
                        boolean success;
                        do {
                            BitSet original = openTransactions.get();
                            BitSet clone = (BitSet) original.clone();
                            assert !clone.get(transactionId);
                            clone.set(transactionId);
                            success = openTransactions.compareAndSet(original, clone);
                        } while (!success);
                        registerTransaction(transactionId, status, name, logId, timeoutMillis, listener);
                    }
                }
            }
//            for (int i = 0; i < MAX_OPEN_TRANSACTIONS; i++) {
//                String undoName = "undoLog-" + i;
//                if (store.hasMap(undoName)) {
//                    if (store.hasData(undoName)) {
//                        undoLogs[i] = store.openMap(undoName, builder);
//                    }
//                }
//            }

/*
            for (MVMap<Long, Record> undoLog : undoLogs) {
                if (undoLog != null) {
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
                        } while (!success);
                        registerTransaction(transactionId, status, name, logId, timeoutMillis, listener);

                        key = undoLog.ceilingKey(getOperationId(transactionId + 1, 0));
                    }
                }
            }
*/
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
        DataUtils.checkArgument(max <= MAX_OPEN_TRANSACTIONS,
                "Concurrent transactions limit is too hight: {0}", max);
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

        if(timeoutMillis <= 0) {
            timeoutMillis = this.timeoutMillis;
        }
        Transaction transaction = registerTransaction(transactionId, Transaction.STATUS_OPEN,
                                                        null, 0, timeoutMillis, listener);
        return transaction;
    }

    private Transaction registerTransaction(int transactionId, int status, String name,
                                     long logId, long timeoutMillis, RollbackListener listener) {
        Transaction transaction = new Transaction(this, transactionId, status, name, logId, timeoutMillis, listener);
        transactions.set(transactionId, transaction);
        if (undoLogs[transactionId] == null) {
            String undoName = UNDO_LOG_NAME_PEFIX + transactionId;
            undoLogs[transactionId] = store.openMap(undoName, builder);
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
            t.wasStored = true;
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
        //TODO: re-implement using "append" (no search, use extra capacity, appendLeaf())
        // since undoLog is Tx-specific now and therefore has a single writer.
        int transactionId = getTransactionId(undoKey);
//        return undoLogs[transactionId].putIfAbsent(undoKey, record) == null;
        undoLogs[transactionId].append(undoKey, record);
        return true;
    }

    /**
     * Remove a log entry.
     * @param undoKey transactionId/LogId
     *
     * @return true if success, false otherwise
     */
    boolean removeUndoLogRecord(Long undoKey) {
        int transactionId = getTransactionId(undoKey);
//        return undoLogs[transactionId].remove(undoKey) != null;
        undoLogs[transactionId].trimLast();
        return true;
    }

    /**
     * Remove the given map.
     *
     * @param <K> the key type
     * @param <V> the value type
     * @param map the map
     */
    <K, V> void removeMap(TransactionMap<K, V> map) {
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
            final int transactionId = t.transactionId;
            do {
                BitSet original = committingTransactions.get();
                assert !original.get(transactionId) : "Double commit";
                BitSet clone = (BitSet) original.clone();
                clone.set(transactionId);
                success = committingTransactions.compareAndSet(original, clone);
            } while(!success);
            try {
                long state = t.setStatus(Transaction.STATUS_COMMITTED);
                final long maxLogId = Transaction.getTxLogId(state);
                final TransactionStore transactionStore = this;
                MVMap<Long, Record> undoLog = undoLogs[transactionId];
                MVMap.RootReference rootReference = undoLog.flushAppendBuffer();
                CommitProcessor committProcessor = new CommitProcessor(transactionStore, transactionId, maxLogId);
                MVMap.process(rootReference.root, null, committProcessor);
                assert committProcessor.counter == maxLogId : committProcessor.counter + " != " + maxLogId;
                undoLog.clear();
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

    private static final class CommitProcessor implements MVMap.Processor<Long, Record> {
        private final FinalCommitDecisionMaker finalCommitDecisionMaker;
        private final TransactionStore transactionStore;
        private final int transactionId;
        private final long maxLogId;
        public int counter;

        private CommitProcessor(TransactionStore transactionStore, int transactionId, long maxLogId) {
            this.transactionStore = transactionStore;
            this.transactionId = transactionId;
            this.maxLogId = maxLogId;
            finalCommitDecisionMaker = new FinalCommitDecisionMaker();
        }

        @Override
        public boolean process(Long undoKey, Record existingValue) {
            assert getTransactionId(undoKey) == transactionId;
            assert getLogId(undoKey) <= maxLogId;
            if (getTransactionId(undoKey) != transactionId || getLogId(undoKey) > maxLogId) {
                return true;
            }
            ++counter;
            finalCommitDecisionMaker.reset();
            finalCommitDecisionMaker.setOperationId(undoKey);

            int mapId = existingValue.mapId;
            MVMap<Object, VersionedValue> map = transactionStore.openMap(mapId);
            if (map != null && !map.isClosed()) { // could be null if map was later removed
                Object key = existingValue.key;
                VersionedValue prev = existingValue.oldValue;
                assert prev == null || prev.operationId == 0 || getTransactionId(prev.operationId) == getTransactionId(finalCommitDecisionMaker.undoKey);
                // TODO: let DecisionMaker have access to the whole LeafNode, make coomit changes in bulk
                map.operate(key, null, finalCommitDecisionMaker);
            }

            return false;
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
            MVMap.Builder<Object, VersionedValue> txMapBuilder = new TxMapBuilder<>(typeRegistry, dataType);
            map = store.openMap(mapId, txMapBuilder);
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
     * End this transaction
     *
     * @param t the transaction
     */
    void endTransaction(Transaction t) {
        int txId = t.transactionId;

        t.closeIt();
        store.deregisterVersionUsage(t.txCounter);
        transactions.set(t.transactionId, null);

        boolean success;
        do {
            BitSet original = openTransactions.get();
            assert original.get(t.transactionId);
            BitSet clone = (BitSet) original.clone();
            clone.clear(t.transactionId);
            success = openTransactions.compareAndSet(original, clone);
        } while(!success);

        if (t.wasStored && !preparedTransactions.isClosed()) {
            preparedTransactions.remove(txId);
        }

        if (store.getAutoCommitDelay() == 0) {
            store.commit();
        } else {
            boolean empty = true;
            BitSet openTrans = openTransactions.get();
            for (int i = openTrans.nextSetBit(0); empty && i >= 0; i = openTrans.nextSetBit(i+1)) {
                MVMap<Long, Record> undoLog = undoLogs[i];
                if (undoLog != null) {
                    empty = undoLog.isEmpty();
                }
            }
            if (empty) {
                // to avoid having to store the transaction log,
                // if there is no open transaction,
                // and if there have been many changes, store them now
                int unsaved = store.getUnsavedMemory();
                int max = store.getAutoCommitMemory();
                // save at 3/4 capacity
                if (unsaved * 4 > max * 3) {
                    store.commit();
                }
            }
        }
    }

    private Transaction getTransaction(int transactionId) {
        return transactions.get(transactionId);
    }

    /**
     * Rollback to an old savepoint.
     *
     * @param t the transaction
     * @param toLogId the log id to roll back to
     */
    void rollbackTo(final Transaction t, long fromLogId, long toLogId) {
        int transactionId = t.getId();
        MVMap<Long, Record> undoLog = undoLogs[transactionId];
        undoLog.flushAppendBuffer();
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

        final MVMap<Long, Record> undoLog = undoLogs[t.getId()];
        undoLog.flushAppendBuffer();
        return new Iterator<Change>() {

            private long logId = maxLogId - 1;
            private Change current;

            private void fetchNext() {
                int transactionId = t.getId();
                while (logId >= toLogId) {
                    Long undoKey = getOperationId(transactionId, logId);
                    Record op = undoLog.get(undoKey);
                    logId--;
                    if (op == null) {
                        // partially rolled back: load previous
                        undoKey = undoLog.floorKey(undoKey);
                        if (undoKey == null || getTransactionId(undoKey) != transactionId) {
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
            return Integer.toHexString(Objects.hashCode(dataType));
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
            return new TMVMap<>(config);
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

        /**
         * The status of a transaction that has been logically committed or rather
         * marked as committed, because it might be still listed among prepared,
         * if it was prepared for commit, undo log entries might still exists for it
         * and not all of it's changes within map's are re-written as committed yet.
         * Nevertheless, those changes should be already viewed by other
         * transactions as committed.
         * This transaction's id can not be re-used until all the above is completed
         * and transaction is closed.
         */
        public static final int STATUS_COMMITTED    = 4;

        /**
         * The status of a transaction that currently in a process of rolling back
         * to a savepoint.
         */
        public static final int STATUS_ROLLING_BACK = 5;

        /**
         * The status of a transaction that has been rolled back completely,
         * but undo operations are not finished yet.
         */
        public static final int STATUS_ROLLED_BACK  = 6;

        private static final String STATUS_NAMES[] = {
                "CLOSED", "OPEN", "PREPARED", "COMMITTING",
                "COMMITTED", "ROLLING_BACK", "ROLLED_BACK"
        };

        /**
         * The transaction store.
         */
        final TransactionStore store;

        private final RollbackListener listener;

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

        private MVStore.TxCounter txCounter;

        private boolean wasStored;
        private MVMap blockingMap;
        private Object blockingKey;

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

        private static int getStatus(long state) {
            return (int)(state >>> 40) & 15;
        }

        private static long getTxLogId(long state) {
            return state & ((1L << 40) - 1);
        }

        private static boolean hasRollback(long state) {
            return (state & (1L << 44)) != 0;
        }

        private static boolean hasChanges(long state) {
            return getTxLogId(state) != 0;
        }

        private static long composeState(int status, long logId, boolean hasRollback) {
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

        private long getLogId() {
            return getTxLogId(statusAndLogId.get());
        }

        public boolean hasChanges() {
            return hasChanges(statusAndLogId.get());
        }

        private int getOwnerId() {
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
            txCounter = store.store.registerVersionUsage();
        }

        public void markStatementEnd() {
            MVStore.TxCounter counter = txCounter;
            txCounter = null;
            if(counter != null) {
                store.store.deregisterVersionUsage(counter);
            }
        }

        /**
         * Add a log entry.
         *
         * @param mapId the map id
         * @param key the key
         * @param oldValue the old value
         */
        long log(int mapId, Object key, VersionedValue oldValue) {
            long currentState = statusAndLogId.getAndIncrement();
            checkNotClosed(Transaction.getStatus(currentState));
            long logId = getTxLogId(currentState);
            int currentStatus = getStatus(currentState);
            checkOpen(currentStatus);
            long undoKey = getOperationId(transactionId, logId);
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
            return undoKey;
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
                    store.commit(this);
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
            Throwable ex = null;
            try {
                store.rollbackTo(this, logId, savepointId);
            } catch (Throwable e) {
                ex = e;
            } finally {
                long expectedState = composeState(STATUS_ROLLING_BACK, logId, hasRollback(lastState));
                long newState = composeState(STATUS_OPEN, savepointId, true);
                if (!statusAndLogId.compareAndSet(expectedState, newState)) {
                    throw DataUtils.newIllegalStateException(
                            DataUtils.ERROR_TRANSACTION_ILLEGAL_STATE,
                            "Transaction {0} concurrently modified " +
                                    "while rollback to savepoint was in progress",
                            transactionId, ex);
                }
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
        private void checkNotClosed(int status) {
            if (status == STATUS_CLOSED) {
                throw DataUtils.newIllegalStateException(
                        DataUtils.ERROR_CLOSED, "Transaction {0} is closed", transactionId);
            }
        }

        private void checkNotClosed() {
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
//            this.readLogId = savepoint;
        }

        /**
         * Get a clone of this map for the given transaction.
         *
         * @param transaction the transaction
         * @return the map
         */
        public TransactionMap<K, V> getInstance(Transaction transaction) {
            return new TransactionMap<>(transaction, map);
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
            TransactionStore store = transaction.store;
            BitSet opentransactions;
            BitSet committingTransactions;
            MVMap.RootReference mapRootReference;
            MVMap.RootReference undoLogRootReferences[];
            long undoLogSize;
            do {
                opentransactions = store.openTransactions.get();
                committingTransactions = store.committingTransactions.get();
                mapRootReference = map.getRoot();
                undoLogRootReferences = new MVMap.RootReference[opentransactions.length()];
                undoLogSize = 0;
                for (int i = opentransactions.nextSetBit(0); i >= 0; i = opentransactions.nextSetBit(i+1)) {
                    MVMap<Long, Record> undoLog = store.undoLogs[i];
                    if (undoLog != null) {
                        MVMap.RootReference rootReference = undoLog.flushAppendBuffer();
//                        MVMap.RootReference rootReference = undoLog.getRoot();
                        undoLogRootReferences[i] = rootReference;
                        undoLogSize += rootReference.root.getTotalCount() + rootReference.appendCounter;
                    }
                }
            } while(mapRootReference != map.getRoot()
                    || committingTransactions != store.committingTransactions.get()
                    || opentransactions != store.openTransactions.get());

            long size = mapRootReference.root.getTotalCount();
            if (undoLogSize == 0) {
                return size;
            }
            AbstractMapSizeAdjuster adjuster;
            if (undoLogSize > size) {
                // the undo log is larger than the map -
                // count the entries of the map
                MapSizeAdjuster processor = new MapSizeAdjuster(committingTransactions, transaction.transactionId);
                MVMap.process(mapRootReference.root, null, processor);
                adjuster = processor;
            } else {
                // The undo log is smaller than the map -
                // scan the undo log, find appropriate map entries and decrement counter for each irrelevant entry.
                // Entry is irrelevant if it was newly added by the uncommitted transaction, other than the curent one.
                // Also irrelevalnt are entries with value of null, if they are modified (not created)
                // by the current transaction or some committed transaction, which is not closed yet.
                UndoLogMapSizeAdjuster processor = new UndoLogMapSizeAdjuster(committingTransactions, mapRootReference.root,
                                                                map.getId(), transaction.transactionId);
                for (MVMap.RootReference undoLogRootReference : undoLogRootReferences) {
                    if (undoLogRootReference != null) {
                        MVMap.process(undoLogRootReference.root, null, processor);
                    }
                }
                adjuster = processor;
            }
            size += adjuster.getAdjustment();
            return size;
        }

        private static class AbstractMapSizeAdjuster {
            private final BitSet committingTransactions;
            private final int    transactionId;
            private       long   adjustment;

            protected AbstractMapSizeAdjuster(BitSet committingTransactions, int transactionId) {
                this.committingTransactions = committingTransactions;
                this.transactionId = transactionId;
            }

            protected final void decrement() {
                --adjustment;
            }

            protected final boolean isVisible(int txId) {
                return committingTransactions.get(txId) || txId == transactionId;
            }

            protected final long getAdjustment() {
                return adjustment;
            }
        }

        private static final class MapSizeAdjuster extends AbstractMapSizeAdjuster
                                                   implements MVMap.Processor<Object,VersionedValue> {

            private MapSizeAdjuster(BitSet committingTransactions, int transactionId) {
                super(committingTransactions, transactionId);
            }

            @Override
            public boolean process(Object key, VersionedValue value) {
                assert value != null;
                long id = value.operationId;
                if (id != 0) {  // skip committed entries
                    Object v = isVisible(getTransactionId(id)) ? value.value : value.committedValue;
                    if (v == null) {
                        decrement();
                    }
                }
                return false;
            }
        }

        private static final class UndoLogMapSizeAdjuster extends AbstractMapSizeAdjuster
                                                          implements MVMap.Processor<Long,Record> {
            private final Page   root;
            private final int    mapId;

            private UndoLogMapSizeAdjuster(BitSet committingTransactions, Page root, int mapId, int transactionId) {
                super(committingTransactions, transactionId);
                this.root = root;
                this.mapId = mapId;
            }

            @Override
            public boolean process(Long undoKey, Record op) {
                if (op.mapId == mapId) {
                    assert undoKey != null;
                    int txId = getTransactionId(undoKey);
                    boolean isVisible = isVisible(txId);
                    if (isVisible ? op.oldValue != null && op.oldValue.operationId == 0 && op.oldValue.value != null
                                  : op.oldValue == null) {
                        VersionedValue currentValue = (VersionedValue) Page.get(root, op.key);
                        if (isVisible ? (currentValue == null || currentValue.value == null)
                                      : currentValue != null) {
                            decrement();
                        }
                    }
                }
                return false;
            }
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

        public MVMap.BufferingAgent<K,V> getBufferingAgent() {
            final MVMap.BufferingAgent<K, VersionedValue> agent = map.getBufferingAgent();
            return new MVMap.BufferingAgent<K, V>() {
                @Override
                public void put(K key, V value) {
                    agent.put(key, new VersionedValue(value));
                }

                @Override
                public void close() {
                    agent.close();
                }
            };
        }

        private V set(K key, V value) {
            TxDecisionMaker decisionMaker = new TxDecisionMaker(map.getId(), key, value, transaction);
            Transaction blockingTransaction;
            do {
                transaction.blockingTransactionId = 0;
                VersionedValue result = map.put(key, VersionedValue.DUMMY, decisionMaker);
                assert decisionMaker.decision != null;
                if (decisionMaker.decision != MVMap.Decision.ABORT) {
                    transaction.blockingMap = null;
                    transaction.blockingKey = null;
                    //noinspection unchecked
                    return result == null ? null : (V) result.value;
                }
                blockingTransaction = decisionMaker.blockingTransaction;
                assert blockingTransaction != null : "Tx " + decisionMaker.blockingId +
                        " is missing, open: " + transaction.store.openTransactions.get().get(decisionMaker.blockingId) +
                        ", committing: " + transaction.store.committingTransactions.get().get(decisionMaker.blockingId);
                decisionMaker.reset();
                transaction.blockingMap = map;
                transaction.blockingKey = key;
            } while (transaction.waitFor(blockingTransaction));
            throw DataUtils.newIllegalStateException(DataUtils.ERROR_TRANSACTION_LOCKED,
                    "Map entry <{0}> with key <{1}> is locked by tx {2} and can not be updated by tx {3} within allocated time interval {4} ms.",
                    map.getName(), key, blockingTransaction.transactionId, transaction.transactionId, transaction.timeoutMillis);
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
                TransactionStore store = transaction.store;
                BitSet committingTransactions;
                MVMap.RootReference mapRootReference;
                do {
                    committingTransactions = store.committingTransactions.get();
                    mapRootReference = map.getRoot();
                } while(committingTransactions != store.committingTransactions.get());

                VersionedValue current = (VersionedValue) Page.get(mapRootReference.root, key);
                VersionedValue old = getValue(current, committingTransactions);
                if (!map.areValuesEqual(old, current)) {
                    assert current != null;
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
         * Whether the map contains the key.
         *
         * @param key the key
         * @return true if the map contains an entry for this key
         */
        public boolean containsKey(K key) {
            return get(key) != null;
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

        /**
         * Get the effective value for the given key.
         *
         * @param key the key
         * @return the value or null
         */
        @SuppressWarnings("unchecked")
        public V get(K key) {
            VersionedValue data = (VersionedValue) Page.get(map.getRootPage(), key);
            if (data == null) {
                // doesn't exist or deleted by a committed transaction
                return null;
            }
            long id = data.operationId;
            if (id == 0) {
                // it is committed
                return (V)data.value;
            }
            int tx = getTransactionId(id);
            if (tx == transaction.transactionId || transaction.store.committingTransactions.get().get(tx)) {
                // added by this transaction or another transaction which is committed by now
                return (V)data.value;
            } else {
                return (V)data.committedValue;
            }
        }

        /**
         * Get the versioned value from the raw versioned value (possibly uncommitted),
         * as visible by the current transaction.
         *
         * @param data the value stored in the main map
         * @param committingTransactions set of transactions being committed
         *                               at the time when snapshot was taken
         * @return the value
         */
        private VersionedValue getValue(VersionedValue data, BitSet committingTransactions) {
            long id;
            int tx;
            if (data != null &&     // skip if entry doesn't exist or it was deleted by a committed transaction
                (id = data.operationId) != 0 && // skip if it is committed
                (tx = getTransactionId(id)) != transaction.transactionId && !committingTransactions.get(tx)) {
                // current value comes from another uncommitted transaction
                // take committed value instead
                data = data.committedValue == null ? null : new VersionedValue(data.committedValue);
            }
            return data;
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
            return keyIterator(from, null, false);
        }

        /**
         * Iterate over keys.
         *
         * @param from the first key to return
         * @param to the last key to return or null if there is no limit
         * @param includeUncommitted whether uncommitted entries should be
         *            included
         * @return the iterator
         */
        public Iterator<K> keyIterator(K from, K to, boolean includeUncommitted) {
            return new KeyIterator<K>(this, from, to, includeUncommitted);
        }

        /**
         * Iterate over entries.
         *
         * @param from the first key to return
         * @return the iterator
         */
        public Iterator<Entry<K, V>> entryIterator(final K from, final K to, boolean includeUncommitted) {
            return new EntryIterator<K,V>(this, from, to, includeUncommitted);
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
            private final Object         value;
            private final Transaction    transaction;
            private       long           undoKey;
            private       int            blockingId;
            private       Transaction    blockingTransaction;
            private       MVMap.Decision decision;

            private TxDecisionMaker(int mapId, Object key, Object value, Transaction transaction) {
                this.mapId = mapId;
                this.key = key;
                this.value = value;
                this.transaction = transaction;
            }

            @Override
            public MVMap.Decision decide(VersionedValue existingValue, VersionedValue providedValue) {
                assert decision == null;
                assert providedValue != null;
//                assert getTransactionId(providedValue.operationId) == transaction.transactionId;
//                assert getLogId(providedValue.operationId) == transaction.getLogId();
                long id;
                // if map does not have that entry yet
                if(existingValue == null ||
                        // or entry is a committed one
                        (id = existingValue.operationId) == 0 ||
                        // or it came from the same transaction
                        (blockingId = getTransactionId(id)) == transaction.transactionId) {
                    decision = MVMap.Decision.PUT;
                    undoKey = transaction.log(mapId, key, existingValue);
                } else if(transaction.store.committingTransactions.get().get(blockingId)
                    // condition above means that entry belongs to a committing transaction
                    // and therefore will be committed soon
                    || (blockingTransaction = transaction.store.getTransaction(blockingId)) == null) {
                    // condition above means transaction has been closed by now

                    // In both cases, we assume that we are looking at final value for this transaction
                    // and if it's not the case, then it will fail later
                    // because a tree root definitely has been changed
                    decision = MVMap.Decision.PUT;
                    undoKey = transaction.log(mapId, key, existingValue.value == null ? null : new VersionedValue(existingValue.value));
                } else {
                    // this entry comes from a different transaction and it's not committed yet
                    // should wait on blockingTransaction that was tetermined earlier
                    decision = MVMap.Decision.ABORT;
                }
                return decision;
            }

            @Override
            public VersionedValue selectValue(VersionedValue existingValue, VersionedValue providedValue) {
                assert decision == MVMap.Decision.PUT;
                return new VersionedValue(undoKey, value, existingValue == null ? null : existingValue.operationId == 0 ? existingValue.value : existingValue.committedValue);
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

        public static final VersionedValue DUMMY = new VersionedValue(new Object());

        /**
         * The operation id.
         */
        public final long operationId;

        /**
         * The value.
         */
        public final Object value;

        /**
         * Initial (committed) value for operationId > 0, null otherwise.
         */
        public final Object committedValue;


        public VersionedValue(Object value) {
            this(0, value, null);
        }

        public VersionedValue(long operationId, Object value, Object committedValue) {
            assert operationId != 0 || value != null;
            this.operationId = operationId;
            this.value = value;
            this.committedValue = committedValue;
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
    public static final class VersionedValueType extends BasicDataType<VersionedValue> implements StatefulDataType
    {
        private final DataType valueType;

        VersionedValueType(DataType valueType) {
            this.valueType = valueType;
        }

        @Override
        public int getMemory(Object obj) {
            if(obj == null) return 0;
            VersionedValue v = (VersionedValue) obj;
            int res = Constants.MEMORY_OBJECT + 8 + 2 * Constants.MEMORY_POINTER +
                      getValMemory(v.value);
            if (v.operationId > 0) {
                res += getValMemory(v.committedValue);
            }
            return res;
        }

        private int getValMemory(Object obj) {
            return obj == null ? 0 : valueType.getMemory(obj);
        }

        @Override
        public int compare(Object a, Object b) {
            if (a == b) {
                return 0;
            }
            VersionedValue one = (VersionedValue) a;
            VersionedValue two = (VersionedValue) b;
            int comp = Long.compare(one.operationId, two.operationId);
            if (comp == 0) {
                comp = valueType.compare(one.value, two.value);
            }
            return comp;
        }

        @Override
        public void write(WriteBuffer buff, Object obj) {
            VersionedValue v = (VersionedValue) obj;
            buff.putVarLong(v.operationId);
            if (v.operationId == 0) {
                valueType.write(buff, v.value);
            } else {
                int flags = (v.value == null ? 0 : 1) | (v.committedValue == null ? 0 : 2);
                buff.put((byte) flags);
                if (v.value != null) {
                    valueType.write(buff, v.value);
                }
                if (v.committedValue != null) {
                    valueType.write(buff, v.committedValue);
                }
            }
        }

        @Override
        public Object read(ByteBuffer buff) {
            long operationId = DataUtils.readVarLong(buff);
            if (operationId == 0) {
                return new VersionedValue(valueType.read(buff));

            } else {
                byte flags = buff.get();
                Object value = (flags & 1) != 0 ? valueType.read(buff) : null;
                Object committedValue = (flags & 2) != 0 ? valueType.read(buff) : null;
                return new VersionedValue(operationId, value, committedValue);
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
    public static final class RecordType extends BasicDataType<Record> {
        private final TransactionStore transactionStore;

        private RecordType(TransactionStore transactionStore) {
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
        public Record read(ByteBuffer buff) {
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

/*
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
*/

    private static final class FinalCommitDecisionMaker extends MVMap.DecisionMaker<VersionedValue> {
        private long           undoKey;
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

        public KeyIterator(TransactionMap<K, ?> transactionMap,
                           K from, K to, boolean includeUncommitted) {
            super(transactionMap, from, to, includeUncommitted);
        }

        @Override
        protected K registerCurrent(K key, VersionedValue data) {
            return key;
        }
    }

    private static final class EntryIterator<K,V> extends TMIterator<K,Entry<K,V>> {

        public EntryIterator(TransactionMap<K, ?> transactionMap, K from, K to, boolean includeUncommitted) {
            super(transactionMap, from, to, includeUncommitted);
        }

        @Override
        protected Entry<K, V> registerCurrent(K key, VersionedValue data) {
            @SuppressWarnings("unchecked")
            V value = (V) data.value;
            return new DataUtils.MapEntry<K, V>(key, value);
        }
    }

    private abstract static class TMIterator<K,X> implements Iterator<X> {
        private final TransactionMap<K,?>      transactionMap;
        private final BitSet                   committingTransactions;
        private final Page                     undoLogRootPage;
        private final Cursor<K,VersionedValue> cursor;
        private final boolean                  includeUncommitted;
        private       X                        current;

        protected TMIterator(TransactionMap<K,?> transactionMap, K from, K to, boolean includeUncommitted)
        {
            this.transactionMap = transactionMap;
            TransactionStore store = transactionMap.transaction.store;
            MVMap<Long, Record> undoLog = store.undoLogs[transactionMap.transaction.transactionId];
            Page undoLogRootPage = undoLog == null ? null : undoLog.getRootPage();
            MVMap<K, VersionedValue> map = transactionMap.map;
            BitSet committingTransactions;
            MVMap.RootReference mapRootReference;
            do {
                committingTransactions = store.committingTransactions.get();
                mapRootReference = map.getRoot();
            } while(committingTransactions != store.committingTransactions.get());

            this.undoLogRootPage = undoLogRootPage;
            this.cursor = new Cursor<>(mapRootReference.root, from, to);
            this.includeUncommitted = includeUncommitted;
            this.committingTransactions = committingTransactions;
        }

        protected abstract X registerCurrent(K key, VersionedValue data);

        private void fetchNext() {
            while (cursor.hasNext()) {
                K key = cursor.next();
                VersionedValue data = cursor.getValue();
                if (!includeUncommitted) {
                    data = transactionMap.getValue(data, committingTransactions);
                }
                if (data != null && data.value != null) {
                    current = registerCurrent(key, data);
                    return;
                }
            }
            current = null;
        }

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
    }
}
