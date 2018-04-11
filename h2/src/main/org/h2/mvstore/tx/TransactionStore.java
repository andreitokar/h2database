/*
 * Copyright 2004-2018 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.mvstore.tx;

import java.util.ArrayList;
import java.util.BitSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.atomic.AtomicReferenceArray;

import org.h2.mvstore.DataUtils;
import org.h2.mvstore.MVMap;
import org.h2.mvstore.MVStore;
import org.h2.mvstore.Page;
import org.h2.mvstore.db.DBMetaType;
import org.h2.mvstore.rtree.MVRTreeMap;
import org.h2.mvstore.type.DataType;
import org.h2.mvstore.type.LongDataType;
import org.h2.mvstore.type.ObjectDataType;
import org.h2.mvstore.type.StringDataType;

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

    /**
     * Default blocked transaction timeout
     */
    private final long timeoutMillis;

    /**
     * The persisted map of prepared transactions.
     * Key: transactionId, value: [ status, name ].
     */
    private final MVMap<Integer, Object[]> preparedTransactions;
    private final MVMap<String, DataType> typeRegistry;
    private final DataType dataType;
    final AtomicReference<VersionedBitSet> openTransactions = new AtomicReference<>(new VersionedBitSet());
    final AtomicReference<BitSet> committingTransactions = new AtomicReference<>(new BitSet());
    private boolean init;
    private int maxTransactionId = MAX_OPEN_TRANSACTIONS;
    private final AtomicReferenceArray<Transaction> transactions = new AtomicReferenceArray<>(MAX_OPEN_TRANSACTIONS);
    /**
     * Undo logs.
     * <p>
     * If the first entry for a transaction doesn't have a logId
     * of 0, then the transaction is partially committed (which means rollback
     * is not possible). Log entries are written before the data is changed
     * (write-ahead).
     * <p>
     * Key: opId, value: [ mapId, key, oldValue ].
     */
    @SuppressWarnings("unchecked")
    final MVMap<Long,Record> undoLogs[] = (MVMap<Long,Record>[])new MVMap[MAX_OPEN_TRANSACTIONS];
    private final MVMap.Builder<Long, Record> undoLogBuilder;

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
        DataType undoLogValueType = new Record.Type(this);
        undoLogBuilder = new MVMap.Builder<Long, Record>()
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
                        MVMap<Long,Record> undoLog = store.openMap(mapName, undoLogBuilder);
                        undoLogs[transactionId] = undoLog;
                        Object[] data = preparedTransactions.get(transactionId);
                        int status;
                        String name;
                        if (data == null) {
                            status = Transaction.STATUS_OPEN;
                            name = null;
                        } else {
                            status = (Integer) data[0];
                            name = (String) data[1];
                        }
                        long logId = getLogId(undoLog.lastKey()) + 1;
                        registerTransaction(status, name, logId, timeoutMillis, 0, listener);
                    }
                }
            }
            init = true;
        }
    }

    boolean isInitialized() {
        return init;
    }

    /**
     * Commit all transactions that are in the committing state, and
     * rollback all open transactions.
     */
    public void endLeftoverTransactions() {
        List<Transaction> list = getOpenTransactions();
        for (Transaction tx : list) {
            int transactionId = tx.transactionId;
            int status = tx.getStatus();
            if (status == Transaction.STATUS_OPEN) {
                // TODO: implement more sophisticated full undoLog analysis for Tx status determination
                MVMap<Long, Record> undoLog = undoLogs[transactionId];
                Long undoKey = undoLog.firstKey();
                if (undoKey == null || getLogId(undoKey) != 0) {
                    status = Transaction.STATUS_COMMITTING;
                } else {
                    Record record = undoLog.get(undoKey);
                    MVMap<Object, VersionedValue> map = openMap(record.mapId);
                    if (map != null && !map.isClosed()) {
                        Object key = record.key;
                        VersionedValue versionedValue = map.get(key);
                        if (versionedValue == null || versionedValue.getOperationId() == 0) {
                            status = Transaction.STATUS_COMMITTING;
                        }
                    }
                }
            }

            if(status == Transaction.STATUS_COMMITTING) {
                tx.commit();
            } else if(status != Transaction.STATUS_PREPARED) {
                tx.rollback();
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
     * Check whether a given map exists.
     *
     * @param name the map name
     * @return true if it exists
     */
    public boolean hasMap(String name) {
        return store.hasMap(name);
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
    static int getTransactionId(long operationId) {
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
        ArrayList<Transaction> list = new ArrayList<>();
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
        return begin(RollbackListener.NONE, timeoutMillis, 0);
    }

    /**
     * Begin a new transaction.
     * @param listener to be notified in case of a rollback
     * @param timeoutMillis to wait for a blocking transaction
     * @param ownerId of the owner (Session?) to be reported by getBlockerId
     * @return the transaction
     */
    public Transaction begin(RollbackListener listener, long timeoutMillis, int ownerId) {

        if(timeoutMillis <= 0) {
            timeoutMillis = this.timeoutMillis;
        }
        Transaction transaction = registerTransaction(Transaction.STATUS_OPEN, null, 0,
                                                      timeoutMillis, ownerId, listener);
        return transaction;
    }

    private Transaction registerTransaction(int status, String name, long logId,
                                            long timeoutMillis, int ownerId, RollbackListener listener) {
        int transactionId;
        long sequenceNo;
        boolean success;
        do {
            VersionedBitSet original = openTransactions.get();
            transactionId = original.nextClearBit(1);
            if (transactionId > maxTransactionId) {
                throw DataUtils.newIllegalStateException(
                        DataUtils.ERROR_TOO_MANY_OPEN_TRANSACTIONS,
                        "There are {0} open transactions",
                        transactionId - 1);
            }
            VersionedBitSet clone = original.cloneIt();
            clone.set(transactionId);
            sequenceNo = clone.getVersion() + 1;
            clone.setVersion(sequenceNo);
            success = openTransactions.compareAndSet(original, clone);
        } while(!success);

        Transaction transaction = new Transaction(this, transactionId, sequenceNo, status, name, logId,
                                                  timeoutMillis, ownerId, listener);
        assert transactions.get(transactionId) == null;
        transactions.set(transactionId, transaction);

        if (undoLogs[transactionId] == null) {
            String undoName = UNDO_LOG_NAME_PEFIX + transactionId;
            undoLogs[transactionId] = store.openMap(undoName, undoLogBuilder);
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
     * Add an undoLog entry.
     * @param undoKey transactionId/LogId
     * @param record Record to add
     */
    void addUndoLogRecord(Long undoKey, Record record) {
        int transactionId = getTransactionId(undoKey);
        undoLogs[transactionId].append(undoKey, record);
    }

    /**
     * Remove an undoLog entry.
     * @param undoKey transactionId/LogId
     */
    void removeUndoLogRecord(Long undoKey) {
        int transactionId = getTransactionId(undoKey);
        undoLogs[transactionId].trimLast();
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
    void commit(Transaction t) {
        if (!store.isClosed()) {
            int transactionId = t.transactionId;
            // this is an atomic action that causes all changes
            // made by this transaction, to be considered as "committed"
            flipCommittingTransactionsBit(transactionId, true);

            try {
                t.setStatus(Transaction.STATUS_COMMITTED);
                MVMap<Long, Record> undoLog = undoLogs[transactionId];
                MVMap.RootReference rootReference = undoLog.flushAppendBuffer();
                Page rootPage = rootReference.root;
                CommitEntryProcessor committProcessor = new CommitEntryProcessor(this, transactionId,
                        rootPage.getTotalCount() > 32);
                MVMap.process(rootPage, null, committProcessor);
                committProcessor.flush();
                undoLog.clear();
            } finally {
                flipCommittingTransactionsBit(transactionId, false);
            }
        }
    }

    private void flipCommittingTransactionsBit(int transactionId, boolean flag) {
        boolean success;
        do {
            BitSet original = committingTransactions.get();
            assert original.get(transactionId) != flag : flag ? "Double commit" : "Mysterious bit's disappearance";
            BitSet clone = (BitSet) original.clone();
            clone.set(transactionId, flag);
            success = committingTransactions.compareAndSet(original, clone);
        } while(!success);
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
        VersionedValue.Type vt = valueType == null ? null : new VersionedValue.Type(valueType);
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
    MVMap<Object, VersionedValue> openMap(int mapId) {
        MVMap<Object, VersionedValue> map = store.getMap(mapId);
        if (map == null) {
            MVMap.Builder<Object, VersionedValue> txMapBuilder = new TxMapBuilder<>(typeRegistry, dataType);
            map = store.openMap(mapId, txMapBuilder);
        }
        return map;
    }

    MVMap<Object,VersionedValue> getMap(int mapId) {
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
     * @param hasChanges whether transaction has done any updated
     */
    void endTransaction(Transaction t, boolean hasChanges) {
        t.closeIt();
        int txId = t.transactionId;
        transactions.set(txId, null);

        boolean success;
        do {
            VersionedBitSet original = openTransactions.get();
            assert original.get(txId);
            VersionedBitSet clone = original.cloneIt();
            clone.clear(txId);
            success = openTransactions.compareAndSet(original, clone);
        } while(!success);

        if (hasChanges) {
            boolean wasStored = t.wasStored;
            if (wasStored && !preparedTransactions.isClosed()) {
                preparedTransactions.remove(txId);
            }

            if (wasStored || store.getAutoCommitDelay() == 0) {
                store.tryCommit();
            } else {
                boolean empty = true;
                BitSet openTrans = openTransactions.get();
                for (int i = openTrans.nextSetBit(0); empty && i >= 0; i = openTrans.nextSetBit(i + 1)) {
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
                        store.tryCommit();
                    }
                }
            }
        }
    }

    Transaction getTransaction(int transactionId) {
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

    private static final class TxMapBuilder<K,V> extends MVMap.Builder<K,V> {

        private final MVMap<String, DataType> typeRegistry;
        private final DataType defaultDataType;

        private TxMapBuilder(MVMap<String, DataType> typeRegistry, DataType defaultDataType) {
            this.typeRegistry = typeRegistry;
            this.defaultDataType = defaultDataType;
        }

        private void registerDataType(DataType dataType) {
            String key = getDataTypeRegistrationKey(dataType);
            DataType registeredDataType = typeRegistry.putIfAbsent(key, dataType);
            if(registeredDataType != null) {
                // TODO: ensure type consistency
            }
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
                setValueType(new VersionedValue.Type(defaultDataType));
                registerDataType(getValueType());
            }

            config.put("store", store);
            config.put("key", getKeyType());
            config.put("val", getValueType());
            return create(config);
        }


        @Override
        @SuppressWarnings("unchecked")
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
                return new TMVMap<>(this);
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

    public interface RollbackListener {
        RollbackListener NONE = new RollbackListener() {
            @Override
            public void onRollback(MVMap<Object, VersionedValue> map, Object key, VersionedValue existingValue, VersionedValue restoredValue) {

            }
        };
        void onRollback(MVMap<Object,VersionedValue> map, Object key,
                        VersionedValue existingValue, VersionedValue restoredValue);
    }

    static final class RollbackDecisionMaker extends MVMap.DecisionMaker<Record> {
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
            if (existingValue == null) {
                // this should only be possible during initialization
                // when previously database was abruptly killed
//                assert !store.isInitialized();
                decision = MVMap.Decision.ABORT;
            } else {
                VersionedValue valueToRestore = existingValue.oldValue;
                long operationId;
                if (valueToRestore == null ||
                        (operationId = valueToRestore.getOperationId()) == 0 ||
                        getTransactionId(operationId) == transactionId
                                && getLogId(operationId) < toLogId) {
                    int mapId = existingValue.mapId;
                    MVMap<Object, VersionedValue> map = store.openMap(mapId);
                    if (map != null && !map.isClosed()) {
                        Object key = existingValue.key;
                        VersionedValue previousValue = map.operate(key, valueToRestore, MVMap.DecisionMaker.DEFAULT);
                        listener.onRollback(map, key, previousValue, valueToRestore);
                    }
                }
                decision = MVMap.Decision.REMOVE;
            }
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
}
