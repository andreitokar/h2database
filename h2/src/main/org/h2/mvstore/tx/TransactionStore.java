/*
 * Copyright 2004-2018 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.mvstore.tx;

import java.util.ArrayList;
import java.util.BitSet;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.PriorityQueue;
import java.util.Queue;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.atomic.AtomicReferenceArray;

import org.h2.mvstore.Cursor;
import org.h2.mvstore.CursorPos;
import org.h2.mvstore.DataUtils;
import org.h2.mvstore.MVMap;
import org.h2.mvstore.MVStore;
import org.h2.mvstore.Page;
import org.h2.mvstore.db.DBMetaType;
import org.h2.mvstore.rtree.MVRTreeMap;
import org.h2.mvstore.type.DataType;
import org.h2.mvstore.type.ExtendedDataType;
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

    private final long timeoutMillis;

    /**
     * The persisted map of prepared transactions.
     * Key: transactionId, value: [ status, name ].
     */
    private final MVMap<Integer, Object[]> preparedTransactions;
    private final MVMap<String, DataType> typeRegistry;
    private final DataType dataType;
    private boolean init;
    private int maxTransactionId = MAX_OPEN_TRANSACTIONS;
    private final AtomicReferenceArray<Transaction> transactions = new AtomicReferenceArray<>(MAX_OPEN_TRANSACTIONS);
    /**
     * The undo logs.
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
    final AtomicReference<VersionedBitSet> openTransactions = new AtomicReference<>(new VersionedBitSet());
    final AtomicReference<BitSet> committingTransactions = new AtomicReference<>(new BitSet());
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
        DataType undoLogValueType = new Record.Type(this);
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
                        registerTransaction(status, name, logId, timeoutMillis, 0, listener);
                    }
                }
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
    private void commit(Transaction t) {
        if (!store.isClosed()) {
            int transactionId = t.transactionId;
            boolean success;
            do {
                BitSet original = committingTransactions.get();
                assert !original.get(transactionId) : "Double commit";
                BitSet clone = (BitSet) original.clone();
                clone.set(transactionId);
                success = committingTransactions.compareAndSet(original, clone);
            } while(!success);
            try {
                long state = t.setStatus(Transaction.STATUS_COMMITTED);
                MVMap<Long, Record> undoLog = undoLogs[transactionId];
                MVMap.RootReference rootReference = undoLog.flushAppendBuffer();
                Page rootPage = rootReference.root;
                CommitProcessor committProcessor = new CommitProcessor(this, transactionId,
                                Transaction.getTxLogId(state), rootPage.getTotalCount() > 32);
                MVMap.process(rootPage, null, committProcessor);
                committProcessor.flush();
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

    private static final class CommitProcessor  extends MVMap.DecisionMaker<VersionedValue>
                                                implements MVMap.Processor<Long, Record>,
                                                           MVMap.LeafProcessor {
        private final TransactionStore transactionStore;
        private final int              transactionId;
        private final long             maxLogId;
        private final boolean          batchMode;
        private final Map<Integer,BatchInfo> batches = new HashMap<>();

        private       long             undoKey;
        private       MVMap.Decision   decision;

        private       BatchInfo        batchInfo;
        private       Object           lastKeyOnPage;


        private CommitProcessor(TransactionStore transactionStore, int transactionId, long maxLogId, boolean batchMode) {
            this.transactionStore = transactionStore;
            this.transactionId = transactionId;
            this.maxLogId = maxLogId;
            this.batchMode = batchMode;
        }

        @Override
        public boolean process(Long undoKey, Record existingValue) {
            assert getTransactionId(undoKey) == transactionId : getTransactionId(undoKey) + " != " + transactionId;
            assert getLogId(undoKey) <= maxLogId : getLogId(undoKey) + " > " + maxLogId;

            int mapId = existingValue.mapId;
            MVMap<Object, VersionedValue> map;
            BatchInfo batchInfo = batches.get(mapId);
            if (batchInfo == null) {
                map = transactionStore.openMap(mapId);
                if (map == null || map.isClosed()) {
                    batchInfo = BatchInfo.NOOP;
                } else if(!batchMode || map.getType() != null) {
                    batchInfo = new BatchInfo(map);
                } else {
                    batchInfo = new BatchInfo(map, 1023);
                }
                batches.put(mapId, batchInfo);
            }
            if (batchInfo != BatchInfo.NOOP) {
                map = batchInfo.map;
                Object key = existingValue.key;
                VersionedValue prev = existingValue.oldValue;
                assert prev == null || prev.getOperationId() == 0 || getTransactionId(prev.getOperationId()) == transactionId;
                if (batchInfo.heap == null) {
                    reset();
                    this.undoKey = undoKey;
                    map.operate(key, null, this);
                } else if (existingValue.oldValue == null || existingValue.oldValue.getOperationId() == 0) {
                    batchInfo.heap.offer(key);
                    if (batchInfo.heap.size() >= 1023) {
                        this.batchInfo = batchInfo;
                        map.operateBatch(this);
                    }
                }
            }
            return false;
        }

        @Override
        public MVMap.Decision decide(VersionedValue existingValue, VersionedValue providedValue) {
            assert decision == null;
            if (existingValue == null) {
                decision = MVMap.Decision.ABORT;
            } else {
                if (existingValue.getOperationId() == undoKey) {
                    if (existingValue.value == null) {
                        decision = MVMap.Decision.REMOVE;
                    } else {
                        decision = MVMap.Decision.PUT;
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
            assert existingValue != null;
            return new VersionedValue(existingValue.value);
        }

        @Override
        public void reset() {
            decision = null;
        }

        @Override
        public String toString() {
            return "final_commit " + getTransactionId(undoKey);
        }

        @Override
        public CursorPos locate(Page rootPage) {
            Queue<Object> heap = batchInfo.heap;
            assert heap != null;
            Object key;
            while ((key = heap.peek()) != null) {
                CursorPos pos = MVMap.traverseDown(rootPage, key);
                int index = pos.index;
                Page leaf = pos.page;
                if (index >= 0 && getTransactionId(((VersionedValue) leaf.getValue(index)).getOperationId()) == transactionId) {
                    lastKeyOnPage = leaf.getKey(leaf.getKeyCount() - 1);
                    return pos;
                }
                heap.poll();
            }
            return null;
        }

        @Override
        public CursorPos locateNext(Page rootPage) {
            return null;
        }

        @Override
        public Page[] process(CursorPos pos) {
            assert pos.index >= 0 : pos.index;
            Page page = pos.page;
            int keyCount = page.getKeyCount();
            int count = 0;
            for (int i = pos.index; i < keyCount; ++i) {
                VersionedValue existingValue = (VersionedValue) page.getValue(i);
                if (getTransactionId(existingValue.getOperationId()) == transactionId) {
                    if (page == pos.page) {
                        page = page.copy();
                    }
                    if (existingValue.value == null) {
                        ++count;
                    }
                }
            }

            if (page == pos.page) {     // no changes
                return null;
            } else if (count == 0) {   // there are no removal, updates only
                for (int i = keyCount - 1; i >= pos.index; --i) {
                    VersionedValue existingValue = (VersionedValue) page.getValue(i);
                    if (getTransactionId(existingValue.getOperationId()) == transactionId) {
                        if (page == pos.page) {
                            page = page.copy();
                        }
                        page.setValue(i, new VersionedValue(existingValue.value));
                    }
                }
            } else {
                int newKeyCount = keyCount - count;
                if (newKeyCount == 0) {   // all keys to be removed, remove the whole Leaf node
                    return new Page[0];
                }

                ExtendedDataType keyType = page.map.getExtendedKeyType();
                ExtendedDataType valueType = page.map.getExtendedValueType();
                Object keys = keyType.createStorage(newKeyCount);
                Object values = valueType.createStorage(newKeyCount);
                count = 0;
                for (int i = 0; i < keyCount; ++i) {
                    VersionedValue value = (VersionedValue) page.getValue(i);
                    if (i >= pos.index && getTransactionId(value.getOperationId()) == transactionId) {
                        if (value.value == null) {
                            continue;
                        }
                        value = new VersionedValue(value.value);
                    }
                    keyType.setValue(keys, count, page.getKey(i));
                    valueType.setValue(values, count, value);
                    ++count;
                }
                page = Page.create(page.map, newKeyCount, keys, values, null, newKeyCount, 0);
            }
            return new Page[]{ page };
        }

        @Override
        public void stepBack() {}

        @Override
        public void confirmSuccess() {
            Comparator<Object> comparator = batchInfo.map.getKeyType();
            Queue<Object> heap = batchInfo.heap;
            assert heap != null;
            Object key;
            while ((key = heap.peek()) != null && comparator.compare(key, lastKeyOnPage) <= 0) {
                heap.poll();
            }
        }

        public void flush() {
            for (BatchInfo batchInfo : batches.values()) {
                if (batchInfo != BatchInfo.NOOP) {
                    Queue<Object> heap = batchInfo.heap;
                    if (heap != null) {
                        this.batchInfo = batchInfo;
                        MVMap<Object, VersionedValue> map = batchInfo.map;
                        while(!heap.isEmpty()) {
                            map.operateBatch(this);
                        }
//                        assert verifyCleanCommit(map);
                    }
                }
            }
        }

        private boolean verifyCleanCommit(MVMap<Object, VersionedValue> map) {
            MVMap.process(map.getRootPage(), null, new MVMap.Processor<Object,VersionedValue>(){
                @Override
                public boolean process(Object key, VersionedValue value) {
                    assert getTransactionId(value.getOperationId()) != transactionId;
                    return false;
                }
            });
            return true;
        }
    }

    private static final class BatchInfo
    {
        final MVMap<Object, VersionedValue> map;
        final Queue<Object>                 heap;

        static final BatchInfo NOOP = new BatchInfo(null);

        BatchInfo(MVMap<Object, VersionedValue> map) {
            this.map = map;
            heap = null;
        }

        BatchInfo(MVMap<Object, VersionedValue> map, int capacity) {
            this.map = map;
            this.heap = new PriorityQueue<>(capacity, map.getKeyType());
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
    private MVMap<Object, VersionedValue> openMap(int mapId) {
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
     */
    void endTransaction(Transaction t) {
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

    private static final class TxMapBuilder<K,V> extends MVMap.Builder<K,V> {

        private final MVMap<String, DataType> typeRegistry;
        private final DataType defaultDataType;

        private TxMapBuilder(MVMap<String, DataType> typeRegistry, DataType defaultDataType) {
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

        public final long sequenceNo;

        /*
         * Transation state is an atomic composite field:
         * bit 44       : flag whether transaction had rollback(s)
         * bits 42-40   : status
         * bits 39-0    : log id of the last entry in the undo log map
         */
        private final AtomicLong statusAndLogId;

        private String name;

        private final long timeoutMillis;

        private volatile Transaction blockingTransaction;

        private final int ownerId;

        private MVStore.TxCounter txCounter;

        private boolean wasStored;
        private MVMap blockingMap;
        private Object blockingKey;

        private Transaction(TransactionStore store, int transactionId, long sequenceNo, int status,
                            String name, long logId, long timeoutMillis, int ownerId, RollbackListener listener) {
            this.store = store;
            this.transactionId = transactionId;
            this.sequenceNo = sequenceNo;
            this.statusAndLogId = new AtomicLong(composeState(status, logId, false));
            this.name = name;
            this.timeoutMillis = timeoutMillis;
            this.ownerId = ownerId;
            this.listener = listener;
        }

        private Transaction(Transaction tx) {
            this.store = tx.store;
            this.transactionId = tx.transactionId;
            this.sequenceNo = tx.sequenceNo;
            this.statusAndLogId = new AtomicLong(tx.statusAndLogId.get());
            this.name = tx.name;
            this.timeoutMillis = tx.timeoutMillis;
            this.blockingTransaction = tx.blockingTransaction;
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

        public void setName(String name) {
            checkNotClosed();
            this.name = name;
            store.storeTransaction(this);
        }

        public String getName() {
            return name;
        }

        public int getBlockerId() {
            return blockingTransaction == null ? 0 : blockingTransaction.ownerId;
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
            store.addUndoLogRecord(undoKey, log);
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
            store.removeUndoLogRecord(undoKey);
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
            try {
                store.rollbackTo(this, logId, savepointId);
                long expectedState = composeState(STATUS_ROLLING_BACK, logId, hasRollback(lastState));
                long newState = composeState(STATUS_OPEN, savepointId, true);
                if (!statusAndLogId.compareAndSet(expectedState, newState)) {
                    throw DataUtils.newIllegalStateException(
                            DataUtils.ERROR_TRANSACTION_ILLEGAL_STATE,
                            "Transaction {0} concurrently modified " +
                                    "while rollback to savepoint was in progress",
                            transactionId);
                }
            } finally {
                notifyAllWaitingTransactions();
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
            store.store.deregisterVersionUsage(txCounter);
            if(hasChanges(lastState) || hasRollback(lastState)) {
                notifyAllWaitingTransactions();
            }
        }

        private synchronized void notifyAllWaitingTransactions() {
            notifyAll();
        }

        public boolean waitFor(Transaction toWaitFor) {
            if (isDeadlocked(toWaitFor)) {
                StringBuilder details = new StringBuilder(String.format("Transaction %d has been chosen as a deadlock victim. Details:%n", transactionId));
                for(Transaction tx = toWaitFor, nextTx; (nextTx = tx.blockingTransaction) != null; tx = nextTx) {
                    details.append(String.format("Transaction %d attempts to update map <%s> entry with key <%s> modified by transaction %s%n",
                            tx.transactionId, tx.blockingMap.getName(), tx.blockingKey, tx.blockingTransaction));
                    if (nextTx == this) {
                        details.append(String.format("Transaction %d attempts to update map <%s> entry with key <%s> modified by transaction %s%n",
                                transactionId, blockingMap.getName(), blockingKey, toWaitFor));
                        throw DataUtils.newIllegalStateException(DataUtils.ERROR_TRANSACTIONS_DEADLOCK, details.toString());
                    }
                }
            }

            blockingTransaction = toWaitFor;
            try {
                return toWaitFor.waitForThisToEnd(timeoutMillis);
            } finally {
                blockingMap = null;
                blockingKey = null;
                blockingTransaction = null;
            }
        }

        private boolean isDeadlocked(Transaction toWaitFor) {
            for(Transaction tx = toWaitFor, nextTx;
                    (nextTx = tx.blockingTransaction) != null && tx.getStatus() == Transaction.STATUS_OPEN;
                    tx = nextTx) {
                if (nextTx == this) {
                    return true;
                }
            }
            return false;
        }

        private synchronized boolean waitForThisToEnd(long millis) {
            long until = System.currentTimeMillis() + millis;
            int status;
            while((status = getStatus()) != STATUS_CLOSED && status != STATUS_ROLLING_BACK) {
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
            long state = statusAndLogId.get();
            return transactionId + "(" + sequenceNo + ") " + STATUS_NAMES[getStatus(state)] + " " + getTxLogId(state);
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
        public final MVMap<K, VersionedValue> map;

        final Transaction transaction;

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
                long id = value.getOperationId();
                if (id != 0) {  // skip committed entries
                    Object v = isVisible(getTransactionId(id)) ? value.value : value.getCommittedValue();
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
                    if (isVisible ? op.oldValue != null && op.oldValue.getOperationId() == 0 && op.oldValue.value != null
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
            TransactionStore store = transaction.store;
            Transaction blockingTransaction;
            long sequenceNoWhenStarted;
            do {
                sequenceNoWhenStarted = store.openTransactions.get().getVersion();
                assert transaction.getBlockerId() == 0;
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
                        " is missing, open: " + store.openTransactions.get().get(decisionMaker.blockingId) +
                        ", committing: " + store.committingTransactions.get().get(decisionMaker.blockingId);
                decisionMaker.reset();
                transaction.blockingMap = map;
                transaction.blockingKey = key;
            } while (blockingTransaction.sequenceNo > sequenceNoWhenStarted || transaction.waitFor(blockingTransaction));

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
                    long tx = getTransactionId(current.getOperationId());
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
            int tx = getTransactionId(data.getOperationId());
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
            long id = data.getOperationId();
            if (id == 0) {
                // it is committed
                return (V)data.value;
            }
            int tx = getTransactionId(id);
            if (tx == transaction.transactionId || transaction.store.committingTransactions.get().get(tx)) {
                // added by this transaction or another transaction which is committed by now
                return (V)data.value;
            } else {
                return (V) data.getCommittedValue();
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
                (id = data.getOperationId()) != 0 && // skip if it is committed
                (tx = getTransactionId(id)) != transaction.transactionId && !committingTransactions.get(tx)) {
                // current value comes from another uncommitted transaction
                // take committed value instead
                Object committedValue = data.getCommittedValue();
                data = committedValue == null ? null : new VersionedValue(committedValue);
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
            while (k != null && get(k) == null) {
                k = map.lowerKey(k);
            }
            return k;
        }

        /**
         * Get the smallest key that is larger than the given key, or null if no
         * such key exists.
         *
         * @param key the key (may not be null)
         * @return the result
         */
        public K higherKey(K key) {
            do {
                key = map.higherKey(key);
            } while (key != null && get(key) == null);
            return key;
        }

        /**
         * Get the smallest key that is larger than or equal to this key,
         * or null if no such key exists.
         *
         * @param key the key (may not be null)
         * @return the result
         */
        public K ceilingKey(K key) {
            Iterator<K> it = keyIterator(key);
            return it.hasNext() ? it.next() : null;
        }

        /**
         * Get the largest key that is smaller than or equal to this key,
         * or null if no such key exists.
         *
         * @param key the key (may not be null)
         * @return the result
         */
        public K floorKey(K key) {
            key = map.floorKey(key);
            while (key != null && get(key) == null) {
                // Use lowerKey() for the next attempts, otherwise we'll get an infinite loop
                key = map.lowerKey(key);
            }
            return key;
        }

        /**
         * Get the largest key that is smaller than the given key, or null if no
         * such key exists.
         *
         * @param key the key (may not be null)
         * @return the result
         */
        public K lowerKey(K key) {
            do {
                key = map.lowerKey(key);
            } while (key != null && get(key) == null);
            return key;
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
        public Iterator<Entry<K, V>> entryIterator(final K from, final K to) {
            return new EntryIterator<K,V>(this, from, to);
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
                long id;
                // if map does not have that entry yet
                if(existingValue == null ||
                        // or entry is a committed one
                        (id = existingValue.getOperationId()) == 0 ||
                        // or it came from the same transaction
                        (blockingId = getTransactionId(id)) == transaction.transactionId) {
                    decision = MVMap.Decision.PUT;
                    undoKey = transaction.log(mapId, key, existingValue);
                } else if(transaction.store.committingTransactions.get().get(blockingId)
                    // condition above means that entry belongs to a committing transaction
                    // and therefore will be committed soon
                    || (blockingTransaction = transaction.store.getTransaction(blockingId)) == null) {
                    // condition above means transaction has been committed and closed by now

                    // In both cases, we assume that we are looking at final value for this transaction
                    // and if it's not the case, then it will fail later
                    // because a tree root definitely has been changed
                    decision = MVMap.Decision.PUT;
                    undoKey = transaction.log(mapId, key, existingValue.value == null ? null : new VersionedValue(existingValue.value));
                } else {
                    // this entry comes from a different transaction, and this transaction is not committed yet
                    // should wait on blockingTransaction that was determined earlier
                    decision = MVMap.Decision.ABORT;
                }
                return decision;
            }

            @Override
            public VersionedValue selectValue(VersionedValue existingValue, VersionedValue providedValue) {
                assert decision == MVMap.Decision.PUT;
                return new VersionedValue.Uncommitted(undoKey, value,
                        existingValue == null ? null : existingValue.getOperationId() == 0 ? existingValue.value : existingValue.getCommittedValue());
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
            assert existingValue != null;
            VersionedValue valueToRestore = existingValue.oldValue;
            long operationId;
            if(valueToRestore == null ||
                    (operationId = valueToRestore.getOperationId()) == 0 ||
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

        public EntryIterator(TransactionMap<K, ?> transactionMap, K from, K to) {
            super(transactionMap, from, to, false);
        }

        @Override
        @SuppressWarnings("unchecked")
        protected Entry<K, V> registerCurrent(K key, VersionedValue data) {
            return new DataUtils.MapEntry<>(key, (V) data.value);
        }
    }

    private abstract static class TMIterator<K,X> implements Iterator<X> {
        private final TransactionMap<K,?>      transactionMap;
        private final BitSet                   committingTransactions;
        private final Cursor<K,VersionedValue> cursor;
        private final boolean                  includeAllUncommitted;
        private       X                        current;

        protected TMIterator(TransactionMap<K,?> transactionMap, K from, K to, boolean includeAllUncommitted)
        {
            this.transactionMap = transactionMap;
            TransactionStore store = transactionMap.transaction.store;
            MVMap<K, VersionedValue> map = transactionMap.map;
            BitSet committingTransactions;
            MVMap.RootReference mapRootReference;
            do {
                committingTransactions = store.committingTransactions.get();
                mapRootReference = map.getRoot();
            } while(committingTransactions != store.committingTransactions.get());

            this.cursor = new Cursor<>(mapRootReference.root, from, to);
            this.includeAllUncommitted = includeAllUncommitted;
            this.committingTransactions = committingTransactions;
        }

        protected abstract X registerCurrent(K key, VersionedValue data);

        private void fetchNext() {
            while (cursor.hasNext()) {
                K key = cursor.next();
                VersionedValue data = cursor.getValue();
                if (!includeAllUncommitted) {
                    data = transactionMap.getValue(data, committingTransactions);
                }
                if (data != null && (data.value != null ||
                        includeAllUncommitted && transactionMap.transaction.transactionId !=
                                                    getTransactionId(data.getOperationId()))) {
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
