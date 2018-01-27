/*
 * Copyright 2004-2018 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.mvstore.tx;

import org.h2.mvstore.Cursor;
import org.h2.mvstore.DataUtils;
import org.h2.mvstore.MVMap;
import org.h2.mvstore.Page;
import static org.h2.mvstore.tx.TransactionStore.getTransactionId;
import org.h2.mvstore.type.DataType;
import java.util.BitSet;
import java.util.Iterator;
import java.util.Map;

/**
 * A map that supports transactions.
 *
 * @param <K> the key type
 * @param <V> the value type
 */
public final class TransactionalMVMap<K, V> {
    /**
     * The map used for writing (the latest version).
     * <p>
     * Key: key the key of the data.
     * Value: { transactionId, oldVersion, value }
     */
    public final MVMap<K, VersionedValue> map;

    final Transaction transaction;

    TransactionalMVMap(Transaction transaction, MVMap<K, VersionedValue> map) {
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
    public TransactionalMVMap<K, V> getInstance(Transaction transaction) {
        return new TransactionalMVMap<>(transaction, map);
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
        private final Page root;
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
    public Iterator<Map.Entry<K, V>> entryIterator(final K from, final K to) {
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


    private static final class KeyIterator<K> extends TMIterator<K,K> {

        public KeyIterator(TransactionalMVMap<K, ?> transactionMap,
                           K from, K to, boolean includeUncommitted) {
            super(transactionMap, from, to, includeUncommitted);
        }

        @Override
        protected K registerCurrent(K key, VersionedValue data) {
            return key;
        }
    }

    private static final class EntryIterator<K,V> extends TMIterator<K,Map.Entry<K,V>> {

        public EntryIterator(TransactionalMVMap<K, ?> transactionMap, K from, K to) {
            super(transactionMap, from, to, false);
        }

        @Override
        @SuppressWarnings("unchecked")
        protected Map.Entry<K, V> registerCurrent(K key, VersionedValue data) {
            return new DataUtils.MapEntry<>(key, (V) data.value);
        }
    }

    private abstract static class TMIterator<K,X> implements Iterator<X> {
        private final TransactionalMVMap<K,?>  transactionMap;
        private final BitSet                   committingTransactions;
        private final Cursor<K,VersionedValue> cursor;
        private final boolean                  includeAllUncommitted;
        private       X                        current;

        protected TMIterator(TransactionalMVMap<K,?> transactionMap, K from, K to, boolean includeAllUncommitted)
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
