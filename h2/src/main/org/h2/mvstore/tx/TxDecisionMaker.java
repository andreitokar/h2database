/*
 * Copyright 2004-2018 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.mvstore.tx;

import org.h2.mvstore.MVMap;
import static org.h2.mvstore.tx.TransactionStore.getTransactionId;

/**
 * Class TxDecisionMaker.
 *
 * @author <a href='mailto:andrei.tokar@gmail.com'>Andrei Tokar</a>
 */
public class TxDecisionMaker extends MVMap.DecisionMaker<VersionedValue> {
    private final int            mapId;
    private final Object         key;
    private final Transaction    transaction;
                  long           undoKey;
    private       int            blockingId;
    private       Transaction    blockingTransaction;
    private       MVMap.Decision decision;

    TxDecisionMaker(int mapId, Object key, Transaction transaction) {
        this.mapId = mapId;
        this.key = key;
        this.transaction = transaction;
    }

    @Override
    public final MVMap.Decision decide(VersionedValue existingValue, VersionedValue providedValue) {
        assert decision == null;
        assert providedValue != null;
        long id;
        // if map does not have that entry yet
        if (existingValue == null ||
                // or entry is a committed one
                (id = existingValue.getOperationId()) == 0 ||
                // or it came from the same transaction
                (blockingId = getTransactionId(id)) == transaction.transactionId) {
            decision = MVMap.Decision.PUT;
            undoKey = transaction.log(mapId, key, existingValue);
        } else if (transaction.store.committingTransactions.get().get(blockingId)
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
    public final void reset() {
        if (decision != MVMap.Decision.ABORT) {
            // map was updated after our decision has been made
            transaction.logUndo();
        }
        blockingTransaction = null;
        decision = null;
    }

    public final MVMap.Decision getDecision() {
        return decision;
    }

    final Transaction getBlockingTransaction() {
        assert blockingTransaction != null : "Tx " + blockingId +
                " is missing, open: " + transaction.store.openTransactions.get().get(blockingId) +
                ", committing: " + transaction.store.committingTransactions.get().get(blockingId);
        return blockingTransaction;
    }

    @Override
    public final String toString() {
        return "tx " + transaction.transactionId;
    }


    public static final class PutDecisionMaker extends TxDecisionMaker
    {
        private final Object         value;

        PutDecisionMaker(int mapId, Object key, Object value, Transaction transaction) {
            super(mapId, key, transaction);
            this.value = value;
        }

        @Override
        public VersionedValue selectValue(VersionedValue existingValue, VersionedValue providedValue) {
            return new VersionedValue.Uncommitted(undoKey, value,
                    existingValue == null ? null : existingValue.getOperationId() == 0 ? existingValue.value : existingValue.getCommittedValue());
        }
    }


    public static final class LockDecisionMaker extends TxDecisionMaker
    {
        LockDecisionMaker(int mapId, Object key, Transaction transaction) {
            super(mapId, key, transaction);
        }

        @Override
        public VersionedValue selectValue(VersionedValue existingValue, VersionedValue providedValue) {
            return new VersionedValue.Uncommitted(undoKey, existingValue == null ? null : existingValue.value,
                    existingValue == null ? null : existingValue.getOperationId() == 0 ? existingValue.value : existingValue.getCommittedValue());
        }
    }
}
