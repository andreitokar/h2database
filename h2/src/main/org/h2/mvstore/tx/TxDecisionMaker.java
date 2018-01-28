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
public final class TxDecisionMaker extends MVMap.DecisionMaker<VersionedValue> {
    private final int            mapId;
    private final Object         key;
    private final Object         value;
    private final Transaction    transaction;
    private       long           undoKey;
    private       int            blockingId;
    private       Transaction    blockingTransaction;
    private       MVMap.Decision decision;

    TxDecisionMaker(int mapId, Object key, Object value, Transaction transaction) {
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
    public VersionedValue selectValue(VersionedValue existingValue, VersionedValue providedValue) {
        assert decision == MVMap.Decision.PUT;
        return new VersionedValue.Uncommitted(undoKey, value,
                existingValue == null ? null : existingValue.getOperationId() == 0 ? existingValue.value : existingValue.getCommittedValue());
    }

    @Override
    public void reset() {
        if (decision != MVMap.Decision.ABORT) {
            // map was updated after our decision has been made
            transaction.logUndo();
        }
        blockingTransaction = null;
        decision = null;
    }

    public MVMap.Decision getDecision() {
        return decision;
    }

    Transaction getBlockingTransaction() {
        assert blockingTransaction != null : "Tx " + blockingId +
                " is missing, open: " + transaction.store.openTransactions.get().get(blockingId) +
                ", committing: " + transaction.store.committingTransactions.get().get(blockingId);
        return blockingTransaction;
    }

    @Override
    public String toString() {
        return "tx " + transaction.transactionId;
    }
}
