/*
 * Copyright 2004-2018 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.mvstore.tx;

import org.h2.mvstore.CursorPos;
import org.h2.mvstore.MVMap;
import org.h2.mvstore.Page;
import org.h2.mvstore.type.ExtendedDataType;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.Queue;

/**
 * Class CommitProcessor.
 *
 * @author <a href='mailto:andrei.tokar@gmail.com'>Andrei Tokar</a>
 */
final class CommitEntryProcessor extends MVMap.DecisionMaker<VersionedValue>
                            implements MVMap.EntryProcessor<Long, Record>,
                                       MVMap.LeafProcessor
{
    private final TransactionStore transactionStore;
    private final int transactionId;
    private final boolean batchMode;
    private final Map<Integer,BatchInfo> batches = new HashMap<>();

    private long undoKey;
    private MVMap.Decision decision;

    private BatchInfo batchInfo;
    private Object lastKeyOnPage;


    CommitEntryProcessor(TransactionStore transactionStore, int transactionId, boolean batchMode) {
        this.transactionStore = transactionStore;
        this.transactionId = transactionId;
        this.batchMode = batchMode;
    }

    @Override
    public boolean process(Long undoKey, Record existingValue) {
        assert TransactionStore.getTransactionId(undoKey) == transactionId :
                TransactionStore.getTransactionId(undoKey) + " != " + transactionId;

        int mapId = existingValue.mapId;
        MVMap<Object, VersionedValue> map;
        BatchInfo batchInfo = batches.get(mapId);
        if (batchInfo == null) {
            map = transactionStore.openMap(mapId);
            if (map == null || map.isClosed()) {
                batchInfo = BatchInfo.NOOP;
            } else if (!batchMode || map.getType() != null) {
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
            assert prev == null || prev.getOperationId() == 0 ||
                    TransactionStore.getTransactionId(prev.getOperationId()) == transactionId;
            Queue<Object> heap = batchInfo.heap;
            if (heap == null) {
                reset();
                this.undoKey = undoKey;
                map.operate(key, null, this);
            } else if (prev == null || prev.getOperationId() == 0) {
                heap.offer(key);
                if (heap.size() >= 1023) {
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

    @SuppressWarnings("unchecked")
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
        return "final_commit " + TransactionStore.getTransactionId(undoKey);
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
            if (index >= 0 && TransactionStore.getTransactionId(((VersionedValue) leaf.getValue(index)).getOperationId()) == transactionId) {
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
            if (TransactionStore.getTransactionId(existingValue.getOperationId()) == transactionId) {
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
                if (TransactionStore.getTransactionId(existingValue.getOperationId()) == transactionId) {
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
                if (i >= pos.index && TransactionStore.getTransactionId(value.getOperationId()) == transactionId) {
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
        return new Page[]{page};
    }

    @Override
    public void stepBack() {
    }

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
                    while (!heap.isEmpty()) {
                        map.operateBatch(this);
                    }
//                        assert verifyCleanCommit(map);
                }
            }
        }
    }

    private boolean verifyCleanCommit(MVMap<Object, VersionedValue> map) {
        MVMap.process(map.getRootPage(), null, new MVMap.EntryProcessor<Object, VersionedValue>() {
            @Override
            public boolean process(Object key, VersionedValue value) {
                assert TransactionStore.getTransactionId(value.getOperationId()) != transactionId;
                return false;
            }
        });
        return true;
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
}
