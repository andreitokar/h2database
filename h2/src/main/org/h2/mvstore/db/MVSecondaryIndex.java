/*
 * Copyright 2004-2014 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.mvstore.db;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.PriorityQueue;
import java.util.Queue;
import org.h2.api.ErrorCode;
import org.h2.engine.Database;
import org.h2.engine.Session;
import org.h2.index.BaseIndex;
import org.h2.index.Cursor;
import org.h2.index.IndexType;
import org.h2.message.DbException;
import org.h2.mvstore.MVMap;
import org.h2.mvstore.db.TransactionStore.Transaction;
import org.h2.mvstore.db.TransactionStore.TransactionMap;
import org.h2.mvstore.type.DataType;
import org.h2.mvstore.type.ObjectDataType;
import org.h2.result.Row;
import org.h2.result.RowFactory;
import org.h2.result.SearchRow;
import org.h2.result.SortOrder;
import org.h2.table.Column;
import org.h2.table.IndexColumn;
import org.h2.table.TableFilter;
import org.h2.util.New;
import org.h2.value.Value;
import org.h2.value.ValueNull;

/**
 * A table stored in a MVStore.
 */
public final class MVSecondaryIndex extends BaseIndex implements MVIndex {

    /**
     * The multi-value table.
     */
    private final MVTable                         mvTable;
    private final RowFactory rowFactory;
    private final TransactionMap<SearchRow,Value> dataMap;

    public MVSecondaryIndex(Database db, MVTable table, int id, String indexName,
                IndexColumn[] columns, IndexType indexType) {
        this.mvTable = table;
        initBaseIndex(table, id, indexName, columns, indexType);
        if (!database.isStarting()) {
            checkIndexColumnTypes(columns);
        }
        // always store the row key in the map key,
        // even for unique indexes, as some of the index columns could be null
        rowFactory = database.getRowFactory().createRowFactory(db.getCompareMode(), db, table.getColumns(), columns);
        DataType keyType = rowFactory.getDataType();
        DataType valueType = ObjectDataType.NoneType.INSTANCE;
        String mapName = "index." + getId();
        Transaction t = mvTable.getTransactionBegin();
        dataMap = t.openMap(mapName, keyType, valueType);
        t.commit();
        assert mapName.equals(dataMap.getName()) : mapName + " != " + dataMap.getName();
        if (!keyType.equals(dataMap.getKeyType())) {
            throw DbException.throwInternalError("Incompatible key types for '" + mapName + "': " + keyType + " and " + dataMap.getKeyType());
        }
    }

    @Override
    public int compareRows(SearchRow rowData, SearchRow compare) {
//        int expected = super.compareRows(rowData, compare);
        int comp = dataMap.getKeyType().compare(rowData, compare);
//        assert comp == expected : comp + " != " + expected;
        return comp;
    }

    @Override
    public void addRowsToBuffer(List<Row> rows, String bufferName) {
        MVMap<SearchRow,Value> map = openMap(bufferName);
        MVMap.BufferingAgent<SearchRow, Value> agent = map.getBufferingAgent();
        for (Row row : rows) {
            SearchRow r = rowFactory.createRow();
            r.copyFrom(row);
            agent.put(r, ValueNull.INSTANCE);
//            map.put(r, ValueNull.INSTANCE);
        }
        agent.close();
    }

    private static final class Source {

        private final Iterator<SearchRow> iterator;
        private       SearchRow           currentRowData;

        public Source(Iterator<SearchRow> iterator) {
            assert iterator.hasNext();
            this.iterator = iterator;
            this.currentRowData = iterator.next();
        }

        public boolean hasNext() {
            boolean result = iterator.hasNext();
            if(result) {
                currentRowData = iterator.next();
            }
            return result;
        }

        public SearchRow next() {
            return currentRowData;
        }

        public static final class Comparator implements java.util.Comparator<Source> {

            private final DataType type;

            public Comparator(DataType type) {
                this.type = type;
            }

            @Override
            public int compare(Source one, Source two) {
                return type.compare(one.currentRowData, two.currentRowData);
            }
        }
    }

    @Override
    public void addBufferedRows(List<String> bufferNames) {
        MVMap.BufferingAgent<SearchRow,Value> agent = dataMap.getBufferingAgent();
        ArrayList<String> mapNames = New.arrayList(bufferNames);
        int buffersCount = bufferNames.size();
        Queue<Source> queue = new PriorityQueue<>(buffersCount,
                                new Source.Comparator(rowFactory.getDataType()));
        for (String bufferName : bufferNames) {
            Iterator<SearchRow> iter = openMap(bufferName).keyIterator(null);
            if (iter.hasNext()) {
                queue.add(new Source(iter));
            }
        }
        try {
            while (!queue.isEmpty()) {
                Source s = queue.remove();
                SearchRow row = s.next();

                if (indexType.isUnique()) {
                    checkUnique(dataMap, row, row.getKey());
                }

                agent.put(row, ValueNull.INSTANCE);

                if (s.hasNext()) {
                    queue.offer(s);
                }
            }
        } finally {
            agent.close();
            for (String tempMapName : mapNames) {
                MVMap<SearchRow,Value> map = openMap(tempMapName);
                map.getStore().removeMap(map);
            }
        }
    }

    private MVMap<SearchRow,Value> openMap(String mapName) {
        DataType keyType = rowFactory.getDataType();
        DataType valueType = ObjectDataType.NoneType.INSTANCE;
        MVMap.Builder<SearchRow,Value> builder = new MVMap.Builder<SearchRow,Value>()
                                                .keyType(keyType)
                                                .valueType(valueType);
        MVMap<SearchRow, Value> map = database.getMvStore().getStore()
                .openMap(mapName, builder);
        if (!keyType.equals(map.getKeyType())) {
            throw DbException.throwInternalError("Incompatible key type");
        }
        return map;
    }

    @Override
    public void close(Session session) {
        // ok
    }

    @Override
    public void add(Session session, Row row) {
        TransactionMap<SearchRow,Value> map = getMap(session);
        SearchRow key = convertToKey(row, null);
        if (indexType.isUnique()) {
            checkUnique(map, row, Long.MIN_VALUE);
        }
        try {
            map.put(key, ValueNull.INSTANCE);
        } catch (IllegalStateException e) {
            throw mvTable.convertException(e);
        }
        if (indexType.isUnique()) {
            checkUnique(map, row, row.getKey());
        }
    }

    private void checkUnique(TransactionMap<SearchRow,Value> map, SearchRow row, long newKey) {
        Iterator<SearchRow> it = map.keyIterator(convertToKey(row, Boolean.FALSE), convertToKey(row, Boolean.TRUE), true);
        while (it.hasNext()) {
            SearchRow k = it.next();

            if (containsNullAndAllowMultipleNull(k)) {
                // this is allowed
                continue;
            }
            if (map.isSameTransaction(k)) {
                if (newKey == k.getKey()) {
                    continue;
                }
            }
            if (map.get(k) != null) {
                // committed
                throw getDuplicateKeyException(k.toString());
            }
            throw DbException.get(ErrorCode.CONCURRENT_UPDATE_1, table.getName());
        }
    }

    @Override
    public void remove(Session session, Row row) {
        SearchRow searchRow = convertToKey(row, null);
        TransactionMap<SearchRow,Value> map = getMap(session);
        try {
            if (map.remove(searchRow) == null) {
                throw DbException.get(ErrorCode.ROW_NOT_FOUND_WHEN_DELETING_1,
                        getSQL() + ": " + row.getKey());
            }
        } catch (IllegalStateException e) {
            throw mvTable.convertException(e);
        }
    }

    @Override
    public Cursor find(Session session, SearchRow first, SearchRow last) {
        return find(session, first, false, last);
    }

    private Cursor find(Session session, SearchRow first, boolean bigger, SearchRow last) {
        SearchRow min = convertToKey(first, bigger);
        SearchRow max = convertToKey(last, Boolean.TRUE);
        TransactionMap<SearchRow,Value> map = getMap(session);
        return new MVStoreCursor(session, map.keyIterator(min, max, false));
    }

    private SearchRow convertToKey(SearchRow r, Boolean minmax) {
        if (r == null) {
            return null;
        }

        SearchRow row = rowFactory.createRow();
        row.copyFrom(r);
        if (minmax != null) {
            row.setKey(minmax ? Long.MAX_VALUE : Long.MIN_VALUE);
        }
        return row;
    }

    @Override
    public MVTable getTable() {
        return mvTable;
    }

    @Override
    public double getCost(Session session, int[] masks,
            TableFilter[] filters, int filter, SortOrder sortOrder,
            HashSet<Column> allColumnsSet) {
        try {
            return 10 * getCostRangeIndex(masks, dataMap.sizeAsLongMax(),
                    filters, filter, sortOrder, false, allColumnsSet);
        } catch (IllegalStateException e) {
            throw DbException.get(ErrorCode.OBJECT_CLOSED, e);
        }
    }

    @Override
    public void remove(Session session) {
        TransactionMap<SearchRow,Value> map = getMap(session);
        if (!map.isClosed()) {
            Transaction t = session.getTransaction();
            t.removeMap(map);
        }
    }

    @Override
    public void truncate(Session session) {
        TransactionMap<SearchRow,Value> map = getMap(session);
        map.clear();
    }

    @Override
    public boolean canGetFirstOrLast() {
        return true;
    }

    @Override
    public Cursor findFirstOrLast(Session session, boolean first) {
        TransactionMap<SearchRow,Value> map = getMap(session);
        SearchRow key = first ? map.firstKey() : map.lastKey();
        while (true) {
            if (key == null) {
                return new MVStoreCursor(session,
                        Collections.<SearchRow>emptyList().iterator());
            }
            if (key.getValue(columnIds[0]) != ValueNull.INSTANCE) {
                break;
            }
            key = first ? map.higherKey(key) : map.lowerKey(key);
        }
        List<SearchRow> list = Collections.singletonList(key);
        MVStoreCursor cursor = new MVStoreCursor(session, list.iterator());
        cursor.next();
        return cursor;
    }

    @Override
    public boolean needRebuild() {
        try {
            return dataMap.sizeAsLongMax() == 0;
        } catch (IllegalStateException e) {
            throw DbException.get(ErrorCode.OBJECT_CLOSED, e);
        }
    }

    @Override
    public long getRowCount(Session session) {
        TransactionMap<SearchRow,Value> map = getMap(session);
        return map.sizeAsLong();
    }

    @Override
    public long getRowCountApproximation() {
        try {
            return dataMap.sizeAsLongMax();
        } catch (IllegalStateException e) {
            throw DbException.get(ErrorCode.OBJECT_CLOSED, e);
        }
    }

    @Override
    public long getDiskSpaceUsed() {
        // TODO estimate disk space usage
        return 0;
    }

    @Override
    public boolean canFindNext() {
        return true;
    }

    @Override
    public Cursor findNext(Session session, SearchRow higherThan, SearchRow last) {
        return find(session, higherThan, true, last);
    }

    @Override
    public void checkRename() {
        // ok
    }

    /**
     * Get the map to store the data.
     *
     * @param session the session
     * @return the map
     */
    private TransactionMap<SearchRow,Value> getMap(Session session) {
        if (session == null) {
            return dataMap;
        }
        Transaction t = session.getTransaction();
        return dataMap.getInstance(t);
    }

    /**
     * A cursor.
     */
    final class MVStoreCursor implements Cursor {

        private final Session             session;
        private final Iterator<SearchRow> it;
        private       SearchRow           current;
        private       Row                 row;

        private MVStoreCursor(Session session, Iterator<SearchRow> it) {
            this.session = session;
            this.it = it;
        }

        @Override
        public Row get() {
            if (row == null) {
                SearchRow r = getSearchRow();
                if (r != null) {
                    row = mvTable.getRow(session, r.getKey());
                }
            }
            return row;
        }

        @Override
        public SearchRow getSearchRow() {
            return current;
        }

        @Override
        public boolean next() {
            current = it.hasNext() ? it.next() : null;
            row = null;
            return current != null;
        }

        @Override
        public boolean previous() {
            throw DbException.getUnsupportedException("previous");
        }
    }

}
