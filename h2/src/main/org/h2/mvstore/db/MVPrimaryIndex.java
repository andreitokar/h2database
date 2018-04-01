/*
 * Copyright 2004-2018 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.mvstore.db;

import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;
import java.util.concurrent.atomic.AtomicLong;
import org.h2.api.ErrorCode;
import org.h2.engine.Database;
import org.h2.engine.Session;
import org.h2.index.BaseIndex;
import org.h2.index.Cursor;
import org.h2.index.IndexType;
import org.h2.index.SingleRowCursor;
import org.h2.message.DbException;
import org.h2.mvstore.DataUtils;
import org.h2.mvstore.tx.Transaction;
import org.h2.mvstore.tx.TransactionMap;
import org.h2.mvstore.type.DataType;
import org.h2.mvstore.type.LongDataType;
import org.h2.result.Row;
import org.h2.result.SearchRow;
import org.h2.result.SortOrder;
import org.h2.table.Column;
import org.h2.table.IndexColumn;
import org.h2.table.TableFilter;
import org.h2.value.Value;
import org.h2.value.ValueArray;
import org.h2.value.ValueLong;
import org.h2.value.ValueNull;

/**
 * A table stored in a MVStore.
 */
public final class MVPrimaryIndex extends BaseIndex
{
    private final MVTable                  mvTable;
    private final String                   mapName;
    private final TransactionMap<Long,Row> dataMap;
    private final AtomicLong               lastKey = new AtomicLong(0);
    private       int                      mainIndexColumn = SearchRow.ROWID_INDEX;

    public MVPrimaryIndex(Database db, MVTable table, int id,
            IndexColumn[] columns, IndexType indexType) {
        this.mvTable = table;
        initBaseIndex(table, id, table.getName() + "_DATA", columns, indexType);
        DataType valueType = table.getRowFactory().getDataType();
        mapName = "table." + getId();
        Transaction t = mvTable.getTransactionBegin();
        dataMap = t.openMap(mapName, LongDataType.INSTANCE, valueType);
        t.commit();
        if (!table.isPersistData() || !indexType.isPersistent()) {
            dataMap.map.setVolatile(true);
        }
        Long k = dataMap.map.lastKey(); // include uncommitted keys
        lastKey.set(k == null ? 0 : k);
    }

    @Override
    public String getCreateSQL() {
        return null;
    }

    @Override
    public String getPlanSQL() {
        return table.getSQL() + ".tableScan";
    }

    public void setMainIndexColumn(int mainIndexColumn) {
        this.mainIndexColumn = mainIndexColumn;
    }

    public int getMainIndexColumn() {
        return mainIndexColumn;
    }

    @Override
    public void close(Session session) {
        // ok
    }

    @Override
    public int compareRows(SearchRow rowOne, SearchRow rowTwo) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void add(Session session, Row row) {
        if (mainIndexColumn == SearchRow.ROWID_INDEX) {
            if (row.getKey() == 0) {
                row.setKey(lastKey.incrementAndGet());
            }
        } else {
            long c = row.getValue(mainIndexColumn).getLong();
            row.setKey(c);
        }

        if (mvTable.getContainsLargeObject()) {
            for (int i = 0, len = row.getColumnCount(); i < len; i++) {
                Value v = row.getValue(i);
                Value v2 = v.copy(database, getId());
                if (v2.isLinkedToTable()) {
                    session.removeAtCommitStop(v2);
                }
                if (v != v2) {
                    row.setValue(i, v2);
                }
            }
        }

        TransactionMap<Long,Row> map = getMap(session);
        long rowKey = row.getKey();
        try {
            Row old = map.putIfAbsent(rowKey, row);
            if (old != null) {
                String sql = "PRIMARY KEY ON " + table.getSQL();
                if (mainIndexColumn >= 0 && mainIndexColumn < indexColumns.length) {
                    sql += "(" + indexColumns[mainIndexColumn].getSQL() + ")";
                }
                int errorCode = ErrorCode.CONCURRENT_UPDATE_1;
                if (map.get(rowKey) != null) {
                    // committed
                    errorCode = ErrorCode.DUPLICATE_KEY_1;
                }
                DbException e = DbException.get(errorCode, sql);
                e.setSource(this);
                throw e;
            }
        } catch (IllegalStateException e) {
            throw mvTable.convertException(e);
        }
        // because it's possible to directly update the key using the _rowid_
        // syntax
        long last;
        while (rowKey > (last = lastKey.get())) {
            if(lastKey.compareAndSet(last, rowKey)) break;
        }
    }

    @Override
    public void remove(Session session, Row row) {
        removeRow(session, row);
    }

    @Override
    public Row removeRow(Session session, Row row) {
        if (mvTable.getContainsLargeObject()) {
            for (int i = 0, len = row.getColumnCount(); i < len; i++) {
                Value v = row.getValue(i);
                if (v.isLinkedToTable()) {
                    session.removeAtCommit(v);
                }
            }
        }
        TransactionMap<Long,Row> map = getMap(session);
        try {
            Row result = map.remove(row.getKey());
//            if (result == null) {
//                throw DbException.get(ErrorCode.ROW_NOT_FOUND_WHEN_DELETING_1,
//                        getSQL() + ": " + row.getKey());
//            }
            return result;
        } catch (IllegalStateException e) {
            throw mvTable.convertException(e);
        }
    }

    @Override
    public void update(Session session, Row oldRow, Row newRow) {
        if (mainIndexColumn != SearchRow.ROWID_INDEX) {
            long c = newRow.getValue(mainIndexColumn).getLong();
            newRow.setKey(c);
        }
        long key = oldRow.getKey();
        assert mainIndexColumn != SearchRow.ROWID_INDEX || key != 0;
        if(newRow.getKey() != key) {
            super.update(session, oldRow, newRow);
        } else {
            if (mvTable.getContainsLargeObject()) {
                for (int i = 0, len = oldRow.getColumnCount(); i < len; i++) {
                    Value oldValue = oldRow.getValue(i);
                    Value newValue = newRow.getValue(i);
                    if(oldValue != newValue) {
                        if (oldValue.isLinkedToTable()) {
                            session.removeAtCommit(oldValue);
                        }
                        Value v2 = newValue.copy(database, getId());
                        if (v2.isLinkedToTable()) {
                            session.removeAtCommitStop(v2);
                        }
                        if (newValue != v2) {
                            newRow.setValue(i, v2);
                        }
                    }
                }
            }

            TransactionMap<Long,Row> map = getMap(session);
            try {
                Row old = map.put(key, newRow);
                if (old == null) {
                    throw DbException.get(ErrorCode.ROW_NOT_FOUND_WHEN_DELETING_1,
                            getSQL() + ": " + key);
                }
            } catch (IllegalStateException e) {
                throw mvTable.convertException(e);
            }
        }


        // because it's possible to directly update the key using the _rowid_
        // syntax
        if (newRow.getKey() > lastKey.get()) {
            lastKey.set(newRow.getKey());
        }
    }

    public void lockRows(Session session, Iterator<Row> rowsForUpdate) {
        TransactionMap<Long, Row> map = getMap(session);
        while (rowsForUpdate.hasNext()) {
            Row row = rowsForUpdate.next();
            long key = row.getKey();
            try {
                map.lock(key);
            } catch (IllegalStateException ex) {
                throw mvTable.convertException(ex);
            }
        }
    }

    @Override
    public Cursor find(Session session, SearchRow first, SearchRow last) {
        Long min = extractPKFromRow(first, Long.MIN_VALUE);
        Long max = extractPKFromRow(last, Long.MAX_VALUE);
        TransactionMap<Long,Row> map = getMap(session);
        return new MVStoreCursor(map.entryIterator(min, max));
    }

    private Long extractPKFromRow(SearchRow row, long defaultValue) {
        Long result;
        if (row == null) {
            result = defaultValue;
        } else if (mainIndexColumn == SearchRow.ROWID_INDEX) {
            result = row.getKey();
        } else {
            Value v = row.getValue(mainIndexColumn);
            if (v == null) {
                result = row.getKey();
            } else {
                result = v.getLong();
            }
        }
        return result;
    }

    @Override
    public MVTable getTable() {
        return mvTable;
    }

    @Override
    public Row getRow(Session session, long key) {
        TransactionMap<Long,Row> map = getMap(session);
        Row row = map.get(key);
        if (row == null) {
            throw DbException.get(ErrorCode.ROW_NOT_FOUND_IN_PRIMARY_INDEX,
                    getSQL(), String.valueOf(key));
        }
        return row;
    }

    @Override
    public double getCost(Session session, int[] masks,
            TableFilter[] filters, int filter, SortOrder sortOrder,
            HashSet<Column> allColumnsSet) {
        try {
            return 10 * getCostRangeIndex(masks, dataMap.sizeAsLongMax(),
                    filters, filter, sortOrder, true, allColumnsSet);
        } catch (IllegalStateException e) {
            throw DbException.get(ErrorCode.OBJECT_CLOSED, e);
        }
    }

    @Override
    public int getColumnIndex(Column col) {
        // can not use this index - use the delegate index instead
        return -1;
    }

    @Override
    public boolean isFirstColumn(Column column) {
        return false;
    }

    @Override
    public void remove(Session session) {
        TransactionMap<Long,Row> map = getMap(session);
        if (!map.isClosed()) {
            Transaction t = session.getTransaction();
            t.removeMap(map);
        }
    }

    @Override
    public void truncate(Session session) {
        TransactionMap<Long,Row> map = getMap(session);
        if (mvTable.getContainsLargeObject()) {
            database.getLobStorage().removeAllForTable(table.getId());
        }
        map.clear();
    }

    @Override
    public boolean canGetFirstOrLast() {
        return true;
    }

    @Override
    public Cursor findFirstOrLast(Session session, boolean first) {
        TransactionMap<Long,Row> map = getMap(session);
        Long rowId = first ? map.firstKey() : map.lastKey();
        if (rowId == null) {
            return new MVStoreCursor(Collections.<Entry<Long,Row>> emptyList().iterator());
        }
        Row value = map.get(rowId);
        Entry<Long,Row> e = new DataUtils.MapEntry<>(rowId, value);
        List<Entry<Long,Row>> list = Collections.singletonList(e);
        MVStoreCursor c = new MVStoreCursor(list.iterator());
        c.next();
        return c;
    }

    @Override
    public boolean needRebuild() {
        return false;
    }

    @Override
    public long getRowCount(Session session) {
        TransactionMap<Long,Row> map = getMap(session);
        return map.sizeAsLong();
    }

    /**
     * The maximum number of rows, including uncommitted rows of any session.
     *
     * @return the maximum number of rows
     */
    public long getRowCountMax() {
        try {
            return dataMap.sizeAsLongMax();
        } catch (IllegalStateException e) {
            throw DbException.get(ErrorCode.OBJECT_CLOSED, e);
        }
    }

    @Override
    public long getRowCountApproximation() {
        return getRowCountMax();
    }

    @Override
    public long getDiskSpaceUsed() {
        // TODO estimate disk space usage
        return 0;
    }

    public String getMapName() {
        return mapName;
    }

    @Override
    public void checkRename() {
        // ok
    }

    /**
     * Get the key from the row.
     *
     * @param row the row
     * @param ifEmpty the value to use if the row is empty
     * @param ifNull the value to use if the column is NULL
     * @return the key
     */
    ValueLong getKey(SearchRow row, ValueLong ifEmpty, ValueLong ifNull) {
        if (row == null) {
            return ifEmpty;
        }
        Value v = row.getValue(mainIndexColumn);
        if (v == null) {
            throw DbException.throwInternalError(row.toString());
        } else if (v == ValueNull.INSTANCE) {
            return ifNull;
        }
        return (ValueLong) v.convertTo(Value.LONG);
    }

    /**
     * Search for a specific row or a set of rows.
     *
     * @param session the session
     * @param first the key of the first row
     * @param last the key of the last row
     * @return the cursor
     */
    Cursor find(Session session, ValueLong first, ValueLong last) {
        TransactionMap<Long,Row> map = getMap(session);
        if (first != null && last != null && first.getLong() == last.getLong()) {
            long key = first.getLong();
            Row row = map.get(key);
            return new SingleRowCursor(row);
        }
        return new MVStoreCursor(map.entryIterator(first == null ? null : first.getLong(),
                                                   last == null ? null : last.getLong()));
    }

    @Override
    public boolean isRowIdIndex() {
        return true;
    }

    /**
     * Get the map to store the data.
     *
     * @param session the session
     * @return the map
     */
    private TransactionMap<Long,Row> getMap(Session session) {
        if (session == null) {
            return dataMap;
        }
        Transaction t = session.getTransaction();
        return dataMap.getInstance(t);
    }

    public static Row convertValueToRow(long key, Object value, MVTable mvTable) {
        Row result;
        if(value instanceof Row) {
            result = (Row) value;
            assert result.getKey() == key
                 : result.getKey() + " != " + key;
        } else {
            ValueArray array = (ValueArray) value;
            result = mvTable.createRow(array.getList(), 0);
            result.setKey(key);
        }
        return result;
    }


    /**
     * A cursor.
     */
    static final class MVStoreCursor implements Cursor {

        private final Iterator<Entry<Long,Row>> it;
        private       Entry<Long,Row>           current;
        private       Row                       row;

        private MVStoreCursor(Iterator<Entry<Long, Row>> it) {
            this.it = it;
        }

        @Override
        public Row get() {
            if (row == null) {
                if (current != null) {
                    row = current.getValue();
                }
            }
            return row;
        }

        @Override
        public SearchRow getSearchRow() {
            return get();
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
