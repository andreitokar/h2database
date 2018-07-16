/*
 * Copyright 2004-2018 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.mvstore.db;

import java.util.Arrays;

import org.h2.engine.Database;
import org.h2.expression.Expression;
import org.h2.message.DbException;
import org.h2.mvstore.Cursor;
import org.h2.mvstore.MVMap;
import org.h2.mvstore.MVMap.Builder;
import org.h2.mvstore.type.DataType;
import org.h2.mvstore.type.LongDataType;
import org.h2.result.ResultExternal;
import org.h2.value.Value;
import org.h2.value.ValueArray;

/**
 * Plain temporary result.
 */
class MVPlainTempResult extends MVTempResult {

    /**
     * The type of the distinct values.
     */
    private final ValueDataType distinctType;

    /**
     * The type of the values in the main map and keys in the index.
     */
    private final ValueDataType valueType;

    /**
     * Map with identities of rows as keys rows as values.
     */
    private final MVMap<Long, ValueArray> map;

    /**
     * Counter for the identities of rows. A separate counter is used instead of
     * {@link #rowCount} because rows due to presence of {@link #removeRow(Value[])}
     * method to ensure that each row will have an own identity.
     */
    private long counter;

    /**
     * Optional index. This index is created only if {@link #contains(Value[])}
     * method is invoked. Only the root result should have an index if required.
     */
    private MVMap<ValueArray, Boolean> index;

    /**
     * Cursor for the {@link #next()} method.
     */
    private Cursor<Long, ValueArray> cursor;

    /**
     * Creates a shallow copy of the result.
     *
     * @param parent
     *                   parent result
     */
    private MVPlainTempResult(MVPlainTempResult parent) {
        super(parent);
        this.distinctType = null;
        this.valueType = null;
        this.map = parent.map;
    }

    /**
     * Creates a new plain temporary result.
     *
     * @param database
     *            database
     * @param expressions
     *            column expressions
     * @param visibleColumnCount
     *            count of visible columns
     */
    MVPlainTempResult(Database database, Expression[] expressions, int visibleColumnCount) {
        super(database, expressions.length, visibleColumnCount);
        DataType keyType = LongDataType.INSTANCE;
        valueType = new ValueDataType(database.getCompareMode(), database.getMode(),
                                        database, new int[expressions.length]);
        if (columnCount == visibleColumnCount) {
            distinctType = valueType;
        } else {
            distinctType = new ValueDataType(database.getCompareMode(), database.getMode(), database, new int[visibleColumnCount]);
        }
        Builder<Long, ValueArray> builder = new MVMap.Builder<Long, ValueArray>().keyType(keyType)
                .valueType(valueType);
        map = store.openMap("tmp", builder);
    }

    @Override
    public int addRow(Value[] values) {
        assert parent == null && index == null;
        map.put(counter++, ValueArray.get(values));
        return ++rowCount;
    }

    @Override
    public boolean contains(Value[] values) {
        // Only parent result maintains the index
        if (parent != null) {
            return parent.contains(values);
        }
        if (index == null) {
            createIndex();
        }
        return index.containsKey(ValueArray.get(values));
    }

    private void createIndex() {
        Builder<ValueArray, Boolean> builder = new MVMap.Builder<ValueArray, Boolean>().keyType(distinctType);
        index = store.openMap("idx", builder);
        Cursor<Long, ValueArray> c = map.cursor(null);
        while (c.hasNext()) {
            c.next();
            ValueArray row = c.getValue();
            if (columnCount != visibleColumnCount) {
                row = ValueArray.get(Arrays.copyOf(row.getList(), visibleColumnCount));
            }
            index.putIfAbsent(row, true);
        }
    }

    @Override
    public synchronized ResultExternal createShallowCopy() {
        if (parent != null) {
            return parent.createShallowCopy();
        }
        if (closed) {
            return null;
        }
        childCount++;
        return new MVPlainTempResult(this);
    }

    @Override
    public Value[] next() {
        if (cursor == null) {
            cursor = map.cursor(null);
        }
        if (!cursor.hasNext()) {
            return null;
        }
        cursor.next();
        return cursor.getValue().getList();
    }

    @Override
    public int removeRow(Value[] values) {
        throw DbException.getUnsupportedException("removeRow()");
    }

    @Override
    public void reset() {
        cursor = null;
    }

}
