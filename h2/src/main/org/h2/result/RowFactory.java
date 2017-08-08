/*
 * Copyright 2004-2014 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.result;

import org.h2.engine.Database;
import org.h2.mvstore.db.ValueDataType;
import org.h2.mvstore.type.DataType;
import org.h2.table.Column;
import org.h2.table.CompactRowFactory;
import org.h2.table.IndexColumn;
import org.h2.value.Value;

/**
 * Creates rows.
 *
 * @author Sergi Vladykin
 */
public abstract class RowFactory {

    private static final class Holder {
        static final RowFactory DEFAULT = new DefaultRowFactory();
        static final RowFactory EFFECTIVE = new DefaultRowFactory();
//        static final RowFactory EFFECTIVE = new CompactRowFactory();
    }

    public static RowFactory getDefaultRowFactory() {
        return Holder.DEFAULT;
    }

    public static RowFactory getRowFactory() {
        return Holder.EFFECTIVE;
    }


    public RowFactory createRowFactory(Database db, Column columns[], IndexColumn indexColumns[]) {
        return this;
    }

    /**
     * Create new row.
     *
     * @param data the values
     * @param memory whether the row is in memory
     * @return the created row
     */
    public abstract Row createRow(Value[] data, int memory);

    public abstract SearchRow createRow();

    public abstract DataType getDataType();

    /**
     * Default implementation of row factory.
     */
    private static final class DefaultRowFactory extends RowFactory {
        private final DataType dataType;

        public DefaultRowFactory() {
            this(new ValueDataType(null, null, null));
        }

        private DefaultRowFactory(DataType dataType) {
            this.dataType = dataType;
        }

        @Override
        public RowFactory createRowFactory(Database db, Column[] columns, IndexColumn[] indexColumns) {
            int[] sortTypes = null;
            if (indexColumns != null) {
                int keyColumns = indexColumns.length;
                sortTypes = new int[keyColumns + 1];
                for (int i = 0; i < keyColumns; i++) {
                    sortTypes[i] = indexColumns[i].sortType;
                }
                sortTypes[keyColumns - 1] = SortOrder.ASCENDING;
            }
            return new DefaultRowFactory(new ValueDataType(db.getCompareMode(), db, sortTypes));
        }

        @Override
        public Row createRow(Value[] data, int memory) {
            return new RowImpl(data, memory);
        }

        @Override
        public SearchRow createRow() {
            return null;
        }

        @Override
        public DataType getDataType() {
            return dataType;
        }
    }
}
