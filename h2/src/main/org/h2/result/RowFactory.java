/*
 * Copyright 2004-2018 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.result;

import org.h2.mvstore.RowDataType;
import org.h2.mvstore.type.DataType;
import org.h2.store.DataHandler;
import org.h2.table.Column;
import org.h2.table.CompactRowFactory;
import org.h2.table.IndexColumn;
import org.h2.value.CompareMode;
import org.h2.value.Value;

/**
 * Creates rows.
 *
 * @author Sergi Vladykin
 */
public abstract class RowFactory {

    private static final class Holder {
        private static final RowFactory EFFECTIVE = DefaultRowFactory.INSTANCE;
//        private static final RowFactory EFFECTIVE = new CompactRowFactory();
    }

    public static DefaultRowFactory getDefaultRowFactory() {
        return DefaultRowFactory.INSTANCE;
    }

    public static RowFactory getRowFactory() {
        return Holder.EFFECTIVE;
    }


    public RowFactory createRowFactory(CompareMode compareMode, DataHandler handler,
                                       Column[] columns, IndexColumn[] indexColumns) {
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
    public static final class DefaultRowFactory extends RowFactory {
        private final RowDataType dataType;
        private final int         columnCount;
        private final int         map[];

        public static final DefaultRowFactory INSTANCE = new DefaultRowFactory();

        protected DefaultRowFactory() {
            this(new RowDataType(null, null, null, null), 0, null);
        }

        private DefaultRowFactory(RowDataType dataType, int columnCount, int indexes[]) {
            this.dataType = dataType;
            this.columnCount = columnCount;
            this.map = indexes == null ? null : new int[columnCount];
            if(indexes != null) {
                for (int i = 0; i < indexes.length; i++) {
                    map[indexes[i]] = i + 1;
                }
            }
        }

        @Override
        public RowFactory createRowFactory(CompareMode compareMode, DataHandler handler,
                                           Column[] columns, IndexColumn[] indexColumns) {
            int indexes[] = null;
            int sortTypes[];
            int columnCount = columns.length;
            if (indexColumns == null) {
                sortTypes = new int[columnCount];
                for (int i = 0; i < columnCount; i++) {
                    sortTypes[i] = SortOrder.ASCENDING;
                }
            } else {
                int len = indexColumns.length;
                indexes = new int[len];
                sortTypes = new int[len];
                for (int i = 0; i < len; i++) {
                    IndexColumn indexColumn = indexColumns[i];
                    indexes[i] = indexColumn.column.getColumnId();
                    sortTypes[i] = indexColumn.sortType;
                }
            }
            return createRowFactory(compareMode, handler, sortTypes, indexes, columnCount);
        }

        public RowFactory createRowFactory(CompareMode compareMode, DataHandler handler,
                                           int[] sortTypes, int[] indexes, int columnCount) {
            RowDataType dataType = new RowDataType(compareMode, handler, sortTypes, indexes);
            DefaultRowFactory defaultRowFactory = new DefaultRowFactory(dataType, columnCount, indexes);
            dataType.setRowFactory(defaultRowFactory);
            return defaultRowFactory;
        }

        @Override
        public Row createRow(Value[] data, int memory) {
            return new RowImpl(data, memory);
        }

        @Override
        public SearchRow createRow() {
            int[] indexes = dataType.getIndexes();
            if (indexes == null) {
                return new RowImpl(columnCount);
            } else if (indexes.length == 1) {
                return new SimpleRowValue(columnCount, indexes[0]);
            } else {
                return new SimpleRow.Sparse(columnCount, indexes.length, map);
            }
        }

        @Override
        public DataType getDataType() {
            return dataType;
        }
    }
}
