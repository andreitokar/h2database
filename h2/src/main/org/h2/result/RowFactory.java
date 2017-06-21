/*
 * Copyright 2004-2014 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.result;

import org.h2.table.Column;
import org.h2.table.CompactRowFactory;
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


    public RowFactory createRowFactory(Column columns[]) {
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

    /**
     * Default implementation of row factory.
     */
    private static final class DefaultRowFactory extends RowFactory {
        @Override
        public Row createRow(Value[] data, int memory) {
            return new RowImpl(data, memory);
        }
    }
}
