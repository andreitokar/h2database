/*
 * Copyright 2004-2018 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.result;

import org.h2.engine.Constants;
import org.h2.util.StatementBuilder;
import org.h2.value.Value;
import org.h2.value.ValueLong;
import org.h2.value.ValueNull;

/**
 * Represents a simple row without state.
 */
public class SimpleRow implements SearchRow {

    private long key;
    private int version;
    private final Value[] data;
    private int memory;

    public SimpleRow(Value[] data) {
        this.data = data;
    }

    @Override
    public int getColumnCount() {
        return data.length;
    }

    @Override
    public long getKey() {
        return key;
    }

    @Override
    public void setKey(long key) {
        this.key = key;
    }

    @Override
    public void setKeyAndVersion(SearchRow row) {
        key = row.getKey();
        version = row.getVersion();
    }

    @Override
    public int getVersion() {
        return version;
    }

    @Override
    public void setValue(int i, Value v) {
        if (i == ROWID_INDEX) {
            key = v.getLong();
        } else {
            data[i] = v;
        }
    }

    @Override
    public Value getValue(int i) {
        return i == ROWID_INDEX ? ValueLong.get(key) : data[i];
    }

    @Override
    public String toString() {
        StatementBuilder buff = new StatementBuilder("( /* key:");
        buff.append(getKey());
        if (version != 0) {
            buff.append(" v:" + version);
        }
        buff.append(" */ ");
        for (Value v : data) {
            buff.appendExceptFirst(", ");
            buff.append(v == null ? "null" : v.getTraceSQL());
        }
        return buff.append(')').toString();
    }

    @Override
    public int getMemory() {
        if (memory == 0) {
            memory = Constants.MEMORY_OBJECT + getColumnCount() * Constants.MEMORY_POINTER;
            for (Value value : data) {
                if (value != null) {
                    memory += value.getMemory();
                }
            }
        }
        return memory;
    }

    @Override
    public boolean isNull(int indx) {
        return data[indx] == null || data[indx] == ValueNull.INSTANCE;
    }

    @Override
    public void copyFrom(SearchRow source) {
        setKey(source.getKey());
        for (int i = 0; i < getColumnCount(); i++) {
            setValue(i, source.getValue(i));
        }
    }

    public static final class Sparse extends SimpleRow {
        private final int columnCount;
        private final int map[];

        public Sparse(int columnCount, int capacity, int[] map) {
            super(new Value[capacity]);
            this.columnCount = columnCount;
            this.map = map;
        }

        @Override
        public int getColumnCount() {
            return columnCount;
        }

        @Override
        public Value getValue(int i) {
            if (i == ROWID_INDEX) {
                return ValueLong.get(getKey());
            }
            int indx = map[i];
            return indx > 0 ? super.getValue(indx - 1) : null;
        }

        @Override
        public void setValue(int i, Value v) {
            if (i == ROWID_INDEX) {
                setKey(v.getLong());
            }
            int indx = map[i];
            if (indx > 0) {
                super.setValue(indx - 1, v);
            }
        }

        @Override
        public boolean isNull(int i) {
            int indx = map[i];
            return indx <= 0 || super.isNull(indx - 1);
        }
    }
}
