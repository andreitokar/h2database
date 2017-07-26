/*
 * Copyright 2004-2014 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.value;

import java.io.InputStream;
import java.io.Reader;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Time;
import java.sql.Timestamp;
import org.h2.engine.Mode;
import org.h2.message.DbException;
import org.h2.table.Column;
import org.h2.mvstore.WriteBuffer;
import org.h2.mvstore.db.ValueDataType;
import org.h2.mvstore.type.ExtendedDataType;
import org.h2.result.SortOrder;
import org.h2.store.DataHandler;

/**
 * Implementation of NULL. NULL is not a regular data type.
 */
public class ValueNull extends Value {

    /**
     * The main NULL instance.
     */
    public static final ValueNull INSTANCE = new ValueNull();

    /**
     * This special instance is used as a marker for deleted entries in a map.
     * It should not be used anywhere else.
     */
    public static final ValueNull DELETED = new ValueNull();

    /**
     * The precision of NULL.
     */
    private static final int PRECISION = 1;

    /**
     * The display size of the textual representation of NULL.
     */
    private static final int DISPLAY_SIZE = 4;

    private ValueNull() {
        // don't allow construction
    }

    @Override
    public String getSQL() {
        return "NULL";
    }

    @Override
    public int getType() {
        return Value.NULL;
    }

    @Override
    public String getString() {
        return null;
    }

    @Override
    public Boolean getBoolean() {
        return null;
    }

    @Override
    public Date getDate() {
        return null;
    }

    @Override
    public Time getTime() {
        return null;
    }

    @Override
    public Timestamp getTimestamp() {
        return null;
    }

    @Override
    public byte[] getBytes() {
        return null;
    }

    @Override
    public byte getByte() {
        return 0;
    }

    @Override
    public short getShort() {
        return 0;
    }

    @Override
    public BigDecimal getBigDecimal() {
        return null;
    }

    @Override
    public double getDouble() {
        return 0.0;
    }

    @Override
    public float getFloat() {
        return 0.0F;
    }

    @Override
    public int getInt() {
        return 0;
    }

    @Override
    public long getLong() {
        return 0;
    }

    @Override
    public InputStream getInputStream() {
        return null;
    }

    @Override
    public Reader getReader() {
        return null;
    }

    @Override
    public Value convertTo(int type, int precision, Mode mode, Column column) {
        return this;
    }

    @Override
    protected int compareSecure(Value v, CompareMode mode) {
        throw DbException.throwInternalError("compare null");
    }

    @Override
    public long getPrecision() {
        return PRECISION;
    }

    @Override
    public int hashCode() {
        return 0;
    }

    @Override
    public Object getObject() {
        return null;
    }

    @Override
    public void set(PreparedStatement prep, int parameterIndex)
            throws SQLException {
        prep.setNull(parameterIndex, DataType.convertTypeToSQLType(Value.NULL));
    }

    @Override
    public int getDisplaySize() {
        return DISPLAY_SIZE;
    }

    @Override
    public boolean equals(Object other) {
        return other == this;
    }


    public static final class Type extends ValueDataType implements ExtendedDataType {

        public static final Type INSTANCE = new Type(null, null, null);

        public Type(CompareMode compareMode, DataHandler handler, int[] sortTypes) {
            super(compareMode, handler, sortTypes);
        }

        @Override
        public Object createStorage(int size) {
            return null;
        }

        @Override
        public Object clone(Object storage) {
            return null;
        }

        @Override
        public int getLength(Object storage) {
            return 0;
        }

        @Override
        public Object getValue(Object storage, int indx) {
            return ValueNull.INSTANCE;
        }

        @Override
        public void setValue(Object storage, int indx, Object value) {
        }

        @Override
        public int getMemorySize(Object storage) {
            return 0;
        }

        @Override
        public int binarySearch(Object what, Object storage, int initialGuess) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void writeStorage(WriteBuffer buff, Object storage) {}

        @Override
        public void read(ByteBuffer buff, Object storage) {}
    }
}
