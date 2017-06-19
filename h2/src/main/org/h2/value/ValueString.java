/*
 * Copyright 2004-2014 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.value;

import java.nio.ByteBuffer;
import java.sql.PreparedStatement;
import java.sql.SQLException;

import com.sun.tools.internal.jxc.ap.Const;
import org.h2.engine.Constants;
import org.h2.engine.SysProperties;
import org.h2.mvstore.DataUtils;
import org.h2.mvstore.WriteBuffer;
import org.h2.mvstore.db.ValueDataType;
import org.h2.mvstore.type.ExtendedDataType;
import org.h2.result.SortOrder;
import org.h2.store.DataHandler;
import org.h2.util.MathUtils;
import org.h2.util.StringUtils;

/**
 * Implementation of the VARCHAR data type.
 * It is also the base class for other ValueString* classes.
 */
public class ValueString extends Value {

    private static final ValueString EMPTY = new ValueString("");

    /**
     * The string data.
     */
    protected final String value;

    protected ValueString(String value) {
        this.value = value;
    }

    @Override
    public String getSQL() {
        return StringUtils.quoteStringSQL(value);
    }

    @Override
    public boolean equals(Object other) {
        return other instanceof ValueString
                && value.equals(((ValueString) other).value);
    }

    @Override
    protected int compareSecure(Value o, CompareMode mode) {
        // compatibility: the other object could be another type
        ValueString v = (ValueString) o;
        return mode.compareString(value, v.value, false);
    }

    @Override
    public String getString() {
        return value;
    }

    @Override
    public long getPrecision() {
        return value.length();
    }

    @Override
    public Object getObject() {
        return value;
    }

    @Override
    public void set(PreparedStatement prep, int parameterIndex)
            throws SQLException {
        prep.setString(parameterIndex, value);
    }

    @Override
    public int getDisplaySize() {
        return value.length();
    }

    @Override
    public int getMemory() {
        return value.length() * 2 + 48;
    }

    @Override
    public Value convertPrecision(long precision, boolean force) {
        if (precision == 0 || value.length() <= precision) {
            return this;
        }
        int p = MathUtils.convertLongToInt(precision);
        return getNew(value.substring(0, p));
    }

    @Override
    public int hashCode() {
        // TODO hash performance: could build a quicker hash
        // by hashing the size and a few characters
        return value.hashCode();

        // proposed code:
//        private int hash = 0;
//
//        public int hashCode() {
//            int h = hash;
//            if (h == 0) {
//                String s = value;
//                int l = s.length();
//                if (l > 0) {
//                    if (l < 16)
//                        h = s.hashCode();
//                    else {
//                        h = l;
//                        for (int i = 1; i <= l; i <<= 1)
//                            h = 31 *
//                                (31 * h + s.charAt(i - 1)) +
//                                s.charAt(l - i);
//                    }
//                    hash = h;
//                }
//            }
//            return h;
//        }

    }

    @Override
    public int getType() {
        return Value.STRING;
    }

    /**
     * Get or create a string value for the given string.
     *
     * @param s the string
     * @return the value
     */
    public static Value get(String s) {
        return get(s, false);
    }

    /**
     * Get or create a string value for the given string.
     *
     * @param s the string
     * @param treatEmptyStringsAsNull whether or not to treat empty strings as
     *            NULL
     * @return the value
     */
    public static Value get(String s, boolean treatEmptyStringsAsNull) {
        if (s.isEmpty()) {
            return treatEmptyStringsAsNull ? ValueNull.INSTANCE : EMPTY;
        }
        ValueString obj = new ValueString(StringUtils.cache(s));
        if (s.length() > SysProperties.OBJECT_CACHE_MAX_PER_ELEMENT_SIZE) {
            return obj;
        }
        return Value.cache(obj);
        // this saves memory, but is really slow
        // return new ValueString(s.intern());
    }

    /**
     * Create a new String value of the current class.
     * This method is meant to be overridden by subclasses.
     *
     * @param s the string
     * @return the value
     */
    protected Value getNew(String s) {
        return ValueString.get(s);
    }


    public static final class Type extends ValueDataType implements ExtendedDataType {

        public static final ValueInt.Type INSTANCE = new ValueInt.Type(null, null, null);

        public Type(CompareMode compareMode, DataHandler handler, int[] sortTypes) {
            super(compareMode, handler, sortTypes);
        }

        @Override
        public Object createStorage(int size) {
            return new String[size];
        }

        @Override
        public Object clone(Object storage) {
            return ((String[])storage).clone();
        }

        @Override
        public int getLength(Object storage) {
            return ((String[])storage).length;
        }

        @Override
        public Object getValue(Object storage, int indx) {
            return ValueString.get(((String[])storage)[indx]);
        }

        @Override
        public void setValue(Object storage, int indx, Object value) {
            ((String[])storage)[indx] = ((Value)value).getString();
        }

        @Override
        public int getMemorySize(Object storage) {
            int size = 0;
            for (String s : ((String[]) storage)) {
                size += s.length();
            }
            size *= 2;
            size += getLength(storage) * (Constants.MEMORY_POINTER + Constants.MEMORY_OBJECT);
            return size;
        }

        @Override
        public int binarySearch(Object what, Object storage, int initialGuess) {
            if (what == null) {
                return -1;
            }
            if (what == ValueNull.INSTANCE) {
                int sortType = SortOrder.ASCENDING;
                return SortOrder.compareNull(true, sortType);
            }
            String[] data = (String[]) storage;
            String key = ((Value) what).getString();
            int low = 0;
            int high = data.length - 1;
            // the cached index minus one, so that
            // for the first time (when cachedCompare is 0),
            // the default value is used
            int x = initialGuess - 1;
            if (x < 0 || x > high) {
                x = high >>> 1;
            }
            while (low <= high) {
                int compare = key.compareTo(data[x]);
                if (compare > 0) {
                    low = x + 1;
                } else if (compare < 0) {
                    high = x - 1;
                } else {
                    return x;
                }
                x = (low + high) >>> 1;
            }
            x = -(low + 1);
            return x;
        }

        @Override
        public void writeStorage(WriteBuffer buff, Object storage) {
            String[] data = (String[]) storage;
            for (String x : data) {
                buff.putVarInt(x.length());
                buff.putStringData(x, x.length());
            }
        }

        @Override
        public void read(ByteBuffer buff, Object storage) {
            String[] data = (String[]) storage;
            for (int i = 0; i < data.length; i++) {
                int len = DataUtils.readVarInt(buff);
                data[i] = DataUtils.readString(buff, len);
            }
        }
    }
}
