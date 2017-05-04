package org.h2.bytecode;

import org.h2.result.Row;
import org.h2.result.SearchRow;
import org.h2.store.Data;
import org.h2.value.*;

import java.math.BigDecimal;

/**
 * Class RowStorage is the base class for all schema-aware auto-generated
 * row storage classes.
 * <UL>
 * <LI> 4/10/17 6:42 PM initial creation
 * </UL>
 *
 * @author <a href='mailto:andrei.tokar@gmail.com'>Andrei Tokar</a>
 */
public abstract class RowStorage extends Value implements Row, Cloneable {

    private static final int DELETED_BIT_MASK = Integer.MIN_VALUE;
    private static final byte[] NULL_BYTES = {};
    private static final String NULL_STRING = new String(NULL_BYTES);
    private static final BigDecimal NULL_BIG_DECIMAL = new BigDecimal(0);

    private long key;
    private int  version;
    private int  sessionId;

    protected RowStorage() {}   // keep it for auto-generated subclasses

    public final void setValues(Value values[]) {
        int valuesCount = values == null ? 0 : values.length;
        for (int indx = 0; indx < valuesCount; ++indx) {
            setValue(indx, values[indx]);
        }
        int columnCount = getColumnCount();
        for (int indx = valuesCount; indx < columnCount; ++indx) {
            setValue(indx, null);
        }
    }

    @Override
    public abstract int getColumnCount();

    @Override
    public abstract int getMemory();

    @Override
    public Value getValue(int index) {
        throw new IllegalArgumentException(getClass().getSimpleName()+".getValue("+index+")");
    }

    @Override
    public void setValue(int index, Value v) {
        throw new IllegalArgumentException(getClass().getSimpleName()+".setValue("+index+", ..)");
    }


    @Override
    public final void setKeyAndVersion(SearchRow row) {
        key = row.getKey();
        version = row.getVersion();
    }

    @Override
    public final int getVersion() {
        return version;
    }

    @Override
    public final void setVersion(int version) {
        this.version = version;
    }

    @Override
    public final long getKey() {
        return key;
    }

    @Override
    public final void setKey(long key) {
        this.key = key;
    }

    @Override
    public final Row getCopy() {
        return clone();
    }

    public RowStorage clone() {
        try {
            return (RowStorage) super.clone();
        } catch (CloneNotSupportedException impossible) {
            throw new RuntimeException(impossible);
        }
    }


    @Override
    public final int getByteCount(Data dummy) {
        int size = 0;
        for (int indx = 0; indx < getColumnCount(); ++indx) {
            size += dummy.getValueLen(getValue(indx));
        }
        return size;
    }

    @Override
    public final boolean isEmpty() {
        for (int indx = 0; indx < getColumnCount(); ++indx) {
            Value value = getValue(indx);
            if(value != null) {
                return true;
            }
        }
        return false;
    }

    @Override
    public final void setDeleted(boolean deleted) {
        sessionId = deleted ? sessionId | DELETED_BIT_MASK : sessionId & ~DELETED_BIT_MASK;
    }

    @Override
    public final void setSessionId(int sessionId) {
        if((sessionId & DELETED_BIT_MASK) != 0) throw new IllegalArgumentException("negative session id");
        this.sessionId = this.sessionId & DELETED_BIT_MASK | sessionId;
    }

    @Override
    public final int getSessionId() {
        return sessionId & ~DELETED_BIT_MASK;
    }

    @Override
    public final void commit() {
        sessionId &= DELETED_BIT_MASK;
    }

    @Override
    public final boolean isDeleted() {
        return (sessionId & DELETED_BIT_MASK) != 0;
    }

    @Override
    public final Value[] getValueList() {
        Value[] values = new Value[getColumnCount()];
        for (int indx = 0; indx < getColumnCount(); ++indx) {
            values[indx] = getValue(indx);
        }
        return values;
    }


    @Override
    public int getType() {
        return Value.ARRAY;
    }

    @Override
    public long getPrecision() {
        long p = 0;
        for (int indx = 0; indx < getColumnCount(); ++indx) {
            Value value = getValue(indx);
            p += value == null ? 0 : value.getPrecision();
        }
        return p;
    }

    @Override
    protected int compareSecure(Value o, CompareMode mode) {
        if(this == o) return 0;
        RowStorage other = (RowStorage) o;
        if(getClass() != o.getClass())
        {
            return Integer.compare(System.identityHashCode(this), System.identityHashCode(o));
        }
        int columnCount = getColumnCount();
        for (int indx = 0; indx < columnCount; ++indx) {
            Value value = getValue(indx);
            Value otherValue = other.getValue(indx);
            int comp = value.compareTo(otherValue, mode);
            if (comp != 0) {
                return comp;
            }
        }
        return 0;
    }



    @Override
    public boolean equals(Object o) {
        if(this == o) return true;
        if(o == null || o.getClass() != getClass()) return false;

        RowStorage other = (RowStorage) o;
        int columnCount = getColumnCount();
        if (key != other.key || version != other.version) return false;
        for (int indx = 0; indx < columnCount; ++indx) {
            Value value = getValue(indx);
            Value otherValue = other.getValue(indx);
            if(value == null && otherValue != null || otherValue == null || value.equals(otherValue)) {
                return false;
            }
        }
        return true;
    }

    @Override
    public int hashCode() {
        int result = (int) (key ^ (key >>> 32));
        result = 31 * result + version;
        int columnCount = getColumnCount();
        for (int indx = 0; indx < columnCount; ++indx) {
            Value value = getValue(indx);
            result = 31 * result + (value == null ? 0 : value.hashCode());
        }
        return result;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("Row{").append(key).append('/').append(version);
        String separator = " ";
        for (int indx = 0; indx < getColumnCount(); ++indx) {
            sb.append(separator).append(getValue(indx));
            separator = ", ";
        }
        sb.append('}');
        return sb.toString();
    }

    /////////////////////////////////////////////////////////////////////

    protected static int getMemory(byte v[]) {
        return v == null ? 0 : v.length;
    }

    protected static int getMemory(BigDecimal v) {
        return 150;
    }

    protected static int getMemory(String v) {
        return v == null ? 0 : 2 * v.length();
    }

    protected static int getMemory(Value v) {
        return v == null ? 0 : v.getMemory();
    }


    protected static int getInt(Value v)
    {
        return v == null ? Integer.MIN_VALUE + 1 : v == ValueNull.INSTANCE ? Integer.MIN_VALUE : v.getInt();
    }

    protected static Value getValueInt(int v)
    {
        return v == Integer.MIN_VALUE + 1 ? null : v == Integer.MIN_VALUE ? ValueNull.INSTANCE : ValueInt.get(v);
    }


    protected static long getLong(Value v)
    {
        return v == null ? Long.MIN_VALUE + 1 : v == ValueNull.INSTANCE ? Long.MIN_VALUE : v.getLong();
    }

    protected static Value getValueLong(long v)
    {
        return v == Long.MIN_VALUE + 1 ? null : v == Long.MIN_VALUE ? ValueNull.INSTANCE : ValueLong.get(v);
    }


    protected static float getFloat(Value v)
    {
        return v == null ? Float.NEGATIVE_INFINITY : v == ValueNull.INSTANCE ? Float.NaN : v.getFloat();
    }

    protected static Value getValueFloat(float v)
    {
        return v == Float.NEGATIVE_INFINITY ? null : Float.isNaN(v) ? ValueNull.INSTANCE : ValueFloat.get(v);
    }


    protected static Double getDouble(Value v)
    {
        return v == null ? Double.NEGATIVE_INFINITY : v == ValueNull.INSTANCE ? Double.NaN : v.getFloat();
    }

    protected static Value getValueDouble(double v)
    {
        return v == Double.NEGATIVE_INFINITY ? null : Double.isNaN(v) ? ValueNull.INSTANCE : ValueDouble.get(v);
    }


    protected static BigDecimal getDecimal(Value v)
    {
        return v == null ? null : v == ValueNull.INSTANCE ? NULL_BIG_DECIMAL : v.getBigDecimal();
    }

    @SuppressWarnings("NumberEquality")
    protected static Value getValueDecimal(BigDecimal v)
    {
        return v == null ? null : v == NULL_BIG_DECIMAL ? ValueNull.INSTANCE : ValueDecimal.get(v);
    }


    protected static byte[] getBytes(Value v)
    {
        return v == null ? null : v == ValueNull.INSTANCE ? NULL_BYTES : v.getString().getBytes();
    }

    protected static Value getValueBytes(byte v[])
    {
        return v == null ? null : v == NULL_BYTES ? ValueNull.INSTANCE  : ValueString.get(new String(v));
    }


    protected static String getString(Value v)
    {
        return v == null ? null : v == ValueNull.INSTANCE ? NULL_STRING : v.getString();
    }

    @SuppressWarnings("StringEquality")
    protected static Value getValueString(String v)
    {
        return v == null ? null : v == NULL_STRING ? ValueNull.INSTANCE  : ValueString.get(v);
    }
}
