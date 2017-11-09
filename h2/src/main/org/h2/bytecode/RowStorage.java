package org.h2.bytecode;

import org.h2.engine.Database;
import org.h2.mvstore.BasicDataType;
import org.h2.mvstore.DataUtils;
import org.h2.mvstore.RowDataType;
import org.h2.mvstore.WriteBuffer;
import org.h2.mvstore.db.StatefulDataType;
import org.h2.mvstore.db.ValueDataType;
import org.h2.mvstore.type.DataType;
import org.h2.result.Row;
import org.h2.result.RowFactory;
import org.h2.result.SearchRow;
import org.h2.result.SortOrder;
import org.h2.store.Data;
import org.h2.store.DataHandler;
import org.h2.value.*;
import sun.reflect.generics.reflectiveObjects.NotImplementedException;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Arrays;

/**
 * Class RowStorage is the base class for all schema-aware auto-generated
 * row storage classes.
 * <UL>
 * <LI> 4/10/17 6:42 PM initial creation
 * </UL>
 *
 * @author <a href='mailto:andrei.tokar@gmail.com'>Andrei Tokar</a>
 */
public class RowStorage extends Value implements Row, Cloneable {

    private static final int DELETED_BIT_MASK = Integer.MIN_VALUE;
    private static final byte[] NULL_BYTES = {};
    private static final String NULL_STRING = new String(NULL_BYTES);
    private static final BigDecimal NULL_BIG_DECIMAL = new BigDecimal(0);

    private long key;
    private int  version;
    private int  sessionId;

    public RowStorage() {}   // keep it for auto-generated subclasses

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
    public int getColumnCount() {
        return 0;
    }

    @Override
    public int getMemory() {
        return 0;
    }

    public int[] getIndexes() {
        return null;
    }

    public int getInt(int indx) { return 0; }
    public long getLong(int indx) { return 0L; }
    public float getFloat(int indx) { return 0.0f; }
    public double getDouble(int indx) { return 0.0; }
    public BigDecimal getDecimal(int indx) { return BigDecimal.ZERO; }
    public byte[] getBytes(int indx) { return null; }
    public String getString(int indx) { return null; }
//    public Object get(int indx) { return null; }

    public void setInt(int indx, int value) {}
    public void setLong(int indx, long value) {}
    public void setFloat(int indx, float value) {}
    public void setDouble(int indx, double value) {}
    public void setDecimal(int indx, BigDecimal value) {}
    public void setBytes(int indx, byte[] value) {}
    public void setString(int indx) {}
//    public void set(int indx, Object value) {}
    protected void nullify(int indx) {}
    protected void clearNull(int indx) {}

    protected int compareToSecure(RowStorage other, CompareMode mode) {
        return 0;
    }

    protected int compareToSecure(RowStorage other, CompareMode mode, int index) {
        return 0;
    }

    public void copyFrom(RowStorage other, int index) {
    }

    protected final int compareTo(RowStorage other, int index, CompareMode compareMode, int sortType) {
        boolean isNull = isNull(index);
        boolean otherIsNull = other.isNull(index);
        if (isNull && otherIsNull) {
            return 0;
        }
        if (isNull || otherIsNull) {
            return SortOrder.compareNull(isNull, sortType);
        }

        int res = compareToSecure(other, compareMode, index);
        assert normalizeCompare(res) == normalizeCompare(getValue(index).compareTypeSafe(other.getValue(index), compareMode)) : res;

        if ((sortType & SortOrder.DESCENDING) != 0) {
            res = -res;
        }
        return res;
    }

    public final void copyFrom(SearchRow source) {
        copyFrom((RowStorage)source);
    }

    public final void copyFrom(RowStorage source) {
        setKey(source.getKey());
        int[] indexes = getIndexes();
        if (indexes == null) {
            int columnCount = getColumnCount();
            for (int i = 0; i < columnCount; i++) {
                copyFrom(source, i);
            }
        } else {
            for (int indx : indexes) {
                copyFrom(source, indx);
            }
        }
    }

    public boolean isNull(int index) {
        throw new IllegalArgumentException(getClass().getSimpleName()+".isNull("+index+")");
    }

    // TODO eliminate boxing and generate type-specific code instead
    public final boolean isEmpty(int index) {
        Value value = getValue(index);
        return value == null;
    }

    @Override
    public final Value getValue(int index) {
        Value value = get(index);
        return value != null && isNull(index) ? ValueNull.INSTANCE : value;
    }

    protected Value get(int index) {
        throw new IllegalArgumentException(getClass().getSimpleName()+".getValue("+index+")");
    }

    @Override
    public final void setValue(int index, Value v) {
        if(isNull(v)) {
            nullify(index);
        } else {
            clearNull(index);
        }
        set(index, v);
    }

    protected void set(int index, Value v) {
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
        return Value.ROW;
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
    public String getSQL() {
        StringBuilder sb = new StringBuilder();
        sb.append("(");
        for (int indx = 0; indx < getColumnCount(); ++indx) {
            if(indx != 0) {
                sb.append(", ");
            }
            Value value = getValue(indx);
            sb.append(value.getSQL());
        }
        sb.append(")");
        return sb.toString();
    }

    @Override
    public int getDisplaySize() {
        int res = 0;
        for (int indx = 0; indx < getColumnCount(); ++indx) {
            Value value = getValue(indx);
            res += value.getDisplaySize();
        }
        return res;
    }

    @Override
    public String getString() {
        return getSQL();
    }

    @Override
    public Object getObject() {
        return this;
    }

    @Override
    public void set(PreparedStatement prep, int parameterIndex) throws SQLException {
        throw new NotImplementedException();
    }

    @Override
    protected int compareSecure(Value o, CompareMode mode) {
        if(this == o) return 0;
        if(getClass() != o.getClass()) {
            return Integer.compare(System.identityHashCode(this), System.identityHashCode(o));
        }
        RowStorage other = (RowStorage)o;
        return compareToSecure(other, mode);
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

    protected static Value getNullValue() {
        return null;
    }

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

    protected static int compare(BigDecimal one, BigDecimal two, CompareMode mode) {
        int res = one == two  ?  0 :
                  one == null ? -1 :
                  two == null ?  1 :
                                normalizeCompare(one.compareTo(two));
        return res;
    }

    protected static int compare(byte[] one, byte[] two, CompareMode mode) {
        int res = one == two   ?  0 :
                  one == null  ? -1 :
                  two == null  ?  1 :
                  normalizeCompare(mode == null ? new String(one).compareTo(new String(two)) :
                                   mode.compareString(new String(one), new String(two), false));
        return res;
    }

    protected static int compare(String one, String two, CompareMode mode) {
        int res = one == two   ?  0 :
                  one == null  ? -1 :
                  two == null  ?  1 :
                  normalizeCompare(mode == null ? one.compareTo(two) :
                                   mode.compareString(one, two, false));
        return res;
    }

    private static int normalizeCompare(int res) {
        return Integer.compare(res, 0);
    }

    protected static int compare(Value one, Value two, CompareMode mode) {
        int res = one == two  ?  0 :
                  one == null ? -1 :
                  two == null ?  1 :
                                one.compareTo(two, mode);
        return res;
    }

    ///////////////////////////////////////////////////////////////////////////
    //      Conversion Methods      //
    //////////////////////////////////


    protected static int toInt(Value v)
    {
        return v == null ? Integer.MIN_VALUE : v == ValueNull.INSTANCE ? 0 : v.getInt();
    }

    protected static Value convertFrom(int v)
    {
        return v == Integer.MIN_VALUE ? null : ValueInt.get(v);
    }


    protected static long toLong(Value v)
    {
        return v == null ? Long.MIN_VALUE : v == ValueNull.INSTANCE ? 0L : v.getLong();
    }

    protected static Value convertFrom(long v)
    {
        return v == Long.MIN_VALUE ? null : ValueLong.get(v);
    }


    protected static float toFloat(Value v)
    {
        return v == null ? Float.NaN : v == ValueNull.INSTANCE ? 0.0f : v.getFloat();
    }

    protected static Value convertFrom(float v)
    {
        return v == Float.NaN ? null : ValueFloat.get(v);
    }


    protected static double toDouble(Value v)
    {
        return v == null ? Double.NaN : v == ValueNull.INSTANCE ? 0.0d : v.getDouble();
    }

    protected static Value convertFrom(double v)
    {
        return v == Double.NaN ? null : ValueDouble.get(v);
    }

    // Non-primitive types below use slightly defferent null / NULL encoding logic

    protected static BigDecimal toDecimal(Value v)
    {
        return v == null ? null : v == ValueNull.INSTANCE ? NULL_BIG_DECIMAL : v.getBigDecimal();
    }

    @SuppressWarnings("NumberEquality")
    protected static Value convertFrom(BigDecimal v)
    {
        return v == null ? null : v == NULL_BIG_DECIMAL ? ValueNull.INSTANCE : ValueDecimal.get(v);
    }

    @SuppressWarnings("NumberEquality")
    protected static boolean isNull(BigDecimal v)
    {
        return v == null || v == NULL_BIG_DECIMAL;
    }


    protected static byte[] toBytes(Value v)
    {
        return v == null ? null : v == ValueNull.INSTANCE ? NULL_BYTES : v.getString().getBytes();
    }

    protected static Value convertFrom(byte v[])
    {
        return v == null ? null : v == NULL_BYTES ? ValueNull.INSTANCE  : ValueString.get(new String(v));
    }

    protected static boolean isNull(byte v[])
    {
        return v == null || v == NULL_BYTES;
    }

    protected static String toString(Value v)
    {
        return v == null ? null : v == ValueNull.INSTANCE ? NULL_STRING : v.getString();
    }

    @SuppressWarnings("StringEquality")
    protected static Value convertFrom(String v)
    {
        return v == null ? null : v == NULL_STRING ? ValueNull.INSTANCE  : ValueString.get(v);
    }

    @SuppressWarnings("StringEquality")
    protected static boolean isNull(String v)
    {
        return v == null || v == NULL_STRING;
    }

    protected static boolean isNull(Value v)
    {
        return v == null || v == ValueNull.INSTANCE;
    }

    ///////////////////////////////////////////////////////////////////////////

    public static final class Type extends BasicDataType<RowStorage> implements StatefulDataType {

        private final ValueDataType valueDataType;
        private final CompareMode   compareMode;
        private final int[]         sortTypes;


        public Type(CompareMode compareMode, DataHandler handler, int[] sortTypes) {
            this.valueDataType = new ValueDataType(compareMode, handler, sortTypes);
            this.compareMode = compareMode;
            this.sortTypes = sortTypes;
        }

        public RowFactory getRowFactory() {
            return valueDataType.getRowFactory();
        }

        public void setRowFactory(RowFactory rowFactory) {
            valueDataType.setRowFactory(rowFactory);
        }

        @Override
        public Object createStorage(int capacity) {
            return new RowStorage[capacity];
        }

        @Override
        public int getMemory(Object obj) {
            return ((RowStorage)obj).getMemory();
        }

        @Override
        public int compare(Object a, Object b) {
            if (a instanceof RowStorage && b instanceof RowStorage) {
                RowStorage rowA = (RowStorage) a;
                RowStorage rowB = (RowStorage) b;
                assert rowA.getColumnCount() == rowB.getColumnCount() : "Incopmatible rows: " + rowA + " " + rowB;
/*
                assert rowA.getIndexes() == rowB.getIndexes() :
                        "Incopmatible rows: " + rowA.getIndexes() + " "  + rowB.getIndexes();
                assert Arrays.equals(rowA.getIndexes(), rowB.getIndexes())
                        : "Incompatible sparse rows" + Arrays.toString(rowA.getIndexes()) + " " + Arrays.toString(rowB.getIndexes());
*/
                return compare(rowA, rowB);
            }
            return super.compare(a, b);
        }

        public int compare(RowStorage a, RowStorage b) {
            assert a.getColumnCount() == b.getColumnCount();
            int[] indexes = a.getIndexes();
//            assert Arrays.equals(indexes, b.getIndexes());
            int comp = compare(a, b, compareMode, sortTypes, indexes);
//            int _comp = _compare(a, b, compareMode, sortTypes, indexes);
//            assert comp == _comp;
            return comp;
        }

        public int compare(RowStorage a, RowStorage b, CompareMode compareMode, int sortTypes[], int indexes[]) {
            int limit = indexes == null ? a.getColumnCount() : indexes.length;
            for (int i = 0; i < limit; i++) {
                int indx = indexes == null ? i : indexes[i];
                boolean aIsEmpty = a.isEmpty(indx);
                boolean bIsEmpty = b.isEmpty(indx);
//                assert !aIsEmpty;
//                assert !bIsEmpty;

                if (indexes == null) {
                    if (aIsEmpty || bIsEmpty) {
                        // can't compare further
                        return 0;
                    }
                } else if (aIsEmpty) {
                    if (!bIsEmpty) {
                        return -1;
                    }
                } else if (bIsEmpty) {
                    return 1;
                }

                int sortType = sortTypes == null ? SortOrder.ASCENDING : sortTypes[i];
                boolean aIsNull = a.isNull(indx);
                boolean bIsNull = b.isNull(indx);
                if (aIsNull != bIsNull) {
                    return SortOrder.compareNull(aIsNull, sortType);
                }
                if (!aIsNull) { // && !bIsNull
                    int res = a.compareToSecure(b, compareMode, indx);
                    if (res != 0) {
                        if ((sortType & SortOrder.DESCENDING) != 0) {
                            res = -res;
                        }
                        return Integer.compare(res, 0);
                    }
                }
            }
            long aKey = a.getKey();
            long bKey = b.getKey();
            return indexes == null || aKey == SearchRow.MATCH_ALL_ROW_KEY
                                   || bKey == SearchRow.MATCH_ALL_ROW_KEY ? 0 : Long.compare(aKey, bKey);
        }

        private int _compare(RowStorage a, RowStorage b, CompareMode compareMode, int sortTypes[], int indexes[]) {
            if (indexes == null) {
                int columnCount = a.getColumnCount();
                for (int i = 0; i < columnCount; i++) {
                    if (a.isEmpty(i) || b.isEmpty(i)) {
                        return 0;
                    }
                    int res = a.compareTo(b, i, compareMode,
                            sortTypes == null ? SortOrder.ASCENDING : sortTypes[i]);
                    if(res != 0) {
                        return Integer.compare(res, 0);
                    }
                }
                return 0;
            } else {
                assert sortTypes.length == indexes.length;
                for (int i = 0; i < indexes.length; i++) {
                    int indx = indexes[i];
                    if (a.isEmpty(indx) || b.isEmpty(indx)) {
                        break;
                    }
                    int res = a.compareTo(b, indx, compareMode, sortTypes[i]);
                    if(res != 0) {
                        return Integer.compare(res, 0);
                    }
                }
                return Long.compare(a.getKey(), b.getKey());
            }
        }

        @Override
        public int binarySearch(Object key, Object storage, int size, int initialGuess) {
            return binarySearch((RowStorage)key, ((RowStorage[])storage), size, initialGuess);
        }

        public int binarySearch(RowStorage key, RowStorage[] keys, int size, int initialGuess) {
            int low = 0;
            int high = size - 1;
            // the cached index minus one, so that
            // for the first time (when cachedCompare is 0),
            // the default value is used
            int x = initialGuess - 1;
            if (x < 0 || x > high) {
                x = high >>> 1;
            }
            while (low <= high) {
                int compare = compare(key, keys[x]);
                if (compare > 0) {
                    low = x + 1;
                } else if (compare < 0) {
                    high = x - 1;
                } else {
                    return x;
                }
                x = (low + high) >>> 1;
            }
            return -(low + 1);
        }

        @Override
        public void write(WriteBuffer buff, Object obj) {
            write(buff, (RowStorage)obj);
        }

        private void write(WriteBuffer buff, RowStorage row) {
            buff.putVarLong(row.getKey());
            int[] indexes = row.getIndexes();
            if(indexes == null) {
                int columnCount = row.getColumnCount();
                for (int i = 0; i < columnCount; i++) {
                    Value value = row.getValue(i);
                    valueDataType.write(buff, value);
                }
            } else {
                for (int i : indexes) {
                    Value value = row.getValue(i);
                    valueDataType.write(buff, value);
                }
            }
        }

        public RowStorage read(ByteBuffer buff) {
            RowStorage row = (RowStorage)getRowFactory().createRow();
            row.setKey(DataUtils.readVarLong(buff));
            int[] indexes = row.getIndexes();
            if (indexes == null) {
                int columnCount = row.getColumnCount();
                for (int i = 0; i < columnCount; i++) {
                    Value value = valueDataType.readValue(buff);
                    row.setValue(i, value);
                }
            } else {
                for (int i : indexes) {
                    Value value = valueDataType.readValue(buff);
                    row.setValue(i, value);
                }
            }
            return row;
        }

        @Override
        public boolean equals(Object obj) {
            if (obj == this) {
                return true;
            } else if (obj == null || obj.getClass() != Type.class) {
                return false;
            }
            Type other = (Type) obj;
            return valueDataType.equals(other.valueDataType)
//                && Arrays.equals(indexes, other.indexes)
//                && Arrays.equals(sortTypes, other.sortTypes)
                    ;
        }


        @Override
        public int hashCode() {
            return valueDataType.hashCode()/* ^ Arrays.hashCode(indexes)*/;
        }

        @Override
        public void save(WriteBuffer buff, DataType metaDataType, Database database) {
            writeIntArray(buff, sortTypes);
        }

        private static void writeIntArray(WriteBuffer buff, int[] array) {
            if(array == null) {
                buff.putVarInt(0);
            } else {
                buff.putVarInt(array.length + 1);
                for (int i : array) {
                    buff.putVarInt(i);
                }
            }
        }

        @Override
        public void load(ByteBuffer buff, DataType metaDataType, Database database) {
            throw DataUtils.newUnsupportedOperationException("load()");
        }

        @Override
        public Factory getFactory() {
            return FACTORY;
        }



        private static final Factory FACTORY = new Factory();

        public static final class Factory implements StatefulDataType.Factory {

            @Override
            public DataType create(ByteBuffer buff, DataType metaDataType, Database database) {
                int[] sortTypes = readIntArray(buff);
//                int[] indexes = readIntArray(buff);
                CompareMode compareMode = database == null ? CompareMode.getInstance(null, 0) : database.getCompareMode();
                return new Type(compareMode, database, sortTypes);
            }

            private static int[] readIntArray(ByteBuffer buff) {
                int len = DataUtils.readVarInt(buff) - 1;
                if(len < 0) {
                    return null;
                }
                int[] res = new int[len];
                for (int i = 0; i < res.length; i++) {
                    res[i] = DataUtils.readVarInt(buff);
                }
                return res;
            }
        }
    }
}
