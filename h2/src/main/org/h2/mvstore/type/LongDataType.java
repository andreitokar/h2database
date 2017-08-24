package org.h2.mvstore.type;

import org.h2.mvstore.DataUtils;
import org.h2.mvstore.WriteBuffer;

import java.nio.ByteBuffer;

/**
 * Class LongDataType.
 * <UL>
 * <LI> 8/21/17 6:52 PM initial creation
 * </UL>
 *
 * @author <a href='mailto:andrei.tokar@gmail.com'>Andrei Tokar</a>
 */
public class LongDataType implements ExtendedDataType {

    public static final LongDataType INSTANCE = new LongDataType();

    public LongDataType() {}


    @Override
    public Object createStorage(int size) {
        return new long[size];
    }

    @Override
    public Object clone(Object storage) {
        return cast(storage).clone();
    }

    @Override
    public int getLength(Object storage) {
        return cast(storage).length;
    }

    @Override
    public Object getValue(Object storage, int indx) {
        return cast(storage)[indx];
    }

    @Override
    public void setValue(Object storage, int indx, Object value) {
        cast(storage)[indx] = ((Long)value);
    }

    @Override
    public int getMemory(Object obj) {
        return 8;
    }

    @Override
    public int getMemorySize(Object storage) {
        return getLength(storage) * 8;
    }

    @Override
    public void write(WriteBuffer buff, Object obj) {
        Long data = (Long)obj;
        buff.putVarLong(data);
    }

    @Override
    public void write(WriteBuffer buff, Object[] obj, int len, boolean key) {
        for (int i = 0; i < len; i++) {
            write(buff, obj[i]);
        }
    }

    @Override
    public void writeStorage(WriteBuffer buff, Object storage) {
        for (long x : cast(storage)) {
            buff.putVarLong(x);
        }
    }

    @Override
    public void read(ByteBuffer buff, Object storage) {
        long[] data = cast(storage);
        for (int i = 0; i < data.length; i++) {
            data[i] = DataUtils.readVarLong(buff);
        }
    }

    @Override
    public Object read(ByteBuffer buff) {
        return DataUtils.readVarLong(buff);
    }

    @Override
    public void read(ByteBuffer buff, Object[] obj, int len, boolean key) {
        for (int i = 0; i < len; i++) {
            obj[i] = read(buff);
        }
    }

    @Override
    public int compare(Object aObj, Object bObj) {
        if (aObj instanceof Long && bObj instanceof Long) {
            Long a = (Long) aObj;
            Long b = (Long) bObj;
            return Long.compare(a,b);
        }
        return compareWithNulls(aObj, bObj);
    }

    @Override
    public int binarySearch(Object what, Object storage, int initialGuess) {
        if (what == null) {
            return -1;
        }
        long[] data = cast(storage);
        long key = ((Long) what);
        int low = 0;
        int high = data.length - 1;
        // the cached index minus one, so that
        // for the first time (when cachedCompare is 0),
        // the default value is used
        int x = initialGuess - 1;
        if (x < 0 || x > high) {
            x = high >>> 1;
        }
        return binarySearch(cast(storage), key, low, high, x);
    }

    private static int binarySearch(long[] data, long key, int low, int high, int x) {
        while (low <= high) {
            int compare = Long.compare(key, data[x]);
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

    @SuppressWarnings("unchecked")
    private static long[] cast(Object storage) {
        return (long[])storage;
    }

    private static int compareWithNulls(Object a, Object b) {
        if (a == b) {
            return 0;
        } else if (a == null) {
            return -1;
        } else if (b == null) {
            return 1;
        }
        throw new UnsupportedOperationException();
    }
}
