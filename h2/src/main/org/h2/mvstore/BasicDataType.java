package org.h2.mvstore;

import org.h2.engine.Constants;
import org.h2.mvstore.type.ExtendedDataType;

import java.nio.ByteBuffer;

/**
 * Class BasicDataType.
 * <UL>
 * <LI> 8/11/17 4:46 PM initial creation
 * </UL>
 *
 * @author <a href='mailto:andrei.tokar@gmail.com'>Andrei Tokar</a>
 */
public abstract class BasicDataType<T> implements ExtendedDataType {

    @Override
    public abstract int getMemory(Object obj);

    @Override
    public abstract void write(WriteBuffer buff, Object obj);

    @Override
    public abstract Object read(ByteBuffer buff);


    @Override
    public Object createStorage(int capacity) {
        return new Object[capacity];
    }

    @Override
    public final Object clone(Object storage) {
        return cast(storage).clone();
    }

    @Override
    public final int getCapacity(Object storage) {
        return cast(storage).length;
    }

    @Override
    public final T getValue(Object storage, int indx) {
        return cast(storage)[indx];
    }

    @Override
    @SuppressWarnings("unchecked")
    public final void setValue(Object storage, int indx, Object value) {
        cast(storage)[indx] = (T)value;
    }

    @Override
    public final int getMemorySize(Object storage, int size) {
        T[] data = cast(storage);
        int res = data.length * Constants.MEMORY_POINTER;
        for (int i = 0; i < size; i++) {
            res += getMemory(data[i]);
        }
        return res;
    }

    @Override
    public int binarySearch(Object key, Object storage, int size, int initialGuess) {
        throw new UnsupportedOperationException();
    }

    @Override
    public final void writeStorage(WriteBuffer buff, Object storage, int size) {
        T[] data = cast(storage);
        for (int i = 0; i < size; i++) {
            write(buff, data[i]);
        }
    }

    @Override
    public final void read(ByteBuffer buff, Object storage, int size) {
        T[] data = cast(storage);
        for (int i = 0; i < size; i++) {
            data[i] = (T)read(buff);
        }

    }

    @Override
    public int compare(Object a, Object b) {
        if (a == b) {
            return 0;
        } else if (a == null) {
            return -1;
        } else if (b == null) {
            return 1;
        }
        throw new UnsupportedOperationException();
    }


    @Override
    public final void write(WriteBuffer buff, Object[] obj, int len, boolean key) {
        for (int i = 0; i < len; i++) {
            write(buff, obj[i]);
        }
    }

    @Override
    public final void read(ByteBuffer buff, Object[] obj, int len, boolean key) {
        for (int i = 0; i < len; i++) {
            obj[i] = read(buff);
        }
    }

    @SuppressWarnings("unchecked")
    private T[] cast(Object storage) {
        return (T[])storage;
    }
}
