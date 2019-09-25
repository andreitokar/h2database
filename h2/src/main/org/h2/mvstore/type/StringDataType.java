/*
 * Copyright 2004-2019 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (https://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.mvstore.type;

import java.nio.ByteBuffer;

import org.h2.engine.Constants;
import org.h2.mvstore.DataUtils;
import org.h2.mvstore.WriteBuffer;

/**
 * A string type.
 */
public final class StringDataType implements ExtendedDataType {

    public static final StringDataType INSTANCE = new StringDataType();

    private StringDataType() {}

    @Override
    public int compare(Object a, Object b) {
        return a.toString().compareTo(b.toString());
    }

    @Override
    public int getMemory(Object obj) {
        return Constants.MEMORY_POINTER + Constants.MEMORY_OBJECT + 2 * obj.toString().length();
    }

    @Override
    public void read(ByteBuffer buff, Object[] obj, int len, boolean key) {
        for (int i = 0; i < len; i++) {
            obj[i] = read(buff);
        }
    }

    @Override
    public void write(WriteBuffer buff, Object[] obj, int len, boolean key) {
        for (int i = 0; i < len; i++) {
            write(buff, obj[i]);
        }
    }

    @Override
    public String read(ByteBuffer buff) {
        return DataUtils.readString(buff);
    }

    @Override
    public void write(WriteBuffer buff, Object obj) {
        String s = obj.toString();
        int len = s.length();
        buff.putVarInt(len).putStringData(s, len);
    }

    @Override
    public Object createStorage(int capacity) {
        return new String[capacity];
    }

    @Override
    public Object clone(Object storage) {
        return ((String[])storage).clone();
    }

    @Override
    public int getCapacity(Object storage) {
        return ((String[])storage).length;
    }

    @Override
    public Object getValue(Object storage, int indx) {
        return ((String[])storage)[indx];
    }

    @Override
    public void setValue(Object storage, int indx, Object value) {
        ((String[])storage)[indx] = ((String)value);
    }

    @Override
    public int getMemorySize(Object storage, int size) {
        String[] data = (String[]) storage;
        int res = 0;
        for (int i = 0; i < size; i++) {
            res += data[i].length();
        }
        return res * 2 + size * (Constants.MEMORY_POINTER + Constants.MEMORY_OBJECT);
    }

    @Override
    public int binarySearch(Object what, Object storage, int size, int initialGuess) {
        if (what == null) {
            return -1;
        }
        String[] data = (String[]) storage;
        String key = ((String) what);
        int low = 0;
        int high = size - 1;
        // the cached index minus one, so that
        // for the first time (when cachedCompare is 0),
        // the default value is used
        int x = initialGuess - 1;
        if (x < 0 || x > high) {
            x = high >>> 1;
        }
        return binarySearch(data, key, low, high, x);
    }

    private static int binarySearch(String[] data, String key, int low, int high, int x) {
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
    public void writeStorage(WriteBuffer buff, Object storage, int size) {
        String[] data = (String[]) storage;
        for (int i = 0; i < size; i++) {
            write(buff, data[i]);
        }
    }

    @Override
    public void read(ByteBuffer buff, Object storage, int size) {
        String[] data = (String[]) storage;
        for (int i = 0; i < size; i++) {
            data[i] = read(buff);
        }
    }
}

