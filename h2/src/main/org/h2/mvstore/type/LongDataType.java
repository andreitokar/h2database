/*
 * Copyright 2004-2021 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (https://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.mvstore.type;

import java.nio.ByteBuffer;
import org.h2.mvstore.DataUtils;
import org.h2.mvstore.WriteBuffer;

/**
 * Class LongDataType.
 * <UL>
 * <LI> 8/21/17 6:52 PM initial creation
 * </UL>
 *
 * @author <a href='mailto:andrei.tokar@gmail.com'>Andrei Tokar</a>
 */
public class LongDataType extends BasicDataType<Long> {

    public static final LongDataType INSTANCE = new LongDataType();

    private static final Long[] EMPTY_LONG_ARR = new Long[0];

    public LongDataType() {}

    @Override
    public int getMemory(Long obj) {
        return 8;
    }

    @Override
    public void write(WriteBuffer buff, Long data) {
        buff.putVarLong(data);
    }

    @Override
    public Long read(ByteBuffer buff) {
        return DataUtils.readVarLong(buff);
    }

    @Override
    public Long[] createStorage(int size) {
        return size == 0 ? EMPTY_LONG_ARR : new Long[size];
    }

    @Override
    public int compare(Long one, Long two) {
        return Long.compare(one, two);
    }

    @Override
    public int binarySearch(Long keyObj, Object storageObj, int size) {
        long key = keyObj;
        Long[] storage = cast(storageObj);
        int low = 0;
        int high = size - 1;
        while (low <= high) {
            int x = (low + high) >>> 1;
            long midVal = storage[x];
            if (key > midVal) {
                low = x + 1;
            } else if (key < midVal) {
                high = x - 1;
            } else {
                return x;
            }
        }
        return ~low;
    }
}
