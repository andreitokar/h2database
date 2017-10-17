/*
 * Copyright 2004-2014 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 */
package org.h2.mvstore.type;

import org.h2.mvstore.WriteBuffer;

import java.nio.ByteBuffer;

/**
 * Interface ExtendedDataType.
 *
 * @author <a href='mailto:andrei.tokar@gmail.com'>Andrei Tokar</a>
 */
public interface ExtendedDataType extends DataType {

    Object createStorage(int capacity);

    Object clone(Object storage);

    int getCapacity(Object storage);

    Object getValue(Object storage, int indx);

    void setValue(Object storage, int indx, Object value);

    /**
     * Estimate the used memory in bytes.
     *
     * @param storage opaque representation
     * @param size number of data items in the storage
     * @return the used memory
     */
    int getMemorySize(Object storage, int size);

    int binarySearch(Object key, Object storage, int size, int initialGuess);

    void writeStorage(WriteBuffer buff, Object storage, int size);

    void read(ByteBuffer buff, Object storage, int size);
}
