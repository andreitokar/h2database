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

    Object createStorage(int size);

    Object clone(Object storage);

    int getLength(Object storage);

    Object getValue(Object storage, int indx);

    void setValue(Object storage, int indx, Object value);

    /**
     * Estimate the used memory in bytes.
     *
     * @param storage the object
     * @return the used memory
     */
    int getMemorySize(Object storage);

    int binarySearch(Object key, Object storage, int initialGuess);

    void writeStorage(WriteBuffer buff, Object storage);

    void read(ByteBuffer buff, Object storage);
}
