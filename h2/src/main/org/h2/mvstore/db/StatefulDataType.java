package org.h2.mvstore.db;

import org.h2.engine.Database;
import org.h2.mvstore.WriteBuffer;
import org.h2.mvstore.type.DataType;

import java.nio.ByteBuffer;

/**
 * Interface StatefulDataType.
 * <UL>
 * <LI> 8/11/17 5:06 PM initial creation
 * </UL>
 *
 * @author <a href='mailto:andrei.tokar@gmail.com'>Andrei Tokar</a>
 */
public interface StatefulDataType {

    void save(WriteBuffer buff, DataType metaDataType, Database database);

    void load(ByteBuffer buff, DataType metaDataType, Database database);

    Factory getFactory();

    interface Factory {

        DataType create(ByteBuffer buff, DataType metaDataType, Database database);
    }
}
