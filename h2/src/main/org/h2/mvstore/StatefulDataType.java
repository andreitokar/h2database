package org.h2.mvstore;

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

    void save(WriteBuffer buff);

    void load(ByteBuffer buff);
}
