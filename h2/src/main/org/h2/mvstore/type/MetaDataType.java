package org.h2.mvstore.type;

import org.h2.engine.Constants;
import org.h2.mvstore.BasicDataType;
import org.h2.mvstore.DataUtils;
import org.h2.mvstore.WriteBuffer;

import java.nio.ByteBuffer;

/**
 * Class MetaDataType.
 * <UL>
 * <LI> 8/11/17 5:01 PM initial creation
 * </UL>
 *
 * @author <a href='mailto:andrei.tokar@gmail.com'>Andrei Tokar</a>
 */
public final class MetaDataType extends BasicDataType<DataType> {

    private final Thread.UncaughtExceptionHandler exceptionHandler;

    public MetaDataType(Thread.UncaughtExceptionHandler exceptionHandler) {
        this.exceptionHandler = exceptionHandler;
    }


    @Override
    public int getMemory(Object obj) {
        return Constants.MEMORY_OBJECT;
    }

    @Override
    public void write(WriteBuffer buff, Object obj) {
        Class<?> clazz = obj.getClass();
        String className = clazz.getName();
        int len = className.length();
        buff.putVarInt(len)
            .putStringData(className, len);
    }

    @Override
    public DataType read(ByteBuffer buff) {
        int len = DataUtils.readVarInt(buff);
        String className = DataUtils.readString(buff, len);
        try {
            Class<?> clazz = Class.forName(className);
            return (DataType) clazz.newInstance();
        } catch (ClassNotFoundException | InstantiationException | IllegalAccessException e) {
            if(exceptionHandler != null) {
                exceptionHandler.uncaughtException(Thread.currentThread(), e);
            }
            throw new RuntimeException(e);
        }
    }
}
