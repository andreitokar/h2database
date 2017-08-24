package org.h2.mvstore.db;

import org.h2.engine.Constants;
import org.h2.engine.Database;
import org.h2.mvstore.BasicDataType;
import org.h2.mvstore.DataUtils;
import org.h2.mvstore.MVStore;
import org.h2.mvstore.WriteBuffer;
import org.h2.mvstore.db.StatefulDataType;
import org.h2.mvstore.type.DataType;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;

/**
 * Class DBMetatype.
 * <UL>
 * <LI> 8/22/17 6:40 PM initial creation
 * </UL>
 *
 * @author <a href='mailto:andrei.tokar@gmail.com'>Andrei Tokar</a>
 */
public final class DBMetaType extends BasicDataType<DataType> {

    private final Database database;
    private final Thread.UncaughtExceptionHandler exceptionHandler;
    private final Map<String,StatefulDataType.Factory> cache = new HashMap<>();

    public DBMetaType(Database database, Thread.UncaughtExceptionHandler exceptionHandler) {
        this.database = database;
        this.exceptionHandler = exceptionHandler;
    }

    @Override
    public int getMemory(Object obj) {
        return Constants.MEMORY_OBJECT;
    }

    @Override
    public void write(WriteBuffer buff, Object obj) {
        Class<?> clazz = obj.getClass();
        StatefulDataType statefulDataType = null;
        if (obj instanceof StatefulDataType) {
            statefulDataType = (StatefulDataType) obj;
            StatefulDataType.Factory factory = statefulDataType.getFactory();
            if (factory != null) {
                clazz = factory.getClass();
            }
        }
        String className = clazz.getName();
        int len = className.length();
        buff.putVarInt(len)
            .putStringData(className, len);
        if (statefulDataType != null) {
            statefulDataType.save(buff, this, database);
        }
    }

    @Override
    public DataType read(ByteBuffer buff) {
        int len = DataUtils.readVarInt(buff);
        String className = DataUtils.readString(buff, len);
        try {
            StatefulDataType.Factory factory = cache.get(className);
            if (factory != null) {
                return factory.create(buff, this, database);
            }
            Class<?> clazz = Class.forName(className);
            Object obj = clazz.newInstance();
            if (obj instanceof StatefulDataType.Factory) {
                factory = (StatefulDataType.Factory) obj;
                cache.put(className, factory);
                return factory.create(buff, this, database);
            } else if(obj instanceof StatefulDataType) {
                ((StatefulDataType)obj).load(buff, this, database);
            }
            return (DataType) obj;
        } catch (ClassNotFoundException | InstantiationException | IllegalAccessException e) {
            if(exceptionHandler != null) {
                exceptionHandler.uncaughtException(Thread.currentThread(), e);
            }
            throw new RuntimeException(e);
        }
    }
}
