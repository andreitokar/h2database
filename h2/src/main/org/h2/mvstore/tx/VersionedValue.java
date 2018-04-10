/*
 * Copyright 2004-2018 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.mvstore.tx;

import org.h2.engine.Constants;
import org.h2.engine.Database;
import org.h2.mvstore.BasicDataType;
import org.h2.mvstore.DataUtils;
import org.h2.mvstore.WriteBuffer;
import org.h2.mvstore.db.StatefulDataType;
import org.h2.mvstore.type.DataType;
import java.nio.ByteBuffer;

/**
 * A versioned value (possibly null). It contains a pointer to the old
 * value, and the value itself.
 */
public class VersionedValue
{
    public static final VersionedValue DUMMY = new VersionedValue(new Object());

    /**
     * The value.
     */
    public final Object value;

    VersionedValue(Object value) {
        assert value != null;
        this.value = value;
    }

    VersionedValue(Object value, @SuppressWarnings("unused") boolean dummy) {
        this.value = value;
    }

    /**
     * The operation id.
     */
    public long getOperationId() {
        return 0;
    }

    /**
     * Initial (committed) value for operationId > 0, committed value otherwise.
     */
    public Object getCommittedValue() {
        return value;
    }

    @Override
    public String toString() {
        return String.valueOf(value);
    }


    static final class Uncommitted extends VersionedValue
    {
        private final long   operationId;
        private final Object committedValue;

        Uncommitted(long operationId, Object value, Object committedValue) {
            super(value, false);
            assert operationId != 0;
            this.operationId = operationId;
            this.committedValue = committedValue;
        }

        @Override
        public long getOperationId() {
            return operationId;
        }

        @Override
        public Object getCommittedValue() {
            return committedValue;
        }

        @Override
        public String toString() {
            return super.toString() +
                    " " + TransactionStore.getTransactionId(operationId) + "/" +
                    TransactionStore.getLogId(operationId) + " " + committedValue;
        }
    }

    /**
     * The value type for a versioned value.
     */
    public static final class Type extends BasicDataType<VersionedValue> implements StatefulDataType
    {
        private final DataType valueType;

        public Type(DataType valueType) {
            this.valueType = valueType;
        }

        @Override
        public int getMemory(Object obj) {
            if(obj == null) return 0;
            VersionedValue v = (VersionedValue) obj;
            int res = Constants.MEMORY_OBJECT + 8 + 2 * Constants.MEMORY_POINTER +
                      getValMemory(v.value);
            if (v.getOperationId() != 0) {
                res += getValMemory(v.getCommittedValue());
            }
            return res;
        }

        private int getValMemory(Object obj) {
            return obj == null ? 0 : valueType.getMemory(obj);
        }

        @Override
        public int compare(Object a, Object b) {
            if (a == b) {
                return 0;
            }
            VersionedValue one = (VersionedValue) a;
            VersionedValue two = (VersionedValue) b;
            int comp = Long.compare(one.getOperationId(), two.getOperationId());
            if (comp == 0) {
                comp = valueType.compare(one.value, two.value);
            }
            return comp;
        }

        @Override
        public void write(WriteBuffer buff, Object obj) {
            VersionedValue v = (VersionedValue) obj;
            long operationId = v.getOperationId();
            buff.putVarLong(operationId);
            if (operationId == 0) {
                valueType.write(buff, v.value);
            } else {
                Object committedValue = v.getCommittedValue();
                int flags = (v.value == null ? 0 : 1) | (committedValue == null ? 0 : 2);
                buff.put((byte) flags);
                if (v.value != null) {
                    valueType.write(buff, v.value);
                }
                if (committedValue != null) {
                    valueType.write(buff, committedValue);
                }
            }
        }

        @Override
        public Object read(ByteBuffer buff) {
            long operationId = DataUtils.readVarLong(buff);
            if (operationId == 0) {
                return new VersionedValue(valueType.read(buff));
            } else {
                byte flags = buff.get();
                Object value = (flags & 1) != 0 ? valueType.read(buff) : null;
                Object committedValue = (flags & 2) != 0 ? valueType.read(buff) : null;
                return new Uncommitted(operationId, value, committedValue);
            }
        }

        @Override
        public int hashCode() {
            return getClass().hashCode() + valueType.hashCode();
        }

        @Override
        public void save(WriteBuffer buff, DataType metaDataType, Database database) {
            metaDataType.write(buff, valueType);
        }

        @Override
        public void load(ByteBuffer buff, DataType metaDataType, Database database) {
            throw DataUtils.newUnsupportedOperationException("load()");
        }

        @Override
        public Factory getFactory() {
            return FACTORY;
        }

        private static final Factory FACTORY = new Factory();

        public static final class Factory implements StatefulDataType.Factory {

            @Override
            public DataType create(ByteBuffer buff, DataType metaDataType, Database database) {
                DataType valueType = (DataType) metaDataType.read(buff);
                return new Type(valueType);
            }
        }
    }
}
