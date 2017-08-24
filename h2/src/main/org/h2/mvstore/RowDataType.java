/*
 * Copyright 2004-2014 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.mvstore;

import java.nio.ByteBuffer;
import java.util.Arrays;

import org.h2.engine.Constants;
import org.h2.engine.Database;
import org.h2.mvstore.db.StatefulDataType;
import org.h2.mvstore.db.TransactionStore;
import org.h2.mvstore.db.ValueDataType;
import org.h2.mvstore.type.DataType;
import org.h2.mvstore.type.ExtendedDataType;
import org.h2.result.SearchRow;
import org.h2.store.DataHandler;
import org.h2.value.CompareMode;

/**
 * Class RowDataType.
 * <UL>
 * <LI> 8/12/17 10:48 AM initial creation
 * </UL>
 *
 * @author <a href='mailto:andrei.tokar@gmail.com'>Andrei Tokar</a>
 */
public final class RowDataType extends ValueDataType implements ExtendedDataType, StatefulDataType {  //TODO rebase to BasicDataType

    private final int[] indexes;

    public RowDataType(CompareMode compareMode, DataHandler handler, int[] sortTypes, int[] indexes) {
        super(compareMode, handler, sortTypes);
        this.indexes = indexes;
    }

    public int[] getIndexes() {
        return indexes;
    }

    @Override
    public Object createStorage(int size) {
        return new SearchRow[size];
    }

    @Override
    public Object clone(Object storage) {
        return ((SearchRow[])storage).clone();
    }

    @Override
    public int getLength(Object storage) {
        return ((SearchRow[])storage).length;
    }

    @Override
    public Object getValue(Object storage, int indx) {
        return ((SearchRow[])storage)[indx];
    }

    @Override
    public void setValue(Object storage, int indx, Object value) {
        ((SearchRow[])storage)[indx] = (SearchRow)value;
    }

    @Override
    public int getMemorySize(Object storage) {
        int size = getLength(storage) * Constants.MEMORY_POINTER;
        for (SearchRow row : ((SearchRow[]) storage)) {
            size += row.getMemory();
        }
        return size;
    }

    @Override
    public int compare(Object a, Object b) {
        if(a instanceof SearchRow && b instanceof SearchRow) {
            return compare((SearchRow)a, (SearchRow)b);
        }
        return super.compare(a, b);
    }

    public int compare(SearchRow a, SearchRow b) {
        if (a == b) {
            return 0;
        }
        if (indexes == null) {
            int len = a.getColumnCount();
            assert len == b.getColumnCount() : len + " != " + b.getColumnCount();
            for (int i = 0; i < len; i++) {
                int comp = compareValues(a.getValue(i), b.getValue(i), sortTypes[i]);
                if (comp != 0) {
                    return comp;
                }
            }
            return 0;
        } else {
            assert sortTypes.length == indexes.length;
            for (int i = 0; i < indexes.length; i++) {
                int indx = indexes[i];
                int comp = compareValues(a.getValue(indx), b.getValue(indx), sortTypes[i]);
                if (comp != 0) {
                    return comp;
                }
            }
            return Long.compare(a.getKey(), b.getKey());
        }
    }

    @Override
    public int binarySearch(Object key, Object storage, int initialGuess) {
        return binarySearch((SearchRow)key, (SearchRow[])storage, initialGuess);
    }

    public int binarySearch(SearchRow key, SearchRow keys[], int initialGuess) {
        int low = 0, high = keys.length - 1;
        // the cached index minus one, so that
        // for the first time (when cachedCompare is 0),
        // the default value is used
        int x = initialGuess - 1;
        if (x < 0 || x > high) {
            x = high >>> 1;
        }
        while (low <= high) {
            int compare = compare(key, keys[x]);
            if (compare > 0) {
                low = x + 1;
            } else if (compare < 0) {
                high = x - 1;
            } else {
                return x;
            }
            x = (low + high) >>> 1;
        }
        return -(low + 1);
    }

    @Override
    public void writeStorage(WriteBuffer buff, Object storage) {
        SearchRow rows[] = (SearchRow[]) storage;
        for (SearchRow row : rows) {
            write(buff, row);
        }
    }

    @Override
    public void read(ByteBuffer buff, Object storage) {
        SearchRow rows[] = (SearchRow[]) storage;
        for (int i = 0; i < rows.length; i++) {
            rows[i] = read(buff);
        }
    }

    @Override
    public int getMemory(Object obj) {
        SearchRow row = (SearchRow) obj;
        return row.getMemory();
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
    public SearchRow read(ByteBuffer buff) {
        SearchRow row = getRowFactory().createRow();
        row.setKey(DataUtils.readVarInt(buff));
        if (indexes == null) {
            int columnCount = DataUtils.readVarInt(buff);
            for (int i = 0; i < columnCount; i++) {
                row.setValue(i, readValue(buff));
            }
        } else {
            for (int i : indexes) {
                row.setValue(i, readValue(buff));
            }
        }
        return row;
    }

    @Override
    public void write(WriteBuffer buff, Object obj) {
        write(buff, (SearchRow)obj);
    }

    public void write(WriteBuffer buff, SearchRow row) {
        buff.putVarLong(row.getKey());
        if (indexes == null) {
            int columnCount = row.getColumnCount();
            buff.putVarInt(columnCount);
            for (int i = 0; i < columnCount; i++) {
                super.write(buff, row.getValue(i));
            }
        } else {
            for (int i : indexes) {
                super.write(buff, row.getValue(i));
            }
        }
    }

    @Override
    public int hashCode() {
        return super.hashCode() ^ Arrays.hashCode(indexes);
    }

    @Override
    public void save(WriteBuffer buff, DataType metaDataType, Database database) {
        writeIntArray(buff, sortTypes);
        writeIntArray(buff, indexes);
    }

    private static void writeIntArray(WriteBuffer buff, int[] array) {
        if(array == null) {
            buff.putVarInt(0);
        } else {
            buff.putVarInt(array.length);
            for (int i : array) {
                buff.putVarInt(i);
            }
        }
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
            int[] sortTypes = readIntArray(buff);
            int[] indexes = readIntArray(buff);
            CompareMode compareMode = database == null ? CompareMode.getInstance(null, 0) : database.getCompareMode();
            return new RowDataType(compareMode, database, sortTypes, indexes);
        }

        private static int[] readIntArray(ByteBuffer buff) {
            int len = DataUtils.readVarInt(buff);
            if(len < 0) {
                return null;
            }
            int[] res = new int[len];
            for (int i = 0; i < res.length; i++) {
                res[i] = DataUtils.readVarInt(buff);
            }
            return res;
        }
    }
}
