/*
 * Copyright 2004-2014 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.mvstore;

import java.nio.ByteBuffer;
import java.util.Arrays;
import org.h2.engine.Database;
import org.h2.mvstore.db.StatefulDataType;
import org.h2.mvstore.db.ValueDataType;
import org.h2.mvstore.type.DataType;
import org.h2.result.RowFactory;
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
public final class RowDataType extends BasicDataType<SearchRow> implements StatefulDataType {

    private final ValueDataType valueDataType;
    private final int[]         sortTypes;
    private final int[]         indexes;

    public RowDataType(CompareMode compareMode, DataHandler handler, int[] sortTypes, int[] indexes) {
        this.valueDataType = new ValueDataType(compareMode, handler, sortTypes);
        this.sortTypes = sortTypes;
        this.indexes = indexes;
    }

    public int[] getIndexes() {
        return indexes;
    }

    public RowFactory getRowFactory() {
        return valueDataType.getRowFactory();
    }

    public void setRowFactory(RowFactory rowFactory) {
        valueDataType.setRowFactory(rowFactory);
    }

    @Override
    public Object createStorage(int capacity) {
        return new SearchRow[capacity];
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
                int comp = valueDataType.compareValues(a.getValue(i), b.getValue(i), sortTypes[i]);
                if (comp != 0) {
                    return comp;
                }
            }
            return 0;
        } else {
            assert sortTypes.length == indexes.length;
            for (int i = 0; i < indexes.length; i++) {
                int indx = indexes[i];
                int comp = valueDataType.compareValues(a.getValue(indx), b.getValue(indx), sortTypes[i]);
                if (comp != 0) {
                    return comp;
                }
            }
            return Long.compare(a.getKey(), b.getKey());
        }
    }

    @Override
    public int binarySearch(Object key, Object storage, int size, int initialGuess) {
        return binarySearch((SearchRow)key, (SearchRow[])storage, size, initialGuess);
    }

    public int binarySearch(SearchRow key, SearchRow keys[], int size, int initialGuess) {
        int low = 0;
        int high = size - 1;
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
    public int getMemory(Object obj) {
        SearchRow row = (SearchRow) obj;
        return row.getMemory();
    }

    @Override
    public SearchRow read(ByteBuffer buff) {
        SearchRow row = valueDataType.getRowFactory().createRow();
        row.setKey(DataUtils.readVarLong(buff));
        if (indexes == null) {
            int columnCount = DataUtils.readVarInt(buff);
            for (int i = 0; i < columnCount; i++) {
                row.setValue(i, valueDataType.readValue(buff));
            }
        } else {
            for (int i : indexes) {
                row.setValue(i, valueDataType.readValue(buff));
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
                valueDataType.write(buff, row.getValue(i));
            }
        } else {
            for (int i : indexes) {
                valueDataType.write(buff, row.getValue(i));
            }
        }
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == this) {
            return true;
        } else if (obj == null || obj.getClass() != RowDataType.class) {
            return false;
        }
        RowDataType other = (RowDataType) obj;
        return Arrays.equals(indexes, other.indexes)
//            && Arrays.equals(sortTypes, other.sortTypes)
            && valueDataType.equals(other.valueDataType);
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
            buff.putVarInt(array.length + 1);
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
            int len = DataUtils.readVarInt(buff) - 1;
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
