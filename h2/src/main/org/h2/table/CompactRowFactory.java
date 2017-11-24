package org.h2.table;

import org.h2.bytecode.RowStorage;
import org.h2.bytecode.RowStorageGenerator;
import org.h2.mvstore.type.DataType;
import org.h2.result.Row;
import org.h2.result.RowFactory;
import org.h2.result.SearchRow;
import org.h2.store.DataHandler;
import org.h2.value.CompareMode;
import org.h2.value.Value;
import org.h2.value.ValueNull;

/**
 * Class CompactRowFactory.
 * <UL>
 * <LI> 4/12/17 9:02 PM initial creation
 * </UL>
 *
 * @author <a href='mailto:andrei.tokar@gmail.com'>Andrei Tokar</a>
 */
public final class CompactRowFactory extends RowFactory {
    private final RowStorage instance;
    private final DataType dataType;

    public CompactRowFactory() {
        this(null, null);
    }

    private CompactRowFactory(RowStorage instance, DataType dataType) {
        this.instance = instance;
        this.dataType = dataType;
    }

    @Override
    public RowFactory createRowFactory(CompareMode compareMode, DataHandler handler, Column[] columns, IndexColumn[] indexColumns) {
        int indexes[] = null;
        int sortTypes[] = null;
        if (indexColumns != null) {
            int len = indexColumns.length;
            indexes = new int[len];
            sortTypes = new int[len];
            for (int i = 0; i < len; i++) {
                IndexColumn indexColumn = indexColumns[i];
                indexes[i] = indexColumn.column.getColumnId();
                sortTypes[i] = indexColumn.sortType;
            }
        }
        int types[] = new int[columns.length];
        for (int i = 0; i < types.length; i++) {
            types[i] = columns[i].getType();
        }
        Class<? extends RowStorage> clazz = RowStorageGenerator.generateStorageClass(types, indexes);
        RowStorage templateRow;
        try {
            templateRow = clazz.newInstance();
        } catch (Exception e) {
            throw new IllegalStateException("CompactRowFactory failure ", e);
        }
        if (indexes != null) {
            for (int index : indexes) {
                templateRow.setValue(index, ValueNull.INSTANCE);
            }
        }
        RowStorage.Type dataType = new RowStorage.Type(compareMode, handler, sortTypes, indexes);
        CompactRowFactory factory = new CompactRowFactory(templateRow, dataType);
        dataType.setRowFactory(factory);
        return factory;
    }

    @Override
    public Row createRow(Value[] data, int memory) {
        if(instance == null)
        {
            return RowFactory.getDefaultRowFactory().createRow(data, memory);
        }
        RowStorage rowStorage = instance.clone();
        rowStorage.setValues(data);
        return rowStorage;
    }

    @Override
    public SearchRow createRow() {
//        if(instance == null)
//        {
//            return RowFactory.getDefaultRowFactory().createRow();
//        }
        return instance.clone();
    }

    @Override
    public DataType getDataType() {
        if(dataType == null)
        {
            throw new IllegalStateException();
//            return RowFactory.getDefaultRowFactory().getDataType();
        }
        return dataType;
    }
}
