package org.h2.table;

import org.h2.bytecode.RowStorage;
import org.h2.bytecode.RowStorageGenerator;
import org.h2.result.Row;
import org.h2.result.RowFactory;
import org.h2.value.Value;

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

    public CompactRowFactory() {
        instance = null;
    }

    private CompactRowFactory(RowStorage instance) {
        this.instance = instance;
    }

    @Override
    public RowFactory createRowFactory(Column[] columns) {
        int types[] = new int[columns.length];
        for (int i = 0; i < types.length; i++) {
            types[i] = columns[i].getType();
        }
        Class<? extends RowStorage> clazz = RowStorageGenerator.generateStorageClass(types);
        RowStorage rowStorage;
        try {
            rowStorage = clazz.newInstance();
        } catch (Exception e) {
            throw new IllegalStateException("CompactRowFactory failure ", e);
        }
        return new CompactRowFactory(rowStorage);
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
}
