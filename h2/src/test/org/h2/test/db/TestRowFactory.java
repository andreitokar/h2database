/*
 * Copyright 2004-2014 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.test.db;

import java.io.File;
import java.io.FileOutputStream;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.concurrent.atomic.AtomicInteger;
import org.h2.bytecode.RowStorage;
import org.h2.bytecode.RowStorageGenerator;
import org.h2.mvstore.type.DataType;
import org.h2.result.Row;
import org.h2.result.RowFactory;
import org.h2.result.RowImpl;
import org.h2.result.SearchRow;
import org.h2.result.SortOrder;
import org.h2.table.CompactRowFactory;
import org.h2.test.TestBase;
import org.h2.value.CompareMode;
import org.h2.value.Value;
import org.h2.value.ValueDate;
import org.h2.value.ValueDecimal;
import org.h2.value.ValueDouble;
import org.h2.value.ValueFloat;
import org.h2.value.ValueInt;
import org.h2.value.ValueLong;
import org.h2.value.ValueNull;
import org.h2.value.ValueString;

/**
 * Test {@link RowFactory} setting.
 *
 * @author Sergi Vladykin
 */
public class TestRowFactory extends TestBase {

    /**
     * Run just this test.
     *
     * @param a ignored
     */
    public static void main(String[] a) throws Exception {
        TestBase.createCaller().init().test();
    }

    @Override
    public void test() throws Exception {
        tetRowStorage();
        testCompactRowFactory();
        testMyRowFactory();
    }

    private void testMyRowFactory() throws Exception {
        deleteDb("rowFactory");
        Connection conn = getConnection("rowFactory;ROW_FACTORY=\"" +
                MyTestRowFactory.class.getName() + '"');
        Statement stat = conn.createStatement();
        stat.execute("create table t1(id int, name varchar)");
        for (int i = 0; i < 1000; i++) {
            stat.execute("insert into t1 values(" + i + ", 'name')");
        }
        assertTrue(MyTestRowFactory.COUNTER.get() >= 1000);
        conn.close();
        deleteDb("rowFactory");
    }

    private void tetRowStorage() {
        int[] valueTypes = { Value.INT, Value.STRING, Value.UNKNOWN, Value.LONG, Value.DOUBLE,
                             Value.FLOAT, Value.DECIMAL, Value.STRING_FIXED };
        Class<? extends RowStorage> cls = generateClass(valueTypes, null);
        Class<? extends RowStorage> icls = generateClass(valueTypes, new int[]{3, 0, 1});

        try {
            Constructor<? extends RowStorage> constructor = cls.getConstructor();
            Value[] initargs = {
                    ValueInt.get(3),
                    ValueString.get("Hello"),
                    ValueDate.parse("2017-08-04"),
                    ValueLong.get(77),
                    ValueDouble.get(3.62),
                    ValueFloat.get(3.62f),
                    ValueDecimal.get(BigDecimal.TEN),
                    ValueString.get("ABC")
            };
            RowStorage row = constructor.newInstance();
            row.setValues(initargs);
            row.setKey(12345);
            assertEquals("Row{12345/0 3, 'Hello', DATE '2017-08-04', 77, 3.62, 3.62, 10, 'ABC'}", row.toString());

            RowStorage rowTwo = row.clone();
            rowTwo.setValue(0, ValueInt.get(5));
            rowTwo.setValue(1, ValueString.get("World"));
            rowTwo.setValue(2, ValueDate.parse("2001-09-11"));
            rowTwo.setValue(3, ValueLong.get(999));
            rowTwo.setValue(4, ValueDouble.get(4.12));
            assertEquals("Row{12345/0 5, 'World', DATE '2001-09-11', 999, 4.12, 3.62, 10, 'ABC'}", rowTwo.toString());
            assertFalse(rowTwo.isNull(5));
            rowTwo.setValue(5, ValueNull.INSTANCE);
            assertTrue(rowTwo.isNull(5));
            assertFalse(rowTwo.isNull(6));
            rowTwo.setValue(6, ValueNull.INSTANCE);
            assertTrue(rowTwo.isNull(6));
            rowTwo.setValue(7, ValueNull.INSTANCE);
            assertEquals("Row{12345/0 5, 'World', DATE '2001-09-11', 999, 4.12, NULL, NULL, NULL}", rowTwo.toString());


            CompareMode compareMode = CompareMode.getInstance(null, 0);
            RowStorage.Type type = new RowStorage.Type(compareMode, null, null);
            assertEquals(-1, type.compare(row, rowTwo));
            assertEquals(1, type.compare(rowTwo, row));

            RowStorage irowTwo = icls.getConstructor().newInstance();
            irowTwo.setValue(0, ValueInt.get(5));
            irowTwo.setValue(1, ValueString.get("World"));
            irowTwo.setValue(3, ValueLong.get(999));
            try {
                irowTwo.setValue(4, ValueDouble.get(4.12));
                fail();
            } catch(Throwable ignore) {/**/}
            irowTwo.setKey(987);
            assertEquals("Row{987/0 5, 'World', null, 999, null, null, null, null}", irowTwo.toString());
            RowStorage.Type itype = new RowStorage.Type(compareMode, null, new int[]{SortOrder.ASCENDING, SortOrder.ASCENDING, SortOrder.ASCENDING});
            assertEquals(0, itype.compare(irowTwo, irowTwo));
            assertEquals(-1, itype.compare(irowTwo, rowTwo));
            irowTwo.setKey(12345);
            assertEquals(0, itype.compare(irowTwo, rowTwo));
            assertEquals(1, itype.compare(irowTwo, row));

            RowStorage irow = icls.getConstructor().newInstance();
            irow.copyFrom(row);
            assertEquals("Row{12345/0 3, 'Hello', null, 77, null, null, null, null}", irow.toString());
            assertEquals(-1, itype.compare(irow, irowTwo));
            assertEquals(0, itype.compare(irow, row));
            assertEquals(0, itype.compare(row, irow));

            row.setValue(3, null);
            assertTrue(row.isNull(3));
            assertNull(row.getValue(3));
            assertEquals("Row{12345/0 3, 'Hello', DATE '2017-08-04', null, 3.62, 3.62, 10, 'ABC'}", row.toString());
            irow.copyFrom(row);
            assertEquals("Row{12345/0 3, 'Hello', null, null, null, null, null, null}", irow.toString());
            assertEquals(-1, itype.compare(irow, irowTwo));
            assertEquals(1, itype.compare(irow, row));
            assertEquals(0, itype.compare(row, irow));
        } catch (InstantiationException | IllegalAccessException | InvocationTargetException | NoSuchMethodException e) {
            e.printStackTrace();
        }
    }

    private static Class<? extends RowStorage> generateClass(int[] valueTypes, int indexes[]) {
        String className = RowStorageGenerator.getClassName(valueTypes, indexes);
        byte classBytes[] = RowStorageGenerator.generateClassDefinition(valueTypes, indexes, className);
        className = className.substring(className.lastIndexOf('.') + 1);
        try (FileOutputStream out = new FileOutputStream(new File(new File("generated"), className + ".class"))) {
            out.write(classBytes);
        } catch (java.io.IOException e) {
            e.printStackTrace();
        }
        return RowStorageGenerator.generateStorageClass(valueTypes, indexes);
    }


    private void testCompactRowFactory() throws Exception {
        deleteDb("rowFactory");
        Connection conn = getConnection("rowFactory;ROW_FACTORY=\"" + CompactRowFactory.class.getName() + '"');
        Statement stat = conn.createStatement();
        stat.execute("create table t1(id int, name varchar)");
        stat.execute("create index name_idx on t1(name)");
        for (int i = 0; i < 1000; i++) {
            stat.execute("insert into t1 values(" + i + ", 'name_"+i+"')");
        }
        ResultSet resultSet = stat.executeQuery("select id from t1 where name='name_500'");
        assertTrue(resultSet.next());
//        assertEquals(500, resultSet.getInt("id"));
        resultSet.close();
        conn.close();
        deleteDb("rowFactory");
    }

    /**
     * Test row factory.
     */
    public static class MyTestRowFactory extends RowFactory {

        /**
         * A simple counter.
         */
        private static final AtomicInteger COUNTER = new AtomicInteger();

        @Override
        public Row createRow(Value[] data, int memory) {
            COUNTER.incrementAndGet();
            return new RowImpl(data, memory);
        }

        @Override
        public SearchRow createRow() {
            return null;
        }

        @Override
        public DataType getDataType() {
            return getDefaultRowFactory().getDataType();
        }
    }
}
