/*
 * Copyright 2004-2014 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.test.db;

import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.LineNumberReader;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import org.h2.test.TestBase;
import org.h2.util.ScriptReader;

/**
 * This test runs a simple SQL script file and compares the output with the
 * expected output.
 */
public class TestScriptSimple extends TestBase {

    private Connection conn;

    /**
     * Run just this test.
     *
     * @param a ignored
     */
    public static void main(String... a) throws Exception {
        TestBase.createCaller().init().test();
    }

    @Override
    public void test() throws Exception {
        deleteDb(getTestName());
        conn = getConnection(getTestName());
        Statement stmt = conn.createStatement();
        stmt.executeUpdate("create table a(x int, y int)");
        stmt.executeUpdate("create unique index a_xy on a(x, y)");
        stmt.executeUpdate("insert into a values(null, null), (null, 0), (0, null), (0, 0)");
        int count1 = stmt.executeUpdate("delete from a where x is null and y is null");
        assertEquals(1, count1);
        int count2 = stmt.executeUpdate("delete from a where x is null and y = 0");
        assertEquals(1, count2);
        int count3 = stmt.executeUpdate("delete from a where x = 0 and y is null");
        assertEquals(1, count3);
        int count4 = stmt.executeUpdate("delete from a where x = 0 and y = 0");
        assertEquals(1, count4);
    }

//    @Override
    public void _test() throws Exception {
        if (config.memory || config.big || config.networked) {
            return;
        }
        deleteDb("scriptSimple");
        reconnect();
        String inFile = "org/h2/test/testSimple.in.txt";
        InputStream is = getClass().getClassLoader().getResourceAsStream(inFile);
        LineNumberReader lineReader = new LineNumberReader(
                new InputStreamReader(is, "Cp1252"));
        try (ScriptReader reader = new ScriptReader(lineReader)) {
            while (true) {
                String sql = reader.readStatement();
                if (sql == null) {
                    break;
                }
                sql = sql.trim();
                try {
                    if ("@reconnect".equals(sql.toLowerCase())) {
                        reconnect();
                    } else if (sql.length() == 0) {
                        // ignore
                    } else if (sql.toLowerCase().startsWith("select")) {
                        ResultSet rs = conn.createStatement().executeQuery(sql);
                        while (rs.next()) {
                            String expected = reader.readStatement().trim();
                            String got = "> " + rs.getString(1);
                            assertEquals(sql, expected, got);
                        }
                    } else {
                        conn.createStatement().execute(sql);
                    }
                } catch (SQLException e) {
                    System.out.println(sql);
                    throw e;
                }
            }
        }
        conn.close();
        deleteDb("scriptSimple");
    }

    private void reconnect() throws SQLException {
        if (conn != null) {
            conn.close();
        }
        conn = getConnection("scriptSimple");
    }

}
