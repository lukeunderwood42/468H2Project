/*
 * Copyright 2004-2010 H2 Group. Multiple-Licensed under the H2 License,
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.samples;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import org.h2.constant.SysProperties;
import org.h2.tools.DeleteDbFiles;

/**
 * Demonstrates the benefit of using the CREATE TABLE ... AS SELECT
 * optimization.
 */
public class DirectInsert {

    /**
     * This method is called when executing this sample application from the
     * command line.
     *
     * @param args the command line parameters
     */
    public static void main(String... args) throws Exception {
        Class.forName("org.h2.Driver");
        DeleteDbFiles.execute("~", "test", true);
        Connection conn = DriverManager.getConnection("jdbc:h2:~/test;LOG=0", "sa", "");
        Statement stat = conn.createStatement();
        initialInsert(conn, stat, 200000);
        for (int i = 0; i < 3; i++) {
            createAsSelect(stat, true);
            createAsSelect(stat, false);
        }
    }

    private static void initialInsert(Connection conn, Statement stat, int len) throws SQLException {
        stat.execute("DROP TABLE IF EXISTS TEST");
        stat.execute("CREATE TABLE TEST(ID INT PRIMARY KEY, NAME VARCHAR)");
        PreparedStatement prep = conn.prepareStatement("INSERT INTO TEST VALUES(?, 'Test' || SPACE(100))");
        long time = System.currentTimeMillis();
        for (int i = 0; i < len; i++) {
            long now = System.currentTimeMillis();
            if (now > time + 1000) {
                time = now;
                System.out.println("Inserting " + (100L * i / len) + "%");
            }
            prep.setInt(1, i);
            prep.execute();
        }
        conn.commit();
    }

    private static void createAsSelect(Statement stat, boolean optimize) throws SQLException {
        SysProperties.optimizeInsertFromSelect = optimize;
        stat.execute("DROP TABLE IF EXISTS TEST2");
        System.out.println("CREATE TABLE ... AS SELECT " + (optimize ? "(optimized)" : ""));
        long time = System.currentTimeMillis();
        stat.execute("CREATE TABLE TEST2 AS SELECT * FROM TEST");
        System.out.printf("%.3f sec.\n", (System.currentTimeMillis() - time) / 1000.0);
        stat.execute("INSERT INTO TEST2 SELECT * FROM TEST2");
    }

}