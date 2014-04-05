/*
 * Copyright 2004-2007 H2 Group. Licensed under the H2 License, Version 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.test.synth;

import java.sql.ResultSet;
import java.sql.SQLException;

class Row implements Comparable {
    private Value[] data;

    public Row(TestSynth config, ResultSet rs, int len) throws SQLException {
        data = new Value[len];
        for (int i = 0; i < len; i++) {
            data[i] = Value.read(config, rs, i + 1);
        }
    }

    public String toString() {
        String s = "";
        for (int i = 0; i < data.length; i++) {
            Object o = data[i];
            s += o == null ? "NULL" : o.toString();
            s += "; ";
        }
        return s;
    }

    public int compareTo(Object o) {
        Row r2 = (Row) o;
        int result = 0;
        for (int i = 0; i < data.length && result == 0; i++) {
            Object o1 = data[i];
            Object o2 = r2.data[i];
            if (o1 == null) {
                result = (o2 == null) ? 0 : -1;
            } else if (o2 == null) {
                result = 1;
            } else {
                result = o1.toString().compareTo(o2.toString());
            }
        }
        return result;
    }

}