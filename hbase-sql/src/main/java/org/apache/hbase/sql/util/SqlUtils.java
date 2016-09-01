package org.apache.hbase.sql.util;

import com.google.common.base.Strings;

/**
 * Created by linghf on 2016/8/29.
 */

public class SqlUtils {


    public static String[] getColumngroup(String columnGroupStr) {
        if (!Strings.isNullOrEmpty(columnGroupStr)) {
            String[] strs = columnGroupStr.split("\\.");
            if (strs != null && strs.length == 2) {
                return strs;
            }
        }
        return null;
    }

    public static String getColumnFamily(String columnGroupStr) {
        String[] strs = getColumngroup(columnGroupStr);
        if (strs != null) {
            return strs[0];
        }
        return null;
    }


    public static String getColumn(String columnGroupStr) {
        String[] strs = getColumngroup(columnGroupStr);
        if (strs != null) {
            return strs[1];
        }
        return null;
    }

    public static String ex(String str) {
        if (!Strings.isNullOrEmpty(str)) {
            str = str.trim();
            if (str.startsWith("'") || str.startsWith("\"")) {
                str = str.substring(1);
            }

            if (str.endsWith("'") || str.endsWith("\"")) {
                str = str.substring(0, str.length() - 1);
            }
        }
        return str;
    }

}
