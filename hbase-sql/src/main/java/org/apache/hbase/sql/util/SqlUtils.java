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


}
