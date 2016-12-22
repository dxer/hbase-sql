package org.dxer.hbase.util;

import org.apache.hadoop.hbase.util.Strings;

/**
 * Created by linghf on 2016/12/21.
 */

public class RowKeyUtil {

    public String reverse(String s) {
        String ret = null;
        if (!Strings.isEmpty(s)) {
            StringBuilder sb = new StringBuilder(s);
            ret = sb.reverse().toString();
        }
        return ret;
    }


}
