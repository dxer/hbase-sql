package org.apache.hbase.sql.result;

import com.google.common.base.Strings;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hbase.sql.visitor.SqlContants;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by linghf on 2016/8/31.
 */

public class Result {

    public static final int RETURN_RK = 0x1;

    public static final int ONLY_RETURN_RK = 0x2;

    private static final String TS_SUFFIX = "._ts";

    private static final String ROW_KEY = SqlContants.ROW_KEY;


    private Map<String, Object> map = null;


    public String getString(String columnGroup) {
        if (!Strings.isNullOrEmpty(columnGroup) && map != null) {
            Object o = map.get(columnGroup);
            if (o != null) {
                return (String) o;
            }
        }
        return null;
    }

    public Integer getInt(String columnGroup) {
        String value = getString(columnGroup);
        if (!Strings.isNullOrEmpty(value)) {
            return Integer.parseInt(value);
        }
        return null;
    }

    public Long getLong(String columnGroup) {
        String value = getString(columnGroup);
        if (!Strings.isNullOrEmpty(value)) {
            return Long.parseLong(value);
        }
        return null;
    }


    public Double getDouble(String columnGroup) {
        String value = getString(columnGroup);
        if (!Strings.isNullOrEmpty(value)) {
            return Double.parseDouble(value);
        }
        return null;
    }

    public Float getFloat(String columnGroup) {
        String value = getString(columnGroup);
        if (!Strings.isNullOrEmpty(value)) {
            return Float.parseFloat(value);
        }
        return null;
    }

    public String getRowKey() {
        return (String) map.get(ROW_KEY);
    }

    public long geTimestamp(String columnGroup) {
        if (!Strings.isNullOrEmpty(columnGroup)) {
            Object o = map.get(columnGroup + TS_SUFFIX);
            if (o != null) {
                return (Long) o;
            }
        }
        return 0;
    }

    @Override
    public String toString() {
        return map.toString();
    }

    public void setResult(org.apache.hadoop.hbase.client.Result result, int type) {
        if (result != null && !result.isEmpty()) {
            List<Cell> cells = result.listCells();
            if (cells != null) {
                map = new HashMap<String, Object>();

                if (type == RETURN_RK || type == ONLY_RETURN_RK) {
                    String rowkey = Bytes.toString(result.getRow());
                    map.put(ROW_KEY, rowkey);
                }

                if (type != ONLY_RETURN_RK) {
                    for (Cell c : cells) {
                        String family = Bytes.toString(CellUtil.cloneFamily(c)); // family
                        String qualifier = Bytes.toString(CellUtil.cloneQualifier(c)); // qualifier
                        String value = Bytes.toString(CellUtil.cloneValue(c)); // value
                        long ts = c.getTimestamp(); // 时间戳

                        map.put(family + "." + qualifier, value);
                        map.put(family + "." + qualifier + TS_SUFFIX, ts);
                    }
                }
            }
        }
    }

}
