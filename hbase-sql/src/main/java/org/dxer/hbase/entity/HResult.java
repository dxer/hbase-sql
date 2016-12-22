package org.dxer.hbase.entity;


import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.serializer.SerializerFeature;
import com.google.common.base.Strings;
import org.dxer.hbase.HBaseSqlContants;

import java.io.Serializable;
import java.util.Map;

/**
 * Created by linghf on 2016/8/31.
 */

public class HResult implements Serializable {

    private static final long serialVersionUID = 5826282291975964435L;

    private Map<String, Object> resultMap = null;

    private String getColumnGroup(String cf, String column) {
        if (!Strings.isNullOrEmpty(cf) && !Strings.isNullOrEmpty(column)) {
            return cf + "." + column;
        }
        return null;
    }

    public String getString(String cf, String column) {
        String columnGroup = getColumnGroup(cf, column);
        if (!Strings.isNullOrEmpty(columnGroup) && resultMap != null) {
            Object o = resultMap.get(cf + column);
            if (o != null) {
                return (String) o;
            }
        }
        return null;
    }

    public Integer getInt(String cf, String column) {
        String value = getString(cf, column);
        if (!Strings.isNullOrEmpty(value)) {
            return Integer.parseInt(value);
        }
        return null;
    }

    public Long getLong(String cf, String column) {
        String value = getString(cf, column);
        if (!Strings.isNullOrEmpty(value)) {
            return Long.parseLong(value);
        }
        return null;
    }

    public Double getDouble(String cf, String column) {
        String value = getString(cf, column);
        if (!Strings.isNullOrEmpty(value)) {
            return Double.parseDouble(value);
        }
        return null;
    }

    public Float getFloat(String cf, String column) {
        String value = getString(cf, column);
        if (!Strings.isNullOrEmpty(value)) {
            return Float.parseFloat(value);
        }
        return null;
    }

    public String getRowKey() {
        return (String) resultMap.get(HBaseSqlContants.ROW_KEY);
    }

    public Long geTimestamp(String columnGroup) {
        if (!Strings.isNullOrEmpty(columnGroup)) {
            Object o = resultMap.get(columnGroup + HBaseSqlContants.TS_SUFFIX);
            if (o != null) {
                return (Long) o;
            }
        }
        return null;
    }

    public Map<String, Object> getResultMap() {
        return resultMap;
    }

    public void setResultMap(Map<String, Object> resultMap) {
        this.resultMap = resultMap;
    }

    @Override
    public String toString() {
        return JSON.toJSONString(resultMap, SerializerFeature.DisableCircularReferenceDetect);
    }

}
