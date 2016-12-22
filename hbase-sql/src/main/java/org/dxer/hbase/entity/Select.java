package org.dxer.hbase.entity;

import java.util.Map;

/**
 * Created by linghf on 2016/12/21.
 */

public class Select {

    private String id;

    private String sql;

    private Map<String, String> params;

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getSql() {
        return sql;
    }

    public void setSql(String sql) {
        this.sql = sql;
    }

    public Map<String, String> getParams() {
        return params;
    }

    public void setParams(Map<String, String> params) {
        this.params = params;
    }
}
