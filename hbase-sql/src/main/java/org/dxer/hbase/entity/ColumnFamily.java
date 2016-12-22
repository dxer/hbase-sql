package org.dxer.hbase.entity;

import java.util.HashSet;
import java.util.Set;

/**
 * Created by linghf on 2016/12/21.
 */

public class ColumnFamily {
    private String name;

    private Set<String> columns = new HashSet<String>();

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public Set<String> getColumns() {
        return columns;
    }

    public void setColumns(Set<String> columns) {
        this.columns = columns;
    }

    public void addColumn(String column) {
        columns.add(column);
    }
}
