package org.dxer.hbase.entity;


import java.util.HashMap;
import java.util.Map;

/**
 * Created by linghf on 2016/12/21.
 */

public class Table {

    private String tableName;

    private RowKey rowKey;

    private Map<String, ColumnFamily> columnFamilyMap = new HashMap<String, ColumnFamily>();

    public String getTableName() {
        return tableName;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    public RowKey getRowKey() {
        return rowKey;
    }

    public void setRowKey(RowKey rowKey) {
        this.rowKey = rowKey;
    }

    public Map<String, ColumnFamily> getColumnFamilyMap() {
        return columnFamilyMap;
    }

    public void setColumnFamilyMap(Map<String, ColumnFamily> cloumnFamilyMap) {
        this.columnFamilyMap = cloumnFamilyMap;
    }

    public void addColumnFamily(ColumnFamily columnFamily) {
        columnFamilyMap.put(columnFamily.getName(), columnFamily);
    }
}
