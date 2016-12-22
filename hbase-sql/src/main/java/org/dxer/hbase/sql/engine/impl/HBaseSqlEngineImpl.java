package org.dxer.hbase.sql.engine.impl;

import com.google.common.base.Strings;
import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.expression.operators.relational.ExpressionList;
import net.sf.jsqlparser.parser.CCJSqlParserManager;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.statement.delete.Delete;
import net.sf.jsqlparser.statement.insert.Insert;
import net.sf.jsqlparser.statement.select.Select;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.client.HConnection;
import org.apache.hadoop.hbase.client.HConnectionManager;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;
import org.dxer.hbase.HBaseSqlContants;
import org.dxer.hbase.client.HBaseConfig;
import org.dxer.hbase.client.HBaseUtils;
import org.dxer.hbase.entity.HResult;
import org.dxer.hbase.sql.engine.HBaseSqlEngine;
import org.dxer.hbase.sql.util.ExpressionUtil;
import org.dxer.hbase.sql.visitor.DeleteSqlVisitor;
import org.dxer.hbase.sql.visitor.SelectSqlVisitor;

import java.io.IOException;
import java.io.StringReader;
import java.sql.SQLSyntaxErrorException;
import java.util.*;

/**
 * Created by linghf on 2016/8/29.
 */

public class HBaseSqlEngineImpl implements HBaseSqlEngine {

    /**
     * 获取HConnection
     *
     * @return
     */
    public HConnection getHConnection() {
        HConnection connection = null;
        try {
            connection = HConnectionManager.createConnection(HBaseConfig.getConfiguration());
        } catch (IOException e) {
            e.printStackTrace();
        }

        return connection;
    }

    private byte[] getBytes(String s) {
        if (!Strings.isNullOrEmpty(s)) {
            return Bytes.toBytes(s);
        }
        return null;
    }


    /**
     * select
     *
     * @param sql
     * @return
     * @throws Exception
     */
    @SuppressWarnings("unchecked")
    public List<HResult> select(String sql) throws Exception {
        HConnection connection = getHConnection();
        if (connection == null) {
            throw new RuntimeException("HConnection is null");
        }

        SelectSqlVisitor sqlVisitor = parseSql(sql);
        String tableName = sqlVisitor.getTableName();

        String rowKey = null;
        List<String> rowKeys = null;
        Map<String, List<String>> queryColumnMap = sqlVisitor.getQueryColumnMap();
        Set<String> queryColumns = sqlVisitor.getQueryColumns();


        List<Result> results = null;

        Scan scan = sqlVisitor.getScanner();

        Long offset = sqlVisitor.getOffset();
        Long rowCount = sqlVisitor.getRowCount();

        System.out.println(scan);
        results = HBaseUtils.getResults(connection, tableName, scan, offset, rowCount, queryColumnMap);

        List<HResult> hResultList = null;
        if (results != null) {
            hResultList = new ArrayList<HResult>();
            for (Result result : results) {
                HResult hResult = getHResult(result, queryColumns);
                if (hResult != null) {
                    hResultList.add(hResult);
                }
            }
        }
        return hResultList;
    }

    private HResult getHResult(Result result) {
        return getHResult(result, null);
    }

    private HResult getHResult(Result result, Set<String> queryColumns) {
        HResult hResult = null;
        if (result != null && !result.isEmpty()) {
            hResult = new HResult();
            Map<String, Object> resultMap = null;
            List<Cell> cells = result.listCells();
            if (cells != null) {
                resultMap = new HashMap<String, Object>();

                if (queryColumns != null &&
                        (queryColumns.contains(HBaseSqlContants.ROW_KEY) || queryColumns
                                .contains(HBaseSqlContants.ASTERISK))) { // 设置rowkey
                    String rowkey = Bytes.toString(result.getRow());
                    resultMap.put(HBaseSqlContants.ROW_KEY, rowkey);
                }

                for (Cell cell : cells) {
                    String family = Bytes.toString(CellUtil.cloneFamily(cell)); // family
                    String qualifier = Bytes.toString(CellUtil.cloneQualifier(cell)); // qualifier

                    String column = family + "." + qualifier;
                    if (queryColumns == null ||
                            (!queryColumns.contains(column) && !queryColumns.contains(HBaseSqlContants.ASTERISK))) {
                        continue;
                    }

                    String value = Bytes.toString(CellUtil.cloneValue(cell)); // value

                    resultMap.put(family + "." + qualifier, value);

                    String columnWithTS = family + "." + qualifier + HBaseSqlContants.TS_SUFFIX;

                    if (queryColumns != null && queryColumns.contains(columnWithTS)) {
                        long ts = cell.getTimestamp(); // 时间戳
                        resultMap.put(columnWithTS, ts);
                    }
                }
                hResult.setResultMap(resultMap);
            }
        }
        return hResult;
    }

    private SelectSqlVisitor parseSql(String sql) throws SQLSyntaxErrorException {
        CCJSqlParserManager parserManager = new CCJSqlParserManager();
        SelectSqlVisitor sqlVisitor = null;
        try {
            Select select = (Select) parserManager.parse(new StringReader(sql));
            sqlVisitor = new SelectSqlVisitor(select);
        } catch (Exception e) {
            throw new SQLSyntaxErrorException(sql, e);
        }
        return sqlVisitor;
    }

    /**
     * insert
     *
     * @param sql
     * @throws Exception
     */
    public void insert(String sql) throws Exception {
        CCJSqlParserManager parserManager = new CCJSqlParserManager();
        Insert insert = (Insert) parserManager.parse(new StringReader(sql));

        String tableName = insert.getTable().getName();

        Map<String, String> map = new HashMap<String, String>();

        int size = insert.getColumns().size();
        for (int i = 0; i < size; i++) {
            String key = ((Column) insert.getColumns().get(i)).getColumnName();
            Object o = ((ExpressionList) insert.getItemsList()).getExpressions().get(i);
            String value = ExpressionUtil.getString(o);

            if (!Strings.isNullOrEmpty(key) && !Strings.isNullOrEmpty(value)) {

                if (HBaseSqlContants.ROW_KEY.equals(key.toUpperCase())) {
                    map.put(HBaseSqlContants.ROW_KEY, value);
                } else {
                    map.put(key, value);
                }
            }
        }
        HConnection connection = null;
        HBaseUtils.putMap(connection, tableName, map);
    }

    /**
     * insert
     *
     * @param sql
     * @param map
     * @throws Exception
     */
    public void insert(String sql, HashMap<String, String> map) throws Exception {
        CCJSqlParserManager parserManager = new CCJSqlParserManager();
        Insert insert = (Insert) parserManager.parse(new StringReader(sql));

        String tableName = insert.getTable().getName();
        HConnection connection = null;
        HBaseUtils.putMap(connection, tableName, map);
    }

    public void insert(String sql, ArrayList<HashMap<String, String>> list) throws Exception {
        CCJSqlParserManager parserManager = new CCJSqlParserManager();
        Insert insert = (Insert) parserManager.parse(new StringReader(sql));

        String tableName = insert.getTable().getName();
        HConnection connection = null;
        HBaseUtils.putMaps(connection, tableName, list);
    }

    private DeleteSqlVisitor parseDeleteSqlVisitor(String sql) throws JSQLParserException {
        CCJSqlParserManager parserManager = new CCJSqlParserManager();
        Delete delete = (Delete) parserManager.parse(new StringReader(sql));
        DeleteSqlVisitor sqlVisitor = new DeleteSqlVisitor(delete);
        return sqlVisitor;
    }

    /**
     *
     */
    public void del(String sql) throws Exception {
        DeleteSqlVisitor sqlVisitor = parseDeleteSqlVisitor(sql);

        String tableName = sqlVisitor.getTableName(); // tableName

        Set<String> rowkeys = sqlVisitor.getRowkeys(); // 需要删除的rowkey

        Map<String, List<String>> columnMap = sqlVisitor.getColumnMap();

        if (!Strings.isNullOrEmpty(tableName) && rowkeys != null && rowkeys.size() > 0) {
            HConnection connection = getHConnection();
            HBaseUtils.deleteColumn(connection, tableName, new ArrayList<String>(rowkeys), columnMap);
        }
    }

    /**
     *
     */
    public void del(String sql, List<String> rowkeys) throws Exception {
        DeleteSqlVisitor sqlVisitor = parseDeleteSqlVisitor(sql);

        String tableName = sqlVisitor.getTableName();
        boolean delAll = sqlVisitor.isDelAll();

        Map<String, List<String>> columnMap = sqlVisitor.getColumnMap();

        if (!Strings.isNullOrEmpty(tableName)) {
            HConnection connection = null;
            if (delAll) { // 删除所有的column
                HBaseUtils.deleteAllColumn(connection, tableName, rowkeys);
            } else { // 删除指定的column
                if (columnMap != null && columnMap.size() > 0) {
                    HBaseUtils.deleteColumn(connection, tableName, rowkeys, columnMap);
                }
            }
        }
    }

    public void del(String sql, List<String> rowkeys, HashMap<String, ArrayList<String>> columnMap) throws Exception {

    }
}
