package org.apache.hbase.sql.engine.impl;

import com.google.common.base.Strings;
import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.expression.operators.relational.ExpressionList;
import net.sf.jsqlparser.parser.CCJSqlParserManager;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.statement.delete.Delete;
import net.sf.jsqlparser.statement.insert.Insert;
import net.sf.jsqlparser.statement.select.Select;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hbase.client.HBaseUtils;
import org.apache.hbase.sql.engine.HBaseSqlEngine;
import org.apache.hbase.sql.result.Result;
import org.apache.hbase.sql.util.VisitorUtils;
import org.apache.hbase.sql.visitor.DeleteSqlVisitor;
import org.apache.hbase.sql.visitor.SelectSqlVisitor;
import org.apache.hbase.sql.visitor.SqlContants;

import java.io.StringReader;
import java.sql.SQLSyntaxErrorException;
import java.util.*;

/**
 * Created by linghf on 2016/8/29.
 */

public class HBaseSqlEngineImpl implements HBaseSqlEngine {


    public List<Result> select(String sql) throws Exception {
        return select(sql, null, null);
    }

    /**
     * select
     *
     * @param sql
     * @param startRow
     * @param stopRow
     * @return
     * @throws Exception
     */
    public List<Result> select(String sql, String startRow, String stopRow) throws Exception {
        SelectSqlVisitor sqlVisitor = parseSql(sql);
        String tableName = sqlVisitor.getTableName();

        Scan scan = sqlVisitor.getScan(startRow, stopRow);

        Map<String, List<String>> columnMap = sqlVisitor.getColumnMap();
        boolean returnRK = sqlVisitor.isReturnRK();
        boolean returnRKOnly = sqlVisitor.isReturnRKOnly();

        int type = 0;
        if (returnRK) {
            type = Result.RETURN_RK;
        }

        if (returnRKOnly) {
            type = Result.ONLY_RETURN_RK;
        }

        List<org.apache.hadoop.hbase.client.Result> results = HBaseUtils.getResult(tableName, scan, columnMap);

        List<Result> resultList = null;
        if (results != null) {
            resultList = new ArrayList<Result>();
            for (org.apache.hadoop.hbase.client.Result result : results) {
                Result r = new Result();
                r.setResult(result, type);

                resultList.add(r);
            }
        }
        return resultList;
    }

    private SelectSqlVisitor parseSql(String sql) throws SQLSyntaxErrorException {
        CCJSqlParserManager parserManager = new CCJSqlParserManager();
        SelectSqlVisitor sqlFinder = null;
        try {
            Select select = (Select) parserManager.parse(new StringReader(sql));
            sqlFinder = new SelectSqlVisitor(select);
        } catch (Exception e) {
            throw new SQLSyntaxErrorException(sql, e);
        }
        return sqlFinder;
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

        String tableName = insert.getTable().getWholeTableName();

        Map<String, String> map = new HashMap<String, String>();

        int size = insert.getColumns().size();
        for (int i = 0; i < size; i++) {
            String key = ((Column) insert.getColumns().get(i)).getWholeColumnName();
            Object o = ((ExpressionList) insert.getItemsList()).getExpressions().get(i);
            String value = VisitorUtils.getString(o);

            if (!Strings.isNullOrEmpty(key) && !Strings.isNullOrEmpty(value)) {

                if (SqlContants.ROW_KEY.equals(key.toUpperCase())) {
                    map.put(SqlContants.ROW_KEY, value);
                } else {
                    map.put(key, value);
                }
            }
        }
        HBaseUtils.putMap(tableName, map);
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

        String tableName = insert.getTable().getWholeTableName();
        HBaseUtils.putMap(tableName, map);
    }

    public void insert(String sql, ArrayList<HashMap<String, String>> list) throws Exception {
        CCJSqlParserManager parserManager = new CCJSqlParserManager();
        Insert insert = (Insert) parserManager.parse(new StringReader(sql));

        String tableName = insert.getTable().getWholeTableName();
        HBaseUtils.putMaps(tableName, list);
    }

    private DeleteSqlVisitor parseDeleteSqlVisitor(String sql) throws JSQLParserException {
        CCJSqlParserManager parserManager = new CCJSqlParserManager();
        Delete delete = (Delete) parserManager.parse(new StringReader(sql));
        DeleteSqlVisitor sqlVisitor = new DeleteSqlVisitor(delete);
        return sqlVisitor;
    }

    public void del(String sql) throws Exception {
        DeleteSqlVisitor sqlVisitor = parseDeleteSqlVisitor(sql);

        String tableName = sqlVisitor.getTableName();

        Set<String> rowkeys = sqlVisitor.getRowkeys();

        Map<String, List<String>> columnMap = sqlVisitor.getColumnMap();

        if (!Strings.isNullOrEmpty(tableName) && rowkeys != null && rowkeys.size() > 0) {
            HBaseUtils.deleteColumn(tableName, new ArrayList<String>(rowkeys), columnMap);
        }
    }

    public void del(String sql, List<String> rowkeys) throws Exception {
        DeleteSqlVisitor sqlVisitor = parseDeleteSqlVisitor(sql);

        String tableName = sqlVisitor.getTableName();
        boolean delAll = sqlVisitor.isDelAll();

        Map<String, List<String>> columnMap = sqlVisitor.getColumnMap();

        if (!Strings.isNullOrEmpty(tableName)) {
            if (delAll) { // 删除所有的column
                HBaseUtils.deleteAllColumn(tableName, rowkeys);
            } else { //删除指定的column
                if (columnMap != null && columnMap.size() > 0) {
                    HBaseUtils.deleteColumn(tableName, rowkeys, columnMap);
                }
            }
        }
    }

    public void del(String sql, List<String> rowkeys, HashMap<String, ArrayList<String>> columnMap) throws Exception {

    }
}
