package org.apache.hbase.sql.engine.impl;

import com.google.common.base.Strings;
import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.expression.DoubleValue;
import net.sf.jsqlparser.expression.LongValue;
import net.sf.jsqlparser.expression.StringValue;
import net.sf.jsqlparser.expression.operators.relational.ExpressionList;
import net.sf.jsqlparser.parser.CCJSqlParserManager;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.statement.delete.Delete;
import net.sf.jsqlparser.statement.insert.Insert;
import net.sf.jsqlparser.statement.select.Select;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hbase.client.HBaseUtils;
import org.apache.hbase.sql.engine.HSqlEngine;
import org.apache.hbase.sql.visitor.DeleteSqlVisitor;
import org.apache.hbase.sql.visitor.SelectSqlVisitor;
import org.apache.hbase.sql.visitor.SqlContants;

import java.io.StringReader;
import java.sql.SQLSyntaxErrorException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by linghf on 2016/8/29.
 */

public class HSqlEngineImpl implements HSqlEngine {


    public List<String> select(String sql) throws Exception {
        return select(sql, null, null);
    }

    public List<String> select(String sql, String startRow, String stopRow) throws Exception {
        SelectSqlVisitor sqlVisitor = parseSql(sql);
        String tableName = sqlVisitor.getTableName();

        Scan scan = sqlVisitor.getScan(startRow, stopRow);

        Map<String, List<String>> columnMap = sqlVisitor.getColumnMap();
        boolean returnRK = sqlVisitor.isReturnRK();

        HBaseUtils.getResult(tableName, scan, columnMap);
        return null;
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

    public void insert(String sql) throws Exception {
        CCJSqlParserManager parserManager = new CCJSqlParserManager();
        Insert insert = (Insert) parserManager.parse(new StringReader(sql));

        String tableName = insert.getTable().getWholeTableName();

        Map<String, String> map = new HashMap<String, String>();

        int size = insert.getColumns().size();
        for (int i = 0; i < size; i++) {
            String key = ((Column) insert.getColumns().get(i)).getWholeColumnName();
            Object o = ((ExpressionList) insert.getItemsList()).getExpressions().get(i);
            String value = null;

            if (o instanceof LongValue) {
                LongValue longValue = (LongValue) o;
                value = ((LongValue) longValue).getStringValue();
            } else if (o instanceof StringValue) {
                StringValue stringValue = (StringValue) o;
                value = stringValue.getValue();
            } else if (o instanceof DoubleValue) {
                DoubleValue doubleValue = (DoubleValue) o;
                value = ((DoubleValue) doubleValue).getValue() + "";
            }

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

        sqlVisitor.getTableName();

        System.out.println(sqlVisitor.getRowkeys());
        System.out.println(sqlVisitor.getColumnMap());
    }

    public void del(String sql, List<String> rowkeys) throws Exception {
        DeleteSqlVisitor sqlVisitor = parseDeleteSqlVisitor(sql);

        String tableName = sqlVisitor.getTableName();
        boolean delAll = sqlVisitor.isDelAll();

        Map<String, List<String>> columnMap = sqlVisitor.getColumnMap();

        if (delAll) { // 删除所有的column
            HBaseUtils.deleteAllColumn(tableName, rowkeys);
        } else { //删除指定的column
            HBaseUtils.deleteColumn(tableName, rowkeys, columnMap);
        }
    }

    public void del(String sql, List<String> rowkeys, HashMap<String, ArrayList<String>> columnMap) throws Exception {

    }
}
