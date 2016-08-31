package org.apache.hbase.client;

import net.sf.jsqlparser.parser.CCJSqlParserManager;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.Select;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Test;

import java.io.StringReader;

/**
 * Created by linghf on 2016/8/29.
 */

public class HBaseUtilsTest {

    CCJSqlParserManager parserManager = new CCJSqlParserManager();

    @Test
    public void put() throws Exception {
        Put put = new Put(Bytes.toBytes("222"));
        put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("name"), Bytes.toBytes("lisi"));
        HBaseUtils.put("user", put);
    }

    @Test
    public void get() throws Exception {
        String statement = "SELECT * FROM tab1 WHERE a > {ts '2004-04-30 04:05:34.56'}";
        PlainSelect plainSelect = (PlainSelect) ((Select) parserManager.parse(new StringReader(statement))).getSelectBody();

    }
}
