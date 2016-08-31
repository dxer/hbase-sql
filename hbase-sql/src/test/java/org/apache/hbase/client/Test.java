package org.apache.hbase.client;

import org.apache.hbase.sql.engine.HSqlEngine;
import org.apache.hbase.sql.engine.impl.HSqlEngineImpl;

/**
 * Created by linghf on 2016/8/29.
 */

public class Test {
    public static void main(String[] args) throws Exception {
        HSqlEngine hSqlEngine = new HSqlEngineImpl();
        String s = "select * from user where _rowkey_=1111";

        hSqlEngine.select(s);

//        String s = "insert into user (_rowkey_, info.name, info.age) values ('sdfdsfsd', 'fdsfsd', 12)";
//        query.insert(s);
//        System.out.println("11111");

//        String s = "delete from user where _rowkey_ in ('111','22232','3333') and _column_ in ('info.name', 'info.age')";
//        query.del(s);
    }
}
