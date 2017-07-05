package org.apache.hbase.client;


import org.dxer.hbase.entity.HResult;
import org.dxer.hbase.sql.engine.HBaseSqlEngine;
import org.dxer.hbase.sql.engine.impl.HBaseSqlEngineImpl;

import java.util.List;

/**
 * Created by linghf on 2016/8/29.
 */

public class Test {
    public static void main(String[] args) throws Exception {
        HBaseSqlEngine sqlEngine = new HBaseSqlEngineImpl();
        String sql = "select * from  user where _rowkey_=111";
        //   String sql = "select * from user where _rowkey_ in (111, 222)";
//        String sql = "select info.age from user where _pre_rowkey_  = 11 ";

        List<HResult> results = sqlEngine.select(sql);

        for (HResult r : results) {
            System.out.println(r.toString());
        }

//        String s = "insert into user (_rowkey_, info.name, info.age) values ('sdfdsfsd', 'fdsfsd', 12)";
//        query.insert(s);
//        System.out.println("11111");

//        String s = "delete from user where _rowkey_ in ('111','22232','3333') and _column_ in ('info.name', 'info.age')";
//        query.del(s);
    }
}
