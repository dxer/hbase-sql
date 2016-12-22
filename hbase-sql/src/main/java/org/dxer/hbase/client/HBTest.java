package org.dxer.hbase.client;

import org.dxer.hbase.entity.HResult;
import org.dxer.hbase.sql.engine.HBaseSqlEngine;
import org.dxer.hbase.sql.engine.impl.HBaseSqlEngineImpl;

import java.util.List;

/**
 * Created by linghf on 2016/9/2.
 */

public class HBTest {

    public static void main(String[] args) throws Exception {

        System.setProperty("hadoop.home.dir", "F:\\big\\hadoop-2.6.2");
        HBaseSqlEngine sqlEngine = new HBaseSqlEngineImpl();
        String sql1 = "select * from user where _rowkey_ = 111";
        String sql2 = "select * from user where _rowkey_ in (111, 222)";
        String sql3 = "select * from user";
        String sql4 = "select _rowkey_ from user limit 3";
        String sql5 = "select _rowkey_,info.age from user where _startrow_ = 222 and _stoprow_ = 333";

        List<HResult> result = sqlEngine.select(sql5);
        for (HResult r : result) {
            System.out.println(r);
        }

//        result = sqlEngine.select(sql4);
//        for (HResult r : result) {
//            System.out.println(r);
//        }
    }
}
