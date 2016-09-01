package org.apache.hbase.sql.engine;



import org.apache.hbase.sql.result.Result;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

/**
 * Created by linghf on 2016/8/29.
 */

public interface HBaseSqlEngine {

    public List<Result> select(String sql) throws Exception;

    public List<Result> select(String sql, String startRow, String stopRow) throws Exception;

    public void insert(String sql) throws Exception;

    public void insert(String sql, HashMap<String, String> map) throws Exception;

    public void insert(String sql, ArrayList<HashMap<String, String>> list) throws Exception;

    public void del(String sql) throws Exception;

    public void del(String sql, List<String> rowkeys) throws Exception;

    public void del(String sql, List<String> rowkeys, HashMap<String, ArrayList<String>> columnMap) throws Exception;
}
