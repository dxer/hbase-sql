package org.apache.hbase.client;

import com.google.common.base.Strings;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hbase.exceptions.NoRowKeyException;
import org.apache.hbase.sql.util.SqlUtils;
import org.apache.hbase.sql.visitor.SqlContants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.util.*;

/**
 * Created by linghf on 2016/8/29.
 */

public class HBaseUtils {

    private static Logger logger = LoggerFactory.getLogger(HBaseUtils.class);

    private static Admin admin = null;

    private static Connection conn = null;

    static {
        init();
    }

    private static void init() {
        try {
            conn = HBaseConnection.getConnection();
            logger.debug("init hbase connection.");
            if (null != conn) {
                admin = conn.getAdmin();
            }
        } catch (Exception e) {
            logger.error("init hbase connection error");
            e.printStackTrace();
        }
    }


    /**
     * HBase表是否存在
     *
     * @param tableName
     * @return
     * @throws IOException
     */
    public static boolean isTableExist(String tableName) throws IOException {
        if (!Strings.isNullOrEmpty(tableName)) {
            return admin.tableExists(TableName.valueOf(tableName));
        } else {
            throw new RuntimeException("tableName is null");
        }
    }


    /**
     * @param tableName
     * @param family
     * @return 创建成功返回0，表已经存在返回1
     * @throws Exception
     */
    public static int creatTable(String tableName, List<String> family) throws IOException {
        if (isTableExist(tableName)) {
            return 1;
        } else {
            HTableDescriptor desc = new HTableDescriptor(tableName);
            for (String string : family) {
                HColumnDescriptor hColumnDescriptor = new HColumnDescriptor(string);
                hColumnDescriptor.setInMemory(true);
                //hColumnDescriptor.setTimeToLive(timeToLive)
                desc.addFamily(hColumnDescriptor);
            }
            admin.createTable(desc);
            logger.debug("create a new table ,tableName:" + tableName + ",familys:" + family);
            return 0;
        }
    }

    /**
     * 删除
     *
     * @param tableName -- 表名
     * @param rowKeys   -- rowkeys
     * @param columnMap -- 列名
     * @return
     */
    public static boolean deleteColumn(String tableName, List<String> rowKeys, Map<String, List<String>> columnMap) {
        Table table = null;
        try {
            table = conn.getTable(TableName.valueOf(tableName));

            List<Delete> deletes = null;
            if (rowKeys != null && rowKeys.size() > 0) {
                deletes = new ArrayList<Delete>();
                for (String rowKey : rowKeys) {
                    Delete deleteColumn = new Delete(Bytes.toBytes(rowKey));
                    if (columnMap != null && !columnMap.isEmpty()) {
                        for (String family : columnMap.keySet()) {
                            List<String> list = columnMap.get(family);
                            for (String column : list) {
                                deleteColumn.addColumn(Bytes.toBytes(family), Bytes.toBytes(column));
                            }
                        }
                    }
                    deletes.add(deleteColumn);
                }
            }

            table.delete(deletes);
            return true;
        } catch (Exception ex) {
            ex.printStackTrace();
        } finally {
            close(table);
        }
        return false;
    }


    /**
     * 删除整行数据
     *
     * @param tableName 表名
     * @param rowKeys   rowKeys
     * @throws IOException
     */
    public static boolean deleteAllColumn(String tableName, List<String> rowKeys) {
        Table table = null;
        try {
            table = conn.getTable(TableName.valueOf(tableName));
            List<Delete> deletes = new ArrayList<Delete>(rowKeys.size());
            for (String rowKey : rowKeys) {
                deletes.add(new Delete(Bytes.toBytes(rowKey)));
            }

            table.delete(deletes);
            logger.debug("delete a table Column,tableName:" + tableName + ",rowKeys:" + rowKeys + "  all columns are deleted!");
            return true;
        } catch (Exception ex) {
            logger.error("delete table Column error,tableName:" + tableName + ",rowKeys:" + rowKeys, ex);
            return false;
        } finally {
            close(table);
        }
    }

    /**
     * 插入数据
     *
     * @param tableName
     * @param list
     * @throws Exception
     */
    public static void putMaps(String tableName, ArrayList<HashMap<String, String>> list) throws Exception {
        if (list == null || list.isEmpty()) {
            return;
        }

        List<Put> puts = new ArrayList<Put>();
        for (Map<String, String> map : list) {
            Put put = build(map);
            if (put != null) {
                puts.add(put);
            }
        }

        puts(tableName, puts);

    }

    /**
     * 插入数据
     *
     * @param tableName
     * @param map
     * @throws Exception
     */
    public static void putMap(String tableName, Map<String, String> map) throws Exception {
        if (Strings.isNullOrEmpty(tableName) || map == null || map.isEmpty()) {
            return;
        }

        Put put = build(map);
        System.out.println(put);
        put(tableName, put);
    }

    /**
     * 插入数据
     *
     * @param tableName
     * @param puts
     * @throws Exception
     */
    public static void puts(String tableName, List<Put> puts) throws Exception {
        Table table = null;

        try {
            conn = HBaseConnection.getConnection();
            table = conn.getTable(TableName.valueOf(tableName));
            if (puts != null && puts.size() > 0) {
                table.put(puts);
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            close(table);
        }
    }

    /**
     * 插入数据
     *
     * @param tableName
     * @param put
     * @throws Exception
     */
    public static void put(String tableName, Put... put) throws Exception {
        if (put != null && put.length > 0) {
            List<Put> puts = new ArrayList<Put>();
            for (Put p : put) {
                puts.add(p);
            }

            puts(tableName, puts);
        }
    }

    private static List<Get> getList(List<byte[]> rowList, Map<String, List<String>> resultColumn) {
        List<Get> list = new LinkedList<Get>();
        for (byte[] row : rowList) {
            Get get = new Get(row);

            if (resultColumn != null) {
                Set<String> cfSet = resultColumn.keySet();
                for (String cf : cfSet) {
                    List<String> cList = resultColumn.get(cf);
                    for (String c : cList) {
                        get.addColumn(Bytes.toBytes(cf), Bytes.toBytes(c));
                    }
                }
            }

            list.add(get);
        }
        return list;
    }


    public static List<Result> getResult(String tableName, Scan scan, Map<String, List<String>> columnMap) {
        Table table = null;
        ResultScanner rs = null;
        List<Result> resultList = null;
        try {
            table = conn.getTable(TableName.valueOf(tableName));
            rs = table.getScanner(scan);
            Set<byte[]> rowList = new HashSet<byte[]>();

            scan.getStartRow();
            scan.getStopRow();

            System.out.println(rs);
            if (rs != null) {
                for (Result result : rs) {
                    rowList.add(result.getRow());
                }
            }

            if (rowList != null && rowList.size() > 0) {
                Result[] results = table.get(getList(new ArrayList<byte[]>(rowList), columnMap));

                if (results != null && results.length > 0) {
                    resultList = new ArrayList<Result>();
                    resultList.addAll(Arrays.asList(results));
                }

                for (Result r : results) {
                    printResult(r);
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            closeScanner(rs);
            close(table);
        }
        return resultList;
    }


    private static void get(ResultScanner rs) {

    }


    public static void printResult(Result r) {
        if (r == null || r.isEmpty()) {
            logger.debug("result is null or empty!");
        } else {
            StringBuilder sb = new StringBuilder();
            for (Cell cell : r.rawCells()) {
                sb.append("Row=" + Bytes.toString(r.getRow()) + "\t\t");
                sb.append("column=" + Bytes.toString(CellUtil.cloneFamily(cell)) + "." + Bytes.toString(CellUtil.cloneQualifier(cell)) + ", ");
                sb.append("timestamp=" + cell.getTimestamp() + ", ");
                sb.append("value=" + Bytes.toString(CellUtil.cloneValue(cell)) + "\n");
            }
            System.out.println(sb.toString());
            logger.debug(sb.toString());
        }
    }

    public static Put build(Map<String, String> map) throws NoRowKeyException {
        Put put = null;
        if (map == null || map.isEmpty()) {
            return put;
        }

        String rowKey = map.get(SqlContants.ROW_KEY);

        if (Strings.isNullOrEmpty(rowKey)) {
            for (String key : map.keySet()) {
                if (SqlContants.ROW_KEY.equals(key.toUpperCase())) {
                    rowKey = map.get(key);
                    break;
                }
            }
        }

        if (Strings.isNullOrEmpty(rowKey)) {
            throw new NoRowKeyException("insert data but no rowkey");
        }

        put = new Put(Bytes.toBytes(rowKey));

        for (String key : map.keySet()) {
            if (SqlContants.ROW_KEY.equals(key.toUpperCase())) {
                continue;
            }

            String family = null;
            String column = null;


            String[] columnGroup = SqlUtils.getColumngroup(key);
            if (columnGroup != null && columnGroup.length == 2) {
                family = columnGroup[0];
                column = columnGroup[1];
            }

            String value = map.get(key);

            if (!Strings.isNullOrEmpty(family) && !Strings.isNullOrEmpty(column) && !Strings.isNullOrEmpty(value)) {
                put.addColumn(Bytes.toBytes(family), Bytes.toBytes(column), Bytes.toBytes(value));
            }
        }

        return put;
    }

    private static void closeScanner(ResultScanner scanner) {
        if (scanner != null)
            scanner.close();
    }

    private static void close(Closeable o) {
        if (o != null) {
            try {
                o.close();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }
}
