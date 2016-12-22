package org.dxer.hbase.client;

import com.google.common.base.Strings;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.FirstKeyOnlyFilter;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.util.Bytes;
import org.dxer.hbase.HBaseSqlContants;
import org.dxer.hbase.entity.PageResult;
import org.dxer.hbase.exceptions.NoRowKeyException;
import org.dxer.hbase.sql.util.ExpressionUtil;
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

    public static HTableInterface getHtable(HConnection connection, String tableName) {
        if (connection == null) {
            return null;
        }

        try {
            return connection.getTable(tableName);
        } catch (IOException e) {
            e.printStackTrace();
            logger.error(e.getMessage(), e);
        }
        return null;
    }

    /**
     * HBase表是否存在
     *
     * @param tableName
     * @return
     * @throws IOException
     */
    public static boolean tableExists(HConnection connection, String tableName) {
        boolean isexists = false;
        HBaseAdmin admin = null;
        try {
            admin = new HBaseAdmin(HBaseConfig.getConfiguration());

            isexists = admin.tableExists(tableName);
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
        } finally {
            close(admin);
        }
        return isexists;
    }

    /**
     * 删除指定rowkeys行
     *
     * @param tableName -- 表名
     * @param rowKeys   -- rowkeys
     * @param columnMap -- 列名
     * @return
     */
    public static boolean deleteColumn(HConnection connection, String tableName, List<String> rowKeys,
                                       Map<String, List<String>> columnMap) {
        HTableInterface table = null;
        try {
            table = getHtable(connection, tableName);

            List<Delete> deletes = null;
            if (rowKeys != null && rowKeys.size() > 0) {
                deletes = new ArrayList<Delete>();
                for (String rowKey : rowKeys) {
                    Delete deleteColumn = new Delete(Bytes.toBytes(rowKey));
                    if (columnMap != null && !columnMap.isEmpty()) {
                        for (String family : columnMap.keySet()) {
                            List<String> list = columnMap.get(family);
                            for (String column : list) {
                                deleteColumn.deleteColumn(Bytes.toBytes(family), Bytes.toBytes(column));
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
     * 删除整行数据，根据rowkey进行删除
     *
     * @param tableName 表名
     * @param rowKeys   rowKeys
     * @throws IOException
     */
    public static boolean deleteAllColumn(HConnection connection, String tableName, List<String> rowKeys) {
        HTableInterface table = null;
        try {
            table = getHtable(connection, tableName);
            List<Delete> deletes = new ArrayList<Delete>(rowKeys.size());
            for (String rowKey : rowKeys) {
                deletes.add(new Delete(Bytes.toBytes(rowKey)));
            }

            table.delete(deletes);
            logger.debug("delete a table Column,tableName:" + tableName + ",rowKeys:" + rowKeys +
                    "  all columns are deleted!");
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
    public static void
    putMaps(HConnection connection, String tableName, ArrayList<HashMap<String, String>> list)
            throws Exception {
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

        puts(connection, tableName, puts);
    }

    /**
     * 插入数据
     *
     * @param tableName
     * @param map
     * @throws Exception
     */
    public static void putMap(HConnection connection, String tableName, Map<String, String> map) throws Exception {
        if (Strings.isNullOrEmpty(tableName) || map == null || map.isEmpty()) {
            return;
        }

        Put put = build(map);
        if (put != null) {
            put(connection, tableName, put);
        }
    }

    /**
     * 将数据插入到指定的表中
     *
     * @param tableName
     * @param puts
     * @throws Exception
     */
    public static void puts(HConnection connection, String tableName, List<Put> puts) throws Exception {
        HTableInterface table = null;
        try {
            table = getHtable(connection, tableName);
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
    public static void put(HConnection connection, String tableName, Put... put) throws Exception {
        if (put != null && put.length > 0) {
            List<Put> puts = new ArrayList<Put>();
            for (Put p : put) {
                puts.add(p);
            }

            puts(connection, tableName, puts);
        }
    }

    /**
     * @param rowList
     * @param resultColumn
     * @return
     */
    public static List<Result> getList(HConnection connection, String tableName, List<byte[]> rowList,
                                       Map<String, List<String>> resultColumn) {
        HTableInterface table = null;
        List<Result> results = null;
        try {
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

            table = getHtable(connection, tableName);
            if (table != null && list != null && list.size() > 0) {
                Result[] rs = table.get(list);
                if (rs != null) {
                    results = new ArrayList<Result>();
                    for (Result r : rs) {
                        results.add(r);
                    }
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            close(table);
        }

        return results;
    }

    public static Result getResult(HConnection connection, String tableName, String rowkey,
                                   Map<String, List<String>> resultColumn) {
        HTableInterface table = null;
        Result result = null;
        if (!Strings.isNullOrEmpty(rowkey)) {
            try {
                Get get = new Get(Bytes.toBytes(rowkey));

                if (resultColumn != null) {
                    Set<String> cfSet = resultColumn.keySet();
                    for (String cf : cfSet) {
                        List<String> cList = resultColumn.get(cf);
                        for (String c : cList) {
                            get.addColumn(Bytes.toBytes(cf), Bytes.toBytes(c));
                        }
                    }
                }

                table = getHtable(connection, tableName);
                result = table.get(get);

            } catch (IOException e) {
                e.printStackTrace();
            } finally {
                close(table);
            }
        }
        return result;
    }

    public static byte[] getBytes(String str) {
        if (!Strings.isNullOrEmpty(str)) {
            return Bytes.toBytes(str);
        }
        return null;
    }

    public static String toStr(byte[] b) {
        if (b != null) {
            return new String(b);
        }
        return null;
    }

    public static Result get(HConnection connection, String tableName, String rowkey) {
        Result result = null;
        HTableInterface htable = null;
        try {
            htable = getHtable(connection, tableName);
            result = htable.get(new Get(rowkey.getBytes()));
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            close(htable);
        }
        return result;
    }

    public static List<Result> get(HConnection connection, String tableName, Map<String, List<String>> resultColumn,
                                   String... rowKeys) {
        List<String> rowKeyList = null;
        if (rowKeys != null && rowKeys.length > 0) {
            rowKeyList = new ArrayList<String>();
            for (String rowkey : rowKeys) {
                rowKeyList.add(rowkey);
            }
        }
        return get(connection, tableName, resultColumn, rowKeyList);
    }

    @SuppressWarnings("unchecked")
    public static List<Result> get(HConnection connection, String tableName, Map<String, List<String>> resultColumn,
                                   List<String> rowKeyList) {
        List<Result> resultList = null;
        List<Get> gets = getListByRowKeys(rowKeyList, resultColumn);
        if (gets != null && gets.size() > 0) {
            HTableInterface htable = null;
            try {
                htable = getHtable(connection, tableName);

                Result[] results = htable.get(gets);
                if (results != null && results.length > 0) {
                    resultList = new ArrayList<Result>();
                    resultList.addAll(Arrays.asList(results));
                }
            } catch (Exception e) {
                e.printStackTrace();
            } finally {
                close(htable);
            }
        }
        return resultList;
    }

    public static List<Get> getListByRowKeyBytes(List<byte[]> rowKeyByteList, Map<String, List<String>> resultColumn) {
        List<Get> list = new LinkedList<Get>();
        if (rowKeyByteList == null || rowKeyByteList.size() == 0) {
            return list;
        }
        for (byte[] row : rowKeyByteList) {
            if (row == null) {
                continue;
            }
            Get get = new Get(row);

            if (resultColumn != null) {
                Set<String> cfSet = resultColumn.keySet();
                for (String cf : cfSet) {
                    List<String> cList = resultColumn.get(cf);
                    for (String c : cList) {
                        get.addColumn(getBytes(cf), getBytes(c));
                    }
                }
            }

            list.add(get);
        }
        return list;
    }

    public static List<Get> getListByRowKeys(List<String> rowKeyList, Map<String, List<String>> resultColumn) {
        if (rowKeyList != null && !rowKeyList.isEmpty()) {
            List<byte[]> rowKeyByteList = new ArrayList<byte[]>();
            for (String rowkey : rowKeyList) {
                rowKeyByteList.add(getBytes(rowkey));
            }

            return getListByRowKeyBytes(rowKeyByteList, resultColumn);
        }
        return null;
    }

    /**
     * getResults
     *
     * @param tableName
     * @param scan
     * @param columnMap
     * @return
     */
    public static List<Result> getResults(HConnection connection, String tableName, Scan scan,
                                          Map<String, List<String>> columnMap) {
        if (connection == null || Strings.isNullOrEmpty(tableName) || scan == null) {
            return null;
        }

        HTableInterface table = null;

        ResultScanner rs = null;
        List<Result> resultList = null;
        try {
            table = getHtable(connection, tableName);
            rs = table.getScanner(scan);

            if (rs != null) {
                resultList = new ArrayList<Result>();
                for (Result result : rs) {
                    if (result != null) {
                        resultList.add(result);
                    }
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

    /**
     * 设置startRow和stopRow
     *
     * @param startRow
     * @param stopRow
     * @return
     */
    private static Scan getScan(String startRow, String stopRow) {
        Scan scan = new Scan();
        scan.setStartRow(getBytes(startRow));
        scan.setStopRow(getBytes(stopRow));
        return scan;
    }

    public static PageResult getRange(HConnection connection, String tableName, String startRow, String stopRow,
                                      Integer currentPage, Integer pageSize, Map<String, Map<String, String>> param,
                                      Map<String, List<String>> resultColumn) {
        return getRange(connection, tableName, null, startRow, stopRow, currentPage, pageSize, param, resultColumn);
    }

    public static PageResult getRange(HConnection connection, String tableName, FilterList extFilterList,
                                      String startRow, String stopRow, Integer currentPage, Integer pageSize,
                                      Map<String, Map<String, String>> param, Map<String, List<String>> resultColumn) {

        ResultScanner scanner = null;
        PageResult tbData = null;
        HTableInterface table = null;
        try {
            // 获取最大返回结果数量
            pageSize = (pageSize == null || pageSize == 0L) ? 100 : pageSize;
            currentPage = (currentPage == null || currentPage == 0) ? 1 : currentPage;

            // 计算起始页和结束页
            Integer firstRow = (currentPage - 1) * pageSize;

            Integer endRow = firstRow + pageSize;

            // 从表池中取出HBASE表对象
            table = getHtable(connection, tableName);
            // 获取筛选对象
            Scan scan = getScan(startRow, stopRow);

            FilterList filterList = new FilterList(FilterList.Operator.MUST_PASS_ALL);

            if (extFilterList != null) {
                filterList.addFilter(extFilterList);
            }

            // filterList.addFilter(new FirstKeyOnlyFilter());
            if (param != null) {
                Set<String> cfSet = param.keySet();
                for (String cf : cfSet) {
                    scan.addFamily(getBytes(cf));

                    Map<String, String> cMap = param.get(cf);
                    Set<String> cSet = cMap.keySet();
                    for (String c : cSet) {
                        String v = cMap.get(c);

                        SingleColumnValueFilter filter = new SingleColumnValueFilter(getBytes(cf), getBytes(c),
                                CompareFilter.CompareOp.EQUAL, getBytes(v));
                        filter.setFilterIfMissing(true);

                        scan.addColumn(getBytes(cf), getBytes(c));
                        filterList.addFilter(filter);
                    }
                }
            }

            if (filterList.getFilters().isEmpty()) {
                filterList.addFilter(new FirstKeyOnlyFilter());
            }

            // filterList.setReversed(true);

            // 给筛选对象放入过滤器
            scan.setFilter(filterList);

            // 缓存1000条数据
            scan.setCaching(1000);
            scan.setCacheBlocks(false);

            scanner = table.getScanner(scan);
            int i = 0;
            List<byte[]> rowList = new LinkedList<byte[]>();
            // 遍历扫描器对象， 并将需要查询出来的数据row key取出
            for (Result result : scanner) {
                String row = toStr(result.getRow());
                if (i >= firstRow && i < endRow) {
                    rowList.add(getBytes(row));
                }
                i++;
            }

            // 获取取出的row key的GET对象
            Result[] results = table.get(getListByRowKeyBytes(rowList, resultColumn));

            // 封装分页对象
            tbData = new PageResult();
            tbData.setCurrentPage(currentPage);
            tbData.setPageSize(pageSize);
            tbData.setTotalCount(i);
            tbData.setTotalPage(getTotalPage(pageSize, i));
            // tbData.setResults(results);
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            closeScanner(scanner);
            close(table);
        }
        return tbData;
    }

    public static List<Result> getResults(HConnection connection, String tableName, Scan scan, Long offset, Long rowCount, Map<String, List<String>> resultColumn) {
        if (offset == null && rowCount == null) {
            return getResults(connection, tableName, scan, resultColumn);
        }

        ResultScanner scanner = null;
        HTableInterface table = null;
        List<Result> results = null;
        try {
            table = getHtable(connection, tableName);  // 从表池中取出HBASE表对象

            FilterList filterList = (FilterList) scan.getFilter();

            if (filterList != null && filterList.getFilters().isEmpty()) {
                filterList.addFilter(new FirstKeyOnlyFilter());
            }
            scan.setFilter(filterList);
            scanner = table.getScanner(scan);

            List<byte[]> rowList = new LinkedList<byte[]>();

            long startIndex = 0;
            if (offset != null) {
                startIndex = offset;
            }

            long endIndex = 0;
            if (rowCount != null && rowCount.longValue() > 0) {
                endIndex = startIndex + rowCount;
            } else if (rowCount != null && rowCount.longValue() > 0) {
                endIndex = startIndex + 1000;
            }

            scan.setCaching(new Long(endIndex).intValue() + 1); // // 设置缓存数据条数
            scan.setCacheBlocks(false);

            // 遍历扫描器对象， 并将需要查询出来的数据row key取出
            long i = 0;
            if (scanner != null) {
                for (Result result : scanner) {
                    String row = toStr(result.getRow());
                    if (i >= startIndex && i < endIndex) {
                        rowList.add(getBytes(row));
                    }
                    i++;
                }
            }

            // 获取取出的row key的GET对象
            Result[] resultArry = table.get(getListByRowKeyBytes(rowList, resultColumn));
            if (resultArry != null && resultArry.length > 0) {
                results = new ArrayList<Result>();
                for (Result result : resultArry) {
                    results.add(result);
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            closeScanner(scanner);
            close(table);
        }
        return results;
    }

    private static int getTotalPage(int pageSize, int totalCount) {
        int n = totalCount / pageSize;
        if (totalCount % pageSize == 0) {
            return n;
        } else {
            return ((int) n) + 1;
        }
    }

    /**
     * put result
     *
     * @param r
     */
    public static void printResult(Result r) {
        if (r == null || r.isEmpty()) {
            logger.debug("result is null or empty!");
        } else {
            StringBuilder sb = new StringBuilder();
            for (Cell cell : r.rawCells()) {
                sb.append("Row=" + Bytes.toString(r.getRow()) + "\t\t");
                sb.append("column=" + Bytes.toString(CellUtil.cloneFamily(cell)) + "." +
                        Bytes.toString(CellUtil.cloneQualifier(cell)) + ", ");
                sb.append("timestamp=" + cell.getTimestamp() + ", ");
                sb.append("value=" + Bytes.toString(CellUtil.cloneValue(cell)) + "\n");
            }
            System.out.println(sb.toString());
            logger.debug(sb.toString());
        }
    }

    /**
     * build put for hbase
     *
     * @param map
     * @return
     * @throws NoRowKeyException
     */
    public static Put build(Map<String, String> map) throws NoRowKeyException {
        Put put = null;
        if (map == null || map.isEmpty()) {
            return put;
        }

        String rowKey = map.get(HBaseSqlContants.ROW_KEY);

        if (Strings.isNullOrEmpty(rowKey)) {
            for (String key : map.keySet()) {
                if (HBaseSqlContants.ROW_KEY.equals(key.toUpperCase())) {
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
            if (HBaseSqlContants.ROW_KEY.equals(key.toUpperCase())) {
                continue;
            }

            String family = null;
            String column = null;

            String[] columnGroup = ExpressionUtil.getColumnGroup(key);
            if (columnGroup != null && columnGroup.length == 2) {
                family = columnGroup[0];
                column = columnGroup[1];
            }

            String value = map.get(key);

            if (!Strings.isNullOrEmpty(family) && !Strings.isNullOrEmpty(column) && !Strings.isNullOrEmpty(value)) {
                put.add(Bytes.toBytes(family), Bytes.toBytes(column), Bytes.toBytes(value));
            }
        }

        return put;
    }

    /**
     * close scanner
     *
     * @param scanner
     */
    private static void closeScanner(ResultScanner scanner) {
        if (scanner != null)
            scanner.close();
    }

    /**
     * close
     *
     * @param o
     */
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
