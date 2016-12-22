package org.dxer.hbase.util;

import com.google.common.base.Strings;
import org.dxer.hbase.entity.ColumnFamily;
import org.dxer.hbase.entity.RowKey;
import org.dxer.hbase.entity.Select;
import org.dxer.hbase.entity.Table;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.NodeList;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import java.io.File;
import java.util.HashMap;
import java.util.Map;


/**
 * Created by linghf on 2016/12/21.
 */

public class HBaseSqlMapParser {

    private static HashMap<String, Table> tableMap = new HashMap<String, Table>();

    private static Map<String, Select> selectMap = new HashMap<String, Select>();

    static {
        try {
            parser("G:\\git\\hbase-sql\\hbase-sql\\src\\main\\resources\\sqlmap.xml");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private static void parser(String fileName) throws Exception {
        if (Strings.isNullOrEmpty(fileName)) {
            return;
        }

        DocumentBuilderFactory builderFactory = DocumentBuilderFactory
                .newInstance();
        DocumentBuilder builder = builderFactory.newDocumentBuilder();
        /*
         * builder.parse()方法将给定文件的内容解析为一个 XML 文档， 并且返回一个新的 DOM Document对象。
         */
        Document document = null;
        try {
            document = builder.parse(new File(fileName));
        } catch (Exception e) {
            e.printStackTrace();
        }

        if (document == null) {
            return;
        }

        NodeList tableList = document.getElementsByTagName("table");

        if (tableList != null && tableList.getLength() > 0) {
            for (int i = 0; i < tableList.getLength(); ++i) {
                Element element = (Element) tableList.item(i);
                parseTable(element);
            }
        }

        NodeList selectList = document.getElementsByTagName("select");
        if (selectList != null && selectList.getLength() > 0) {
            for (int i = 0; i < selectList.getLength(); i++) {
                Element selectElement = (Element) tableList.item(i);
                Select select = parseSelect(selectElement);
                if (select != null) {
                    selectMap.put(select.getId(), select);
                }
            }
        }


        NodeList insertList = document.getElementsByTagName("insert");
        if (insertList != null && insertList.getLength() > 0) {
            for (int i = 0; i < insertList.getLength(); i++) {

            }
        }


        NodeList deleteList = document.getElementsByTagName("delete");
        if (deleteList != null && deleteList.getLength() > 0) {
            for (int i = 0; i < deleteList.getLength(); i++) {

            }
        }


    }

    private static Table parseTable(Element element) {
        Table table = null;
        if (element == null) {
            return table;
        }
        table = new Table();
        String tableName = element.getAttribute("name");
        if (Strings.isNullOrEmpty(tableName)) {
            return table;
        }

        table.setTableName(tableName);

        String rowKeyName = null;
        if (element.getElementsByTagName("rowkey") != null && element.getElementsByTagName("rowkey").getLength() > 0) {
            rowKeyName = ((Element) element.getElementsByTagName("rowkey").item(0)).getAttribute("name");
        }

        if (!Strings.isNullOrEmpty(rowKeyName)) {
            RowKey rowKey = new RowKey();
            rowKey.setAliasName(rowKeyName);
            table.setRowKey(rowKey);
        }

        NodeList cfNodeList = element.getElementsByTagName("columnFamily");

        if (cfNodeList != null && cfNodeList.getLength() > 0) {
            for (int i = 0; i < cfNodeList.getLength(); i++) {
                Element cfElement = (Element) cfNodeList.item(i);
                if (cfElement == null) {
                    continue;
                }
                ColumnFamily columnFamily = new ColumnFamily();
                String columnFamilyName = cfElement.getAttribute("name");
                if (Strings.isNullOrEmpty(columnFamilyName)) {
                    continue;
                }
                columnFamily.setName(columnFamilyName);

                parseColumnFamily(cfElement, columnFamily);

                table.addColumnFamily(columnFamily);
            }
        }
        return table;
    }


    private static void parseColumnFamily(Element element, ColumnFamily columnFamily) {
        if (element == null || columnFamily == null) {
            return;
        }

        NodeList columnNodeList = element.getElementsByTagName("column");
        if (columnNodeList != null && columnNodeList.getLength() > 0) {
            columnFamily = new ColumnFamily();
            for (int i = 0; i < columnNodeList.getLength(); i++) {
                Element columnNode = (Element) columnNodeList.item(i);
                if (columnNode == null) {
                    continue;
                }

                String columnName = columnNode.getAttribute("name");
                columnFamily.addColumn(columnName);
            }
        }
    }

    private static Select parseSelect(Element element) {
        Select select = null;
        if (element == null) {
            return select;
        }

        String id = element.getAttribute("id");
        String sql = element.getNodeValue();
        if (Strings.isNullOrEmpty(id) || Strings.isNullOrEmpty(sql)) {
            return select;
        }

        select = new Select();
        select.setId(id);
        select.setSql(sql);
        return select;
    }

    public static Map<String, Table> getTableMap() {
        return tableMap;
    }

    public static Table getTable(String tableName) {
        if (tableMap != null) {
            return tableMap.get(tableName);
        }
        return null;
    }

    public static void main(String[] args) {
        new HBaseSqlMapParser();
    }
}
