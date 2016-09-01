package org.apache.hbase.sql.visitor;

import com.google.common.base.Strings;
import net.sf.jsqlparser.expression.*;
import net.sf.jsqlparser.expression.operators.arithmetic.*;
import net.sf.jsqlparser.expression.operators.conditional.AndExpression;
import net.sf.jsqlparser.expression.operators.conditional.OrExpression;
import net.sf.jsqlparser.expression.operators.relational.*;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.select.*;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hbase.sql.util.SqlUtils;
import org.apache.hbase.sql.util.VisitorUtils;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by linghf on 2016/8/29.
 */

public class SelectSqlVisitor implements SelectVisitor, FromItemVisitor, ExpressionVisitor {

    private List<String> tables = new ArrayList<String>();

    private long limit = 0L;

    private long offset = 0L;

    /*HBase过滤器*/
    private FilterList filters = new FilterList();

    /*hbase scan*/
    private Scan scan = new Scan();

    /**
     * 记录要返回的列名
     */
    private Map<String, List<String>> columnMap = new HashMap<String, List<String>>();

    /*是否返回rowkey*/
    private boolean returnRK = false;

    public SelectSqlVisitor(Select select) {
        select.getSelectBody().accept(this);
    }

    public String getTableName() {
        return this.tables.get(0);
    }

    public Scan getScan(String startRow, String stopRow) {
        setScanRange(startRow, stopRow);
        if (filters.getFilters().size() > 0) {
            this.scan.setFilter(this.filters); // 设置过滤器
        }
        return this.scan;
    }

    private void setScanRange(String startRow, String stopRow) {
        if (!Strings.isNullOrEmpty(startRow)) {
            this.scan.setStartRow(Bytes.toBytes(startRow));
        }

        if (!Strings.isNullOrEmpty(stopRow)) {
            this.scan.setStopRow(Bytes.toBytes(stopRow));
        }
    }

    public Map<String, List<String>> getColumnMap() {
        return columnMap;
    }

    public void setColumnMap(Map<String, List<String>> columnMap) {
        this.columnMap = columnMap;
    }

    public boolean isReturnRK() {
        return returnRK;
    }

    public void setReturnRK(boolean returnRK) {
        this.returnRK = returnRK;
    }

    public void visit(EqualsTo equalsTo) {
        String columnGroupStr = equalsTo.getLeftExpression().toString();
        String value = equalsTo.getRightExpression().toString();

        Filter filter = null;
        if (!Strings.isNullOrEmpty(columnGroupStr)) {
            if (SqlContants.ROW_KEY.equals(columnGroupStr.toUpperCase())) {
                filter = new RowFilter(CompareFilter.CompareOp.EQUAL, new BinaryComparator(Bytes.toBytes(value)));
                this.filters.addFilter(filter);
                return;
            } else if (SqlContants.PRE_ROW_KEY.equals(columnGroupStr.toUpperCase())) {
                System.out.println(value);
                this.scan.setRowPrefixFilter(Bytes.toBytes(value));
            } else if (!Strings.isNullOrEmpty(columnGroupStr)) {
                String[] columnGroup = SqlUtils.getColumngroup(columnGroupStr);
                if (columnGroup != null && columnGroup.length == 2) {
                    String columnFamily = columnGroup[0]; // 列族
                    String column = columnGroup[1]; // 列名

                    filter = new SingleColumnValueFilter(
                            Bytes.toBytes(columnFamily), Bytes.toBytes(column),
                            CompareFilter.CompareOp.EQUAL, new BinaryComparator(Bytes.toBytes(value)));
                }
            }
        }


        if (filter != null) {
            this.filters.addFilter(filter);
        }
    }

    /**
     * 设置column
     *
     * @param selectItems
     */
    private void setColumn(List selectItems) {
        for (Object item : selectItems) {
            String colStr = item.toString();
            if (Strings.isNullOrEmpty(colStr)) {
                continue;
            }

            if (colStr.equals("*")) {
                break;
            }

            if (SqlContants.ROW_KEY.equals(colStr.toUpperCase())) {
                returnRK = true;
            } else {
                String[] columnGroup = SqlUtils.getColumngroup(item.toString());

                if (columnGroup == null || Strings.isNullOrEmpty(columnGroup[0]) || Strings.isNullOrEmpty(columnGroup[1])) {
                    throw new RuntimeException("");
                }
                String columnFamily = columnGroup[0];
                String column = columnGroup[1];

                List<String> columns = columnMap.get(columnFamily);
                if (columns == null) {
                    columns = new ArrayList<String>();
                    columns.add(column);
                } else {
                    if (!columns.contains(column)) {
                        columns.add(column);
                    }
                }
                columnMap.put(columnFamily, columns);
            }
        }
    }

    private void initLimitOffset(PlainSelect plainSelect) {
        Limit limit = plainSelect.getLimit();
        if (limit == null) {
            return;
        }

        this.limit = limit.getRowCount();
        this.offset = limit.getOffset();
    }

    public void visit(NullValue nullValue) {

    }

    public void visit(Function function) {

    }

    public void visit(InverseExpression inverseExpression) {

    }

    public void visit(JdbcParameter jdbcParameter) {

    }

    public void visit(DoubleValue doubleValue) {

    }

    public void visit(LongValue longValue) {

    }

    public void visit(DateValue dateValue) {

    }

    public void visit(TimeValue timeValue) {

    }

    public void visit(TimestampValue timestampValue) {

    }

    public void visit(Parenthesis parenthesis) {

    }

    public void visit(StringValue stringValue) {

    }

    public void visit(Addition addition) {

    }

    public void visit(Division division) {

    }

    public void visit(Multiplication multiplication) {

    }

    public void visit(Subtraction subtraction) {

    }

    public void visit(AndExpression andExpression) {
        andExpression.getLeftExpression().accept(this);
        andExpression.getRightExpression().accept(this);
    }

    public void visit(OrExpression orExpression) {
        orExpression.getLeftExpression().accept(this);
        orExpression.getRightExpression().accept(this);
    }

    public void visit(Between between) {

    }


    public void visit(GreaterThan greaterThan) {

    }

    public void visit(GreaterThanEquals greaterThanEquals) {

    }

    public void visit(InExpression inExpression) {
        String key = inExpression.getLeftExpression().toString();

        ItemsList itemList = inExpression.getItemsList();
        if (SqlContants.ROW_KEY.equals(key.toUpperCase())) { // row key
            FilterList rkFilterList = new FilterList(FilterList.Operator.MUST_PASS_ONE);
            List<String> strList = VisitorUtils.getStringList(inExpression);
            if (strList != null && !strList.isEmpty()) {
                for (String str : strList) {
                    RowFilter rFilter = new RowFilter(CompareFilter.CompareOp.EQUAL, new BinaryComparator(Bytes.toBytes(str)));
                    rkFilterList.addFilter(rFilter);
                }
                filters.addFilter(rkFilterList);
            }
        } else { // TODO

        }

    }

    public void visit(IsNullExpression isNullExpression) {


    }

    public void visit(LikeExpression likeExpression) {
        String key = VisitorUtils.getString(likeExpression.getLeftExpression());
        String value = VisitorUtils.getString(likeExpression.getRightExpression());
        if (!Strings.isNullOrEmpty(value)) {
            value = SqlUtils.ex(value);
        }

        if (SqlContants.ROW_KEY.equals(key.toUpperCase())) {

            Filter rkFilter = new RowFilter(CompareFilter.CompareOp.EQUAL, new RegexStringComparator(value));

            filters.addFilter(rkFilter);
        } else {

        }
    }

    public void visit(MinorThan minorThan) {

    }

    public void visit(MinorThanEquals minorThanEquals) {

    }

    public void visit(NotEqualsTo notEqualsTo) {

    }

    public void visit(Column column) {

    }

    public void visit(CaseExpression caseExpression) {

    }

    public void visit(WhenClause whenClause) {

    }

    public void visit(ExistsExpression existsExpression) {

    }

    public void visit(AllComparisonExpression allComparisonExpression) {

    }

    public void visit(AnyComparisonExpression anyComparisonExpression) {

    }

    public void visit(Concat concat) {

    }

    public void visit(Matches matches) {

    }

    public void visit(BitwiseAnd bitwiseAnd) {

    }

    public void visit(BitwiseOr bitwiseOr) {

    }

    public void visit(BitwiseXor bitwiseXor) {

    }

    public void visit(Table table) {
        String tableWholeName = table.getWholeTableName();
        this.tables.add(tableWholeName);
    }

    public void visit(SubSelect subSelect) {

    }

    public void visit(SubJoin subJoin) {

    }

    public void visit(PlainSelect plainSelect) {
        List selectItems = plainSelect.getSelectItems();
        setColumn(selectItems);
        initLimitOffset(plainSelect);

        plainSelect.getFromItem().accept(this);
        if (plainSelect.getWhere() != null) {
            plainSelect.getWhere().accept(this);
        }
    }

    public void visit(Union union) {

    }

}
