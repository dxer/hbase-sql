package org.dxer.hbase.sql.visitor;

import com.google.common.base.Strings;
import net.sf.jsqlparser.expression.*;
import net.sf.jsqlparser.expression.operators.arithmetic.*;
import net.sf.jsqlparser.expression.operators.conditional.AndExpression;
import net.sf.jsqlparser.expression.operators.conditional.OrExpression;
import net.sf.jsqlparser.expression.operators.relational.*;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.statement.delete.Delete;
import net.sf.jsqlparser.statement.select.SubSelect;
import org.dxer.hbase.HBaseSqlContants;
import org.dxer.hbase.sql.util.ExpressionUtil;

import java.util.*;

/**
 * Created by linghf on 2016/8/30.
 */

public class DeleteSqlVisitor implements ExpressionVisitor {

    private Set<String> rowkeys = new HashSet<String>();

    private Delete delete;

    private boolean delAll = false;

    private Map<String, List<String>> columnMap = new HashMap<String, List<String>>();

    public DeleteSqlVisitor(Delete delete) {
        this.delete = delete;
        delete.getWhere().accept(this);
    }

    public Set<String> getRowkeys() {
        return rowkeys;
    }


    public Map<String, List<String>> getColumnMap() {
        return columnMap;
    }

    public boolean isDelAll() {
        return delAll;
    }

    public String getTableName() {
        return delete != null ? delete.getTable().getName() : null;
    }

    public void visit(NullValue nullValue) {

    }

    public void visit(Function function) {

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

    public void visit(EqualsTo equalsTo) {
        String key = equalsTo.getLeftExpression().toString();
        String value = equalsTo.getRightExpression().toString();

        if (!Strings.isNullOrEmpty(key) && !Strings.isNullOrEmpty(value)) {
            if (HBaseSqlContants.ROW_KEY.equals(key.toUpperCase())) {
                rowkeys.add(value);
            } else if (HBaseSqlContants.HBASE_COLUMN.equals(key.toUpperCase())) {
                if (Strings.isNullOrEmpty(value)) {
                    return;
                }

                if (!delAll && value.equals(HBaseSqlContants.ASTERISK)) {
                    delAll = true;
                } else if (!delAll) {
                    ExpressionUtil.setColumnMap(value, columnMap);
                }
            }
        }
    }

    public void visit(GreaterThan greaterThan) {

    }

    public void visit(GreaterThanEquals greaterThanEquals) {

    }

    public void visit(InExpression inExpression) {
        String key = inExpression.getLeftExpression().toString();
        if (HBaseSqlContants.ROW_KEY.equals(key.toUpperCase())) {
            List<String> values = ExpressionUtil.getStringList(inExpression.getRightItemsList());
            if (values != null && !values.isEmpty()) {
                for (String value : values) {
                    rowkeys.add(value);
                }
            }
        } else if (HBaseSqlContants.HBASE_COLUMN.equals(key.toUpperCase())) {
            ItemsList itemList = null;//inExpression.getItemsList();
            if (itemList != null) {
                List list = ((ExpressionList) itemList).getExpressions();
                if (list != null && list.size() > 0) {
                    for (Object o : list) {
                        String value = null;
                        if (o instanceof StringValue) {
                            StringValue stringValue = (StringValue) o;
                            value = stringValue.getValue();
                        } else {
                            continue;
                        }

                        if (!Strings.isNullOrEmpty(value)) {
                            ExpressionUtil.setColumnMap(value, columnMap);
                        }
                    }
                }
            }
        }
    }

    public void visit(SignedExpression signedExpression) {

    }

    public void visit(JdbcNamedParameter jdbcNamedParameter) {

    }

    public void visit(HexValue hexValue) {

    }

    public void visit(IsNullExpression isNullExpression) {

    }

    public void visit(LikeExpression likeExpression) {

    }

    public void visit(MinorThan minorThan) {

    }

    public void visit(MinorThanEquals minorThanEquals) {

    }

    public void visit(NotEqualsTo notEqualsTo) {

    }

    public void visit(Column column) {

    }

    public void visit(SubSelect subSelect) {

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

    public void visit(CastExpression castExpression) {

    }

    public void visit(Modulo modulo) {

    }

    public void visit(AnalyticExpression analyticExpression) {

    }

    public void visit(WithinGroupExpression withinGroupExpression) {

    }

    public void visit(ExtractExpression extractExpression) {

    }

    public void visit(IntervalExpression intervalExpression) {

    }

    public void visit(OracleHierarchicalExpression oracleHierarchicalExpression) {

    }

    public void visit(RegExpMatchOperator regExpMatchOperator) {

    }

    public void visit(JsonExpression jsonExpression) {

    }

    public void visit(RegExpMySQLOperator regExpMySQLOperator) {

    }

    public void visit(UserVariable userVariable) {

    }

    public void visit(NumericBind numericBind) {

    }

    public void visit(KeepExpression keepExpression) {

    }

    public void visit(MySQLGroupConcat mySQLGroupConcat) {

    }

    public void visit(RowConstructor rowConstructor) {

    }

    public void visit(OracleHint oracleHint) {

    }

    public void visit(TimeKeyExpression timeKeyExpression) {

    }

    public void visit(DateTimeLiteralExpression dateTimeLiteralExpression) {

    }
}
