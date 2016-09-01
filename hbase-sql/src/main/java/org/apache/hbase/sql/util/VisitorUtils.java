package org.apache.hbase.sql.util;

import com.google.common.base.Strings;
import net.sf.jsqlparser.expression.DoubleValue;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.LongValue;
import net.sf.jsqlparser.expression.StringValue;
import net.sf.jsqlparser.expression.operators.relational.ExpressionList;
import net.sf.jsqlparser.expression.operators.relational.InExpression;
import net.sf.jsqlparser.expression.operators.relational.ItemsList;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Created by linghf on 2016/9/2.
 */

public class VisitorUtils {

    public static List<String> getStringList(InExpression inExpression) {
        List<String> list = null;
        if (inExpression != null) {
            list = new ArrayList<String>();
            ItemsList itemList = inExpression.getItemsList();
            if (itemList != null) {
                List items = ((ExpressionList) itemList).getExpressions();
                if (items != null && items.size() > 0) {
                    for (Object o : items) {
                        String value = getString((Expression) o);

                        if (!Strings.isNullOrEmpty(value)) {
                            list.add(value);
                        }
                    }
                }
            }

        }
        return list;
    }

    public static void setColumnMap(String columnGroup, Map<String, List<String>> columnMap) {
        if (Strings.isNullOrEmpty(columnGroup) || columnMap == null) {
            return;
        }

        String[] strs = SqlUtils.getColumngroup(columnGroup);
        if (strs != null && strs.length == 2) {
            String family = strs[0];
            String column = strs[1];

            if (!Strings.isNullOrEmpty(family) && !Strings.isNullOrEmpty(column)) {
                List<String> columns = columnMap.get(family);
                if (columns != null) {
                    columns.add(column);
                } else {
                    columns = new ArrayList<String>();
                    columns.add(column);
                }
                columnMap.put(family, columns);
            }
        }
    }


    public static String getString(Object o) {
        String value = null;
        if (o != null) {
            if (o instanceof LongValue) {
                LongValue longValue = (LongValue) o;
                value = ((LongValue) longValue).getStringValue();
            } else if (o instanceof StringValue) {
                StringValue stringValue = (StringValue) o;
                value = stringValue.getValue();
            } else if (o instanceof DoubleValue) {
                DoubleValue doubleValue = (DoubleValue) o;
                value = ((DoubleValue) doubleValue).getValue() + "";
            }
        }
        return value;
    }

    public static String getString(Expression expression) {
        String value = null;
        if (expression != null) {
            value = expression.toString();
        }

        return value;
    }

}
