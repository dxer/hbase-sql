package org.apache.hbase;

import com.alibaba.druid.sql.SQLUtils;
import com.alibaba.druid.sql.ast.SQLStatement;
import com.alibaba.druid.sql.ast.statement.SQLSelect;
import com.alibaba.druid.sql.ast.statement.SQLSelectItem;
import com.alibaba.druid.sql.ast.statement.SQLSelectStatement;
import com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlSelectQueryBlock;
import com.alibaba.druid.sql.dialect.mysql.parser.MySqlStatementParser;
import com.alibaba.druid.sql.parser.SQLStatementParser;
import com.alibaba.druid.sql.visitor.SQLASTOutputVisitor;
import com.alibaba.druid.util.JdbcUtils;

import java.util.List;

/**
 * Created by linghf on 2016/8/30.
 */

public class DruidTest {
    public static void main(String[] args) {

        String sql = "select id,v2 from t1,t2 where t1.id=t2.id and t1.id <2 and t2.id < 3 order by a limit 10";
        StringBuffer selectList = new StringBuffer();
        StringBuffer select = new StringBuffer();
        StringBuffer from = new StringBuffer();
        StringBuffer where = new StringBuffer();
        StringBuffer order = new StringBuffer();
        StringBuffer limit = new StringBuffer();
// parser得到AST
        SQLStatementParser parser = new MySqlStatementParser(sql);
        List<SQLStatement> stmtList = parser.parseStatementList();
// 将AST通过visitor输出
        SQLASTOutputVisitor selectVisitor = SQLUtils.createFormatOutputVisitor(select, stmtList, JdbcUtils.MYSQL);
        SQLASTOutputVisitor fromVisitor = SQLUtils.createFormatOutputVisitor(from, stmtList, JdbcUtils.MYSQL);
        SQLASTOutputVisitor whereVisitor = SQLUtils.createFormatOutputVisitor(where, stmtList, JdbcUtils.MYSQL);
        SQLASTOutputVisitor orderVisitor = SQLUtils.createFormatOutputVisitor(order, stmtList, JdbcUtils.MYSQL);
        SQLASTOutputVisitor limitVisitor = SQLUtils.createFormatOutputVisitor(limit, stmtList, JdbcUtils.MYSQL);
        for (SQLStatement stmt : stmtList) {
//stmt.accept(visitor);
            if (stmt instanceof SQLSelectStatement) {
                SQLSelectStatement sstmt = (SQLSelectStatement) stmt;
                SQLSelect sqlselect = sstmt.getSelect();
                MySqlSelectQueryBlock query = (MySqlSelectQueryBlock) sqlselect.getQuery();
                query.getFrom().accept(fromVisitor);
                List<SQLSelectItem> items = query.getSelectList();

                for (SQLSelectItem item : items) {
                    item.accept(selectVisitor);
                    selectList.append(select).append(",");
                }
                query.getWhere().accept(whereVisitor);
                query.getOrderBy().accept(orderVisitor);
                query.getLimit().accept(limitVisitor);
            }
            System.out.println("from:" + from.toString());
            System.out.println("select:" + selectList.deleteCharAt(selectList.length() - 1));
            System.out.println("where:" + where);
            System.out.println("order:" + order);
            System.out.println("limit:" + limit);
        }

    }
}
