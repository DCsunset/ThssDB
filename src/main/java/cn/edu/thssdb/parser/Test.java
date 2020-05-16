package cn.edu.thssdb.parser;

import org.antlr.runtime.tree.ParseTree;
import org.antlr.v4.runtime.CharStreams;
import org.antlr.v4.runtime.CommonTokenStream;

import cn.edu.thssdb.parser.SQLParser.ConditionContext;
import cn.edu.thssdb.parser.SQLParser.ExpressionContext;
import cn.edu.thssdb.parser.SQLParser.Multiple_conditionContext;
import cn.edu.thssdb.parser.SQLParser.ParseContext;
import cn.edu.thssdb.parser.SQLParser.Sql_stmtContext;

import java.io.*;
import java.util.concurrent.locks.Condition;

public class Test {
    public static void main(String[] args) {
        String str = "SELECT CUST_NAME, id FROM CUSTOMERS WHERE CUST_NAME = 'Kash%'; insert into stu where id = '101';insert into stu where id=101;";

        SQLLexer lexer = new SQLLexer(CharStreams.fromString(str));
        CommonTokenStream tokens = new CommonTokenStream(lexer);
        SQLParser parser = new SQLParser(tokens);
        ParseContext root = parser.parse();
        System.out.println(root.sql_stmt_list().sql_stmt().size());
        int type = root.sql_stmt_list().sql_stmt().get(0).getStart().getType();
        if (type == parser.K_SELECT) {
            // parse select
            SQLParser.Select_stmtContext ctx = root.sql_stmt_list().sql_stmt().get(0).select_stmt();
            System.out.println(ctx.toStringTree());
            System.out.println(ctx.result_column().size());
            System.out.println(ctx.result_column().get(1).getText());
            System.out.println(ctx.table_query().get(0).getText());
            Multiple_conditionContext conditions = ctx.multiple_condition();
            // parse first condition
            ConditionContext condition = conditions.condition();
            System.out.println(condition.getText());
            System.out.println(condition.expression().size());
            System.out.println(condition.expression().get(0).getText());
            System.out.println(condition.expression().get(1).getText());
            System.out.println(condition.comparator().getText());
            // check whether other condition exists
            System.out.println(conditions.multiple_condition().size());
        }
        Sql_stmtContext s2 = root.sql_stmt_list().sql_stmt().get(1);
        type = s2.getStart().getType();
        if (type == parser.K_INSERT) {
            System.out.print("insert detected");
        }
    }
}