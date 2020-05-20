package cn.edu.thssdb.schema;

import org.antlr.v4.runtime.CharStreams;
import org.antlr.v4.runtime.CommonTokenStream;

import cn.edu.thssdb.parser.SQLLexer;
import cn.edu.thssdb.parser.SQLParser;
import cn.edu.thssdb.parser.SQLParser.Multiple_conditionContext;
import cn.edu.thssdb.parser.SQLParser.ParseContext;

public class MultipleConditionTest {
    public static void main(String[] args) throws Exception {
        String str = "select * from table where id=1 && name='hello' || password='c' && q=1234 || p='1212'";

        SQLLexer lexer = new SQLLexer(CharStreams.fromString(str));
        CommonTokenStream tokens = new CommonTokenStream(lexer);
        SQLParser parser = new SQLParser(tokens);
        ParseContext root = parser.parse();
        Multiple_conditionContext ctx = root.sql_stmt_list().sql_stmt().get(0).select_stmt().multiple_condition();
        MultipleCondition mc = new MultipleCondition(null, ctx);
    }
}