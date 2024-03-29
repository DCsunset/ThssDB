package cn.edu.thssdb.query;

import cn.edu.thssdb.parser.SQLLexer;
import cn.edu.thssdb.parser.SQLParser;
import cn.edu.thssdb.parser.SQLParser.ParseContext;
import cn.edu.thssdb.parser.SQLParser.Sql_stmtContext;
import cn.edu.thssdb.schema.Database;
import cn.edu.thssdb.schema.Manager;
import org.antlr.v4.runtime.CharStreams;
import org.antlr.v4.runtime.CommonTokenStream;

public class DeleteTest {
    public static void main(String[] args) {
        // Create database
        Manager manager = new Manager();
        manager.createDatabaseIfNotExists("db1");
        manager.switchDatabase("db1");
        Database db = manager.currentDatabase;

        // Create table (commented when table exists)
        String str = "create TABLE person (name String(256), id Int not null, PRIMARY KEY(ID));"
                + "insert into person values('test-1', 1);" + "insert into person values('hello', 2);"
                + "delete from person where id = 2 || name='test-1';";

        SQLLexer lexer = new SQLLexer(CharStreams.fromString(str));
        CommonTokenStream tokens = new CommonTokenStream(lexer);
        SQLParser parser = new SQLParser(tokens);
        ParseContext root = parser.parse();
        /*
        for (int i = 0; i < root.sql_stmt_list().sql_stmt().size(); i++) {
            Sql_stmtContext stmtCtx = root.sql_stmt_list().sql_stmt().get(i);
            Statement stmt = null;
            int type = stmtCtx.getStart().getType();
            if (type == SQLParser.K_SHOW) {
                stmt = new ShowTableStatement(manager, stmtCtx);
            } else if (type == SQLParser.K_DROP) {
                if (stmtCtx.drop_db_stmt() != null) {
                    stmt = new DropDatabaseStatement(manager, stmtCtx);
                } else {
                    stmt = new DropTableStatement(manager, stmtCtx);
                }
            }
            // else if (type == SQLParser.K_CREATE) {
            //     stmt = new CreateTableStatement(manager, stmtCtx);
            // }
            // else if (type == SQLParser.K_INSERT) {
            // stmt = new InsertStatement(manager, stmtCtx);
            // } else if (type == SQLParser.K_UPDATE) {
            // stmt = new UpdateStatement(manager, stmtCtx);
            // } else if (type == SQLParser.K_DELETE) {
            // stmt = new DeleteStatement(manager, stmtCtx);
            // }
            try {
                stmt.parse();
                stmt.execute();
                System.out.println(stmt.getResult());
            } catch (Exception e) {
                e.printStackTrace();
                System.out.println(e.getMessage());
            }
        }
         */
    }
}