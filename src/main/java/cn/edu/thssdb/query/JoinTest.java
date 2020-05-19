package cn.edu.thssdb.query;

import cn.edu.thssdb.parser.SQLLexer;
import cn.edu.thssdb.parser.SQLParser;
import cn.edu.thssdb.parser.SQLParser.ParseContext;
import cn.edu.thssdb.parser.SQLParser.Sql_stmtContext;
import cn.edu.thssdb.schema.*;
import cn.edu.thssdb.type.ColumnInfo.ColumnType;
import cn.edu.thssdb.utils.Global.OpType;

import org.antlr.v4.runtime.CharStreams;
import org.antlr.v4.runtime.CommonTokenStream;

public class JoinTest {
    public static void main(String[] args) {
        // Create database
        Manager manager = new Manager();
        manager.createDatabaseIfNotExists("db1");
        manager.switchDatabase("db1");
        Database db = manager.currentDatabase;

        // Create table (commented when table exists)
        String str = "create TABLE person (name String(256), ID Int not null, PRIMARY KEY(ID));"
                + "create table info (name String(256), password String(256), primary key(name));"
                + "insert into person values ('Bob', 15);" + "insert into person values ('Alice',19);"
                + "insert into person values ('Ted',10);" + "insert into info values ('Alice','151515');"
                + "insert into info values ('Bob','123456');";

        SQLLexer lexer = new SQLLexer(CharStreams.fromString(str));
        CommonTokenStream tokens = new CommonTokenStream(lexer);
        SQLParser parser = new SQLParser(tokens);
        ParseContext root = parser.parse();
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
            } else if (type == SQLParser.K_CREATE) {
                stmt = new CreateTableStatement(manager, stmtCtx);
            } else if (type == SQLParser.K_INSERT) {
                stmt = new InsertStatement(manager, stmtCtx);
            }
            stmt.parse();
            stmt.execute();
            System.out.println(stmt.getResult());
        }

        // join
        Table person = db.getTables().get("person");
        Table info = db.getTables().get("info");

        new QueryTable(person).join(new QueryTable(info), "name", "name", null);
        new QueryTable(person).join(new QueryTable(info), "name", "name",
                new Condition(person, "name", OpType.EQ, "'Bob'"));
    }
}