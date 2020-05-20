package cn.edu.thssdb.query;

import cn.edu.thssdb.parser.SQLLexer;
import cn.edu.thssdb.parser.SQLParser;
import cn.edu.thssdb.parser.SQLParser.ParseContext;
import cn.edu.thssdb.parser.SQLParser.Sql_stmtContext;
import cn.edu.thssdb.schema.Condition;
import cn.edu.thssdb.schema.Database;
import cn.edu.thssdb.schema.Manager;
import cn.edu.thssdb.schema.Table;
import cn.edu.thssdb.utils.Global.OpType;
import org.antlr.v4.runtime.CharStreams;
import org.antlr.v4.runtime.CommonTokenStream;

public class SelectTest {
    public static void main(String[] args) {
        // Create database
        Manager manager = new Manager();
        manager.createDatabaseIfNotExists("db1");
        manager.switchDatabase("db1");
        Database db = manager.currentDatabase;

        // Create table (commented when table exists)
        String str = "create TABLE person (name String(256), ID Int not null, PRIMARY KEY(ID));"
                + "create table info (name String(256), password String(256), primary key(name));"
                + "create table grade (ID Int not null, grade Int, primary key(ID));"
                + "insert into person values ('Bob', 1);"
                + "insert into person values ('Alice',2);"
                + "insert into person values ('Ted',3);"
                + "insert into info values ('Alice','151515');"
                + "insert into info values ('Bob','123456');"
                + "insert into grade values (1, 80);"
                + "insert into grade values (2, 90);"
                + "insert into grade values (3, 92);"
                //+ "select * from person where ID = 10;";
                + "select * from person join info on person.name = info.name;";

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
            } else if (type == SQLParser.K_SELECT) {
                stmt = new SelectStatement(manager, stmtCtx);
            }
            try {
                stmt.parse();
                stmt.execute();
                System.out.println(stmt.getResult());
            }
            catch (Exception e) {
                e.printStackTrace();
                System.out.println(e.getMessage());
            }
        }

        // join
        /*
        Table person = db.getTables().get("person");
        Table info = db.getTables().get("info");

        new QueryTable(person).join(new QueryTable(info), "name", "name", null);
        new QueryTable(person).join(new QueryTable(info), "name", "name",
                new Condition(person, "name", OpType.EQ, "Bob"));

         */
    }
}