package cn.edu.thssdb.transaction;

import cn.edu.thssdb.parser.SQLLexer;
import cn.edu.thssdb.parser.SQLParser;
import cn.edu.thssdb.parser.SQLParser.ParseContext;
import cn.edu.thssdb.parser.SQLParser.Sql_stmtContext;
import cn.edu.thssdb.query.CreateTableStatement;
import cn.edu.thssdb.query.DeleteStatement;
import cn.edu.thssdb.query.DropDatabaseStatement;
import cn.edu.thssdb.query.InsertStatement;
import cn.edu.thssdb.query.SelectStatement;
import cn.edu.thssdb.query.ShowTableStatement;
import cn.edu.thssdb.query.Statement;
import cn.edu.thssdb.query.UpdateStatement;
import cn.edu.thssdb.schema.Database;
import cn.edu.thssdb.schema.Manager;
import org.antlr.v4.runtime.CharStreams;
import org.antlr.v4.runtime.CommonTokenStream;

public class TransactionTest {
    private static class Thread1 extends Thread {
        Transaction t;
        Manager manager;

        public Thread1(Manager m) {
            this.t = new Transaction();
            manager = m;
        }

        public void run() {
            String str = "create TABLE person (name String(256), id Int not null, PRIMARY KEY(ID));"
                    + "insert into person values('test-1', 1);" + "insert into person values('hello', 2);";
            // + "delete from person where id = 2 || name='test-1';";
            SQLLexer lexer = new SQLLexer(CharStreams.fromString(str));
            CommonTokenStream tokens = new CommonTokenStream(lexer);
            SQLParser parser = new SQLParser(tokens);
            ParseContext root = parser.parse();
            for (int i = 0; i < root.sql_stmt_list().sql_stmt().size(); i++) {
                Sql_stmtContext stmtCtx = root.sql_stmt_list().sql_stmt().get(i);
                Statement stmt = null;
                int type = stmtCtx.getStart().getType();
                if (type == SQLParser.K_CREATE) {
                    stmt = new CreateTableStatement(manager, stmtCtx);
                } else if (type == SQLParser.K_INSERT) {
                    stmt = new InsertStatement(manager, stmtCtx, t);
                } else if (type == SQLParser.K_UPDATE) {
                    stmt = new UpdateStatement(manager, stmtCtx, t);
                } else if (type == SQLParser.K_DELETE) {
                    stmt = new DeleteStatement(manager, stmtCtx, t);
                }
                try {
                    stmt.parse();
                    stmt.execute();
                    System.out.println(stmt.getResult());
                } catch (Exception e) {
                    e.printStackTrace();
                    System.out.println(e.getMessage());
                }
            }
            try {
                Thread.sleep(3000);
            } catch (Exception e) {

            }
            t.commit();
            System.out.println("commit");
        }
    }

    private static class Thread2 extends Thread {
        Transaction t;
        Manager manager;

        public Thread2(Manager m) {
            this.t = new Transaction();
            manager = m;
        }

        public void run() {
            String str = "select * from person;";
            SQLLexer lexer = new SQLLexer(CharStreams.fromString(str));
            CommonTokenStream tokens = new CommonTokenStream(lexer);
            SQLParser parser = new SQLParser(tokens);
            ParseContext root = parser.parse();
            for (int i = 0; i < root.sql_stmt_list().sql_stmt().size(); i++) {
                Sql_stmtContext stmtCtx = root.sql_stmt_list().sql_stmt().get(i);
                Statement stmt = null;
                int type = stmtCtx.getStart().getType();
                if (type == SQLParser.K_SELECT) {
                    stmt = new SelectStatement(manager, stmtCtx, t);
                }
                try {
                    stmt.parse();
                    System.out.println("before select");
                    stmt.execute();
                    System.out.println("after select");
                    System.out.println(stmt.getResult());
                } catch (Exception e) {
                    e.printStackTrace();
                    System.out.println(e.getMessage());
                }
            }
        }

    }

    public static void main(String[] args) {
        // Create database
        Manager manager = new Manager();
        manager.createDatabaseIfNotExists("db1");
        manager.switchDatabase("db1");
        Database db = manager.currentDatabase;
        new Thread1(manager).start();
        try {
            Thread.sleep(1000);
        } catch (Exception e) {

        }
        new Thread2(manager).start();
    }
}