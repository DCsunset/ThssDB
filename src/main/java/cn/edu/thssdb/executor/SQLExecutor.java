package cn.edu.thssdb.executor;

import cn.edu.thssdb.parser.SQLLexer;
import cn.edu.thssdb.parser.SQLParser;
import cn.edu.thssdb.query.*;
import cn.edu.thssdb.schema.Manager;
import cn.edu.thssdb.transaction.Log;
import cn.edu.thssdb.transaction.Savepoint;
import cn.edu.thssdb.transaction.SimpleLog;
import cn.edu.thssdb.transaction.Transaction;
import cn.edu.thssdb.transaction.Log.LogType;

import java.io.IOException;

import org.antlr.v4.runtime.CharStreams;
import org.antlr.v4.runtime.CommonTokenStream;

public class SQLExecutor {
    Transaction transaction = null;

    public String execute(String sqlStatement) {
        String result = "";
        Manager manager = Manager.getInstance();
        SQLLexer lexer = new SQLLexer(CharStreams.fromString(sqlStatement));
        CommonTokenStream tokens = new CommonTokenStream(lexer);
        SQLParser parser = new SQLParser(tokens);
        SQLParser.ParseContext root = parser.parse();

        for (int i = 0; i < root.sql_stmt_list().sql_stmt().size(); i++) {
            Transaction t = transaction;
            if (t == null) {
                // New temporary transaction
                t = new Transaction();
            }

            SQLParser.Sql_stmtContext stmtCtx = root.sql_stmt_list().sql_stmt().get(i);
            Statement stmt = null;
            if (stmtCtx.create_db_stmt() != null) {
                stmt = new CreateDatabaseStatement(manager, stmtCtx);
            } else if (stmtCtx.create_table_stmt() != null) {
                stmt = new CreateTableStatement(manager, stmtCtx, t);
            } else if (stmtCtx.insert_stmt() != null) {
                stmt = new InsertStatement(manager, stmtCtx, t);
            } else if (stmtCtx.update_stmt() != null) {
                stmt = new UpdateStatement(manager, stmtCtx, t);
            } else if (stmtCtx.select_stmt() != null) {
                stmt = new SelectStatement(manager, stmtCtx, t);
            } else if (stmtCtx.transaction_stmt() != null) {
                if (transaction != null) {
                    System.err.println("Already in transaction");
                    return result;
                }
                // Begin transaction
                transaction = t;
                System.out.println("Transaction begins");
                try {
                    new SimpleLog(transaction.uuid, Log.LogType.Start).serialize();
                } catch (IOException e) {
                    System.err.println(e.getMessage());
                    e.printStackTrace();
                }
            } else if (stmtCtx.commit_stmt() != null) {
                if (transaction == null) {
                    System.err.println("No transaction begins");
                    return result;
                }
                try {
                    new SimpleLog(transaction.uuid, LogType.Commit).serialize();
                } catch (IOException e) {
                    System.err.println(e.getMessage());
                    e.printStackTrace();
                }
                transaction.commit();
                transaction = null;
                System.out.println("Transaction committed");
            } else if (stmtCtx.rollback_stmt() != null) {
                if (transaction == null) {
                    System.err.println("No transaction begins");
                    return result;
                }
                try {
                    transaction.rollback();
                } catch (Exception e) {
                    e.printStackTrace();
                }
                transaction = null;
                System.out.println("Transaction rollback");
            } else if (stmtCtx.delete_stmt() != null) {
                stmt = new DeleteStatement(manager, stmtCtx, t);
            } else if (stmtCtx.checkpoint_stmt() != null) {
                Savepoint.save();
                System.out.println("Checkpoint saved");
            } else if (stmtCtx.drop_db_stmt() != null) {
                stmt = new DropDatabaseStatement(manager, stmtCtx);
            } else if (stmtCtx.drop_table_stmt() != null) {
                stmt = new DropTableStatement(manager, stmtCtx, t);
            } else {
                System.err.println("Invalid SQL statement");
                return result;
            }

            try {
                if (stmt != null) {
                    stmt.parse();
                    stmt.execute();
                    result += (stmt.getResult() + "\n");
                    System.out.println(stmt.getResult());
                    if (transaction == null)
                        t.commit();
                }
            } catch (Exception e) {
                e.printStackTrace();
                System.err.println(e.getMessage());
                return result;
            }
        }
        return result;
    }
}
