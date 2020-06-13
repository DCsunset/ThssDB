package cn.edu.thssdb.executor;

import cn.edu.thssdb.parser.SQLLexer;
import cn.edu.thssdb.parser.SQLParser;
import cn.edu.thssdb.query.*;
import cn.edu.thssdb.rpc.thrift.ExecuteMultiStatementResp;
import cn.edu.thssdb.rpc.thrift.ExecuteStatementResp;
import cn.edu.thssdb.rpc.thrift.Status;
import cn.edu.thssdb.schema.Manager;
import cn.edu.thssdb.transaction.Log;
import cn.edu.thssdb.transaction.Savepoint;
import cn.edu.thssdb.transaction.SimpleLog;
import cn.edu.thssdb.transaction.Transaction;
import cn.edu.thssdb.transaction.Log.LogType;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.antlr.v4.runtime.CharStreams;
import org.antlr.v4.runtime.CommonTokenStream;

import static cn.edu.thssdb.query.Statement.constructErrorResp;
import static cn.edu.thssdb.query.Statement.constructSuccessResp;

public class SQLExecutor {
    Transaction transaction = null;

    public ExecuteStatementResp execute(String sqlStatement) {
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
                    return constructErrorResp("Already in transaction");
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
                return constructSuccessResp("Transaction begins");
            } else if (stmtCtx.commit_stmt() != null) {
                if (transaction == null) {
                    return constructErrorResp("No transaction begins");
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
                return constructSuccessResp("Transaction committed");
            } else if (stmtCtx.rollback_stmt() != null) {
                if (transaction == null) {
                    return constructErrorResp("No transaction begins");
                }
                try {
                    transaction.rollback();
                } catch (Exception e) {
                    e.printStackTrace();
                }
                transaction = null;
                System.out.println("Transaction rollback");
                return constructSuccessResp("Transaction rollback success");
            } else if (stmtCtx.delete_stmt() != null) {
                stmt = new DeleteStatement(manager, stmtCtx, t);
            } else if (stmtCtx.checkpoint_stmt() != null) {
                Savepoint.save();
                System.out.println("Checkpoint saved");
                return constructSuccessResp("Checkpoint saved");
            } else if (stmtCtx.drop_db_stmt() != null) {
                stmt = new DropDatabaseStatement(manager, stmtCtx);
            } else if (stmtCtx.drop_table_stmt() != null) {
                stmt = new DropTableStatement(manager, stmtCtx, t);
            } else if (stmtCtx.use_db_stmt() != null) {
                stmt = new UseDatabaseStatement(manager, stmtCtx);
            } else if (stmtCtx.show_meta_stmt() != null) {
                stmt = new ShowTableStatement(manager, stmtCtx);
            } else {
                return constructErrorResp("Invalid SQL statement");
            }

            try {
                stmt.parse();
                stmt.execute();
                if (transaction == null)
                    t.commit();
                return stmt.getResult();
            } catch (Exception e) {
                if (stmtCtx.transaction_stmt() != null) {
                    return Statement.constructSuccessResp("Transaction begins successfully!");
                } else if (stmtCtx.rollback_stmt() != null) {
                    return Statement.constructSuccessResp("Transaction rollback successfully!");
                }
                e.printStackTrace();
                System.err.println(e.getMessage());
                return constructErrorResp(e.getMessage());
            }
        }

        return constructErrorResp("Invalid SQL statement");
    }

    public ExecuteMultiStatementResp executeMultiStatement(String sqlStatement) {
        Manager manager = Manager.getInstance();
        SQLLexer lexer = new SQLLexer(CharStreams.fromString(sqlStatement));
        CommonTokenStream tokens = new CommonTokenStream(lexer);
        SQLParser parser = new SQLParser(tokens);
        SQLParser.ParseContext root = parser.parse();

        List<ExecuteStatementResp> responses = new ArrayList<>();
        ExecuteMultiStatementResp result = new ExecuteMultiStatementResp();

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
                    responses.add(constructErrorResp("Already in transaction"));
                    result.setResults(responses);
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
                responses.add(constructSuccessResp("Transaction begins"));
                continue;
            } else if (stmtCtx.commit_stmt() != null) {
                if (transaction == null) {
                    responses.add(constructErrorResp("No transaction begins"));
                    result.setResults(responses);
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
                responses.add(constructSuccessResp("Transaction committed"));
                continue;
            } else if (stmtCtx.rollback_stmt() != null) {
                if (transaction == null) {
                    responses.add(constructErrorResp("No transaction begins"));
                    result.setResults(responses);
                    return result;
                }
                try {
                    transaction.rollback();
                } catch (Exception e) {
                    e.printStackTrace();
                }
                transaction = null;
                System.out.println("Transaction rollback");
                responses.add(constructSuccessResp("Transaction rollback success"));
                continue;
            } else if (stmtCtx.delete_stmt() != null) {
                stmt = new DeleteStatement(manager, stmtCtx, t);
            } else if (stmtCtx.checkpoint_stmt() != null) {
                Savepoint.save();
                System.out.println("Checkpoint saved");
                responses.add(constructSuccessResp("Checkpoint saved"));
                continue;
            } else if (stmtCtx.drop_db_stmt() != null) {
                stmt = new DropDatabaseStatement(manager, stmtCtx);
            } else if (stmtCtx.drop_table_stmt() != null) {
                stmt = new DropTableStatement(manager, stmtCtx, t);
            } else if (stmtCtx.use_db_stmt() != null) {
                stmt = new UseDatabaseStatement(manager, stmtCtx);
            } else if (stmtCtx.show_meta_stmt() != null) {
                stmt = new ShowTableStatement(manager, stmtCtx);
            } else {
                responses.add(constructErrorResp("Invalid SQL statement"));
                result.setResults(responses);
                return result;
            }

            try {
                stmt.parse();
                stmt.execute();
                if (transaction == null)
                    t.commit();
                responses.add(stmt.getResult());
            } catch (Exception e) {
                if (stmtCtx.transaction_stmt() != null) {
                    responses.add(Statement.constructSuccessResp("Transaction begins successfully!"));
                } else if (stmtCtx.rollback_stmt() != null) {
                    responses.add(Statement.constructSuccessResp("Transaction rollback successfully!"));
                } else {
                    e.printStackTrace();
                    String msg = e.getMessage();
                    if (msg == null)
                        msg = "Invalid SQL query";
                    responses.add(constructErrorResp(msg));
                    result.setResults(responses);
                    return result;
                }
            }
        }

        result.setResults(responses);
        return result;
    }
}
