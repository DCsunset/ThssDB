package cn.edu.thssdb.query;

import cn.edu.thssdb.exception.CurrentDatabaseNullException;
import cn.edu.thssdb.exception.TableNotExistException;
import cn.edu.thssdb.parser.SQLParser;
import cn.edu.thssdb.parser.SQLParser.Sql_stmtContext;
import cn.edu.thssdb.schema.*;
import cn.edu.thssdb.transaction.Transaction;
import cn.edu.thssdb.utils.Global;
import com.sun.org.apache.xpath.internal.operations.Mult;

import java.util.ArrayList;
import java.util.LinkedList;

public class SelectStatement extends Statement {
    private SQLParser.Select_stmtContext ctx;
    private Transaction transaction;

    public SelectStatement(Manager manager, Sql_stmtContext parseCtx, Transaction transaction) {
        super(manager, parseCtx);
        this.transaction = transaction;
    }

    @Override
    public final void parse() {
        ctx = this.parseCtx.select_stmt();
    }

    @Override
    public final void execute() throws Exception {
        Database db = this.manager.currentDatabase;
        QueryTable resultTable = null;

        /*
         * System.out.println(ctx.table_query(0).getText());
         * System.out.println(ctx.table_query(0).table_name().size());
         * System.out.println(ctx.table_query(0).table_name(0).getText());
         */

        SQLParser.Table_queryContext tbCtx = ctx.table_query(0);
        ArrayList<Table> tables = new ArrayList<Table>();

        for (int i = 0; i < tbCtx.table_name().size(); ++i) {
            String tableName = tbCtx.table_name(i).getText().toUpperCase();
            if (db == null) {
                throw new CurrentDatabaseNullException();
            }
            if (!db.getTables().containsKey(tableName)) {
                throw new TableNotExistException(tableName);
            }
            tables.add(db.getTables().get(tableName));
            this.transaction.acquireLock(tables.get(tables.size() - 1).lock);
            if (i == 0)
                resultTable = new QueryTable(db.getTables().get(tableName), tbCtx.table_name().size() > 1);
            else {
                QueryTable newTable = new QueryTable(db.getTables().get(tableName), true);
                resultTable = resultTable.join(newTable);
            }
        }

        if (tbCtx.table_name().size() > 1 && tbCtx.multiple_condition() != null) {
            MultipleCondition onCondition = new MultipleCondition(resultTable, tbCtx.multiple_condition());
            resultTable.filter(onCondition);
        }

        if (ctx.multiple_condition() != null) {
            MultipleCondition condition = new MultipleCondition(resultTable, ctx.multiple_condition());
            resultTable.filter(condition);
        }

        String columnNames[] = new String[ctx.result_column().size()];
        for (int i = 0; i < ctx.result_column().size(); ++i) {
            columnNames[i] = ctx.result_column(i).getText().toUpperCase();
        }
        if (!columnNames[0].equals("*"))
            resultTable = resultTable.project(columnNames);

        // release lock after read request
        /*
        for (Table table : tables) {
            this.transaction.releaseLock(table.lock);
        }
         */

        //resultTable.output();
        result = constructSuccessResp("");
        result.setHasResult(true);
        result.setColumnsList(resultTable.getColumns());
        result.setRowList(resultTable.getRows());
    }
}