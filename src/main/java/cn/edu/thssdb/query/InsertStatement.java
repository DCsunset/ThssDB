package cn.edu.thssdb.query;

import java.util.Dictionary;
import java.util.Hashtable;

import cn.edu.thssdb.parser.SQLParser;
import cn.edu.thssdb.parser.SQLParser.Sql_stmtContext;
import cn.edu.thssdb.schema.*;
import cn.edu.thssdb.transaction.Log;
import cn.edu.thssdb.transaction.Transaction;
import cn.edu.thssdb.transaction.Log.LogType;
import cn.edu.thssdb.type.ColumnInfo;
import javafx.scene.control.Tab;

public class InsertStatement extends Statement {
    private String result = "";
    private SQLParser.Insert_stmtContext ctx;
    private String names[];
    private String values[];

    private Transaction transaction;

    public InsertStatement(Manager manager, Sql_stmtContext parseCtx, Transaction transaction) {
        super(manager, parseCtx);
        this.transaction = transaction;
    }

    @Override
    public final void parse() {
        ctx = this.parseCtx.insert_stmt();

        names = new String[ctx.column_name().size()];
        for (int i = 0; i < names.length; ++i) {
            names[i] = ctx.column_name(i).getText();
        }

        values = new String[ctx.value_entry(0).literal_value().size()];
        for (int i = 0; i < values.length; ++i) {
            values[i] = ctx.value_entry(0).literal_value(i).getText();
        }
    }

    @Override
    public final void execute() throws Exception {
        Database db = this.manager.currentDatabase;
        String tableName = ctx.table_name().getText();
        if (!db.getTables().containsKey(tableName)) {
            result = "Table does not exist\n";
            return;
        }
        /*
         * else { result = String.format("%s %s", ctx.column_name(),
         * ctx.value_entry(0).literal_value().get(0) ); return; }
         */

        Table table = db.getTables().get(tableName);
        transaction.acquireLock(table.lock);
        Row row = table.createRow(names, values);
        table.insert(transaction.uuid, row);
        result = "Insert successfully\n";
    }

    @Override
    public final String getResult() {
        return this.result;
    }
}