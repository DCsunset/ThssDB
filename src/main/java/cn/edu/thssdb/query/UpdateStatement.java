package cn.edu.thssdb.query;

import cn.edu.thssdb.exception.ColumnNotExistException;
import cn.edu.thssdb.exception.TableNotExistException;
import cn.edu.thssdb.parser.SQLParser;
import cn.edu.thssdb.parser.SQLParser.Sql_stmtContext;
import cn.edu.thssdb.schema.*;
import cn.edu.thssdb.transaction.Transaction;
import cn.edu.thssdb.utils.Global;
import javafx.util.Pair;

import java.util.Iterator;

public class UpdateStatement extends Statement {
    private Table table;
    // Column index
    private int index;
    private Comparable value;
    private SQLParser.Update_stmtContext ctx;
    private Transaction transaction;

    public UpdateStatement(Manager manager, Sql_stmtContext parseCtx, Transaction transaction) {
        super(manager, parseCtx);
        this.transaction = transaction;
    }

    @Override
    public final void parse() throws Exception {
        ctx = this.parseCtx.update_stmt();
        Database db = this.manager.currentDatabase;
        String tableName = ctx.table_name().getText();
        if (!db.getTables().containsKey(tableName)) {
            throw new TableNotExistException(tableName);
        }

        table = db.getTables().get(tableName);

        index = table.findColumnByName(ctx.column_name().getText());
        if (index < 0)
            throw new ColumnNotExistException(ctx.column_name().getText());

        // set attr=value
        value = table.stringToValue(table.getMetadata().columns[index], ctx.expression().getText());
    }

    @Override
    public final void execute() throws Exception {
        this.transaction.acquireLock(this.table.lock);
        MultipleCondition condition = new MultipleCondition(table, ctx.multiple_condition());

        // where attr=value
        int count = 0;
        Iterator<Pair<Entry, VRow>> it = table.iterator();
        while (it.hasNext()) {
            Pair<Entry, VRow> item = it.next();
            Row row = table.read(item.getValue());
            boolean ok = condition.satisfy(row);
            if (ok) {
                Entry entry = item.getKey();
                Entry[] entries = new Entry[row.getEntries().size()];
                entries = row.getEntries().toArray(entries);
                entries[index].value = value;
                table.update(transaction.uuid, new Row(entries), entry);
                ++count;
            }
        }
        result = constructSuccessResp(String.format("Updated %d rows", count));
    }
}