package cn.edu.thssdb.query;

import cn.edu.thssdb.parser.SQLParser;
import cn.edu.thssdb.parser.SQLParser.Sql_stmtContext;
import cn.edu.thssdb.schema.*;
import cn.edu.thssdb.utils.Global;
import javafx.util.Pair;

import java.util.Iterator;

public class UpdateStatement extends Statement {
    private String result = "";
    private Table table;
    // Column index
    private int index;
    private Comparable value;
    private SQLParser.Update_stmtContext ctx;

    public UpdateStatement(Manager manager, Sql_stmtContext parseCtx) {
        super(manager, parseCtx);
    }

    @Override
    public final void parse() throws Exception {
        ctx = this.parseCtx.update_stmt();
        Database db = this.manager.currentDatabase;
        String tableName = ctx.table_name().getText();
        if (!db.getTables().containsKey(tableName)) {
            throw new Exception("Table does not exist");
        }

        table = db.getTables().get(tableName);

        index = table.findColumnByName(ctx.column_name().getText());
        if (index < 0)
            throw new Exception(String.format("Column %s does not exist", ctx.column_name().getText()));

        // set attr=value
        value = table.stringToValue(
                table.getMetadata().columns[index],
                ctx.expression().getText()
        );
    }

    @Override
    public final void execute() throws Exception {
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
                table.update(new Row(entries), entry);
                ++count;
            }
        }
        result = String.format("Updated %d rows", count);
    }

    @Override
    public final String getResult() {
        return this.result;
    }
}