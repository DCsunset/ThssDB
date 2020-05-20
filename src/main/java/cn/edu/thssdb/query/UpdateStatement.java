package cn.edu.thssdb.query;

import cn.edu.thssdb.parser.SQLParser;
import cn.edu.thssdb.parser.SQLParser.Sql_stmtContext;
import cn.edu.thssdb.schema.*;
import cn.edu.thssdb.utils.Global;
import javafx.util.Pair;

import java.util.Iterator;

public class UpdateStatement extends Statement {
    private String result = "";
    private SQLParser.Update_stmtContext ctx;

    public UpdateStatement(Manager manager, Sql_stmtContext parseCtx) {
        super(manager, parseCtx);
    }

    @Override
    public final void parse() {
        ctx = this.parseCtx.update_stmt();
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
        else {
            result = String.format("%s %s",
                    ctx.column_name().getText(),
                    ctx.multiple_condition().condition().expression(1).getText()
            );
            return;
        }
         */

        Table table = db.getTables().get(tableName);

        int index = table.findColumnByName(ctx.column_name().getText());

        // set attr=value
        Comparable value = table.stringToValue(
                table.getMetadata().columns[index],
                ctx.expression().getText()
        );

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