package cn.edu.thssdb.query;

import cn.edu.thssdb.parser.SQLParser;
import cn.edu.thssdb.parser.SQLParser.Sql_stmtContext;
import cn.edu.thssdb.schema.*;
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
    public final void execute() {
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
        if (index == -1) {
            result = String.format("No column named %s", ctx.column_name().getText());
            return;
        }

        try {
            Comparable value = table.stringToValue(
                    table.getMetadata().columns[index],
                    ctx.expression().getText()
            );

            // TODO: multiple conditions
            int condition_cnt = 1;
            int[] condition_indices = new int[1];
            Comparable[] condition_values = new Comparable[1];
            for (int i = 0; i < condition_cnt; ++i) {
                SQLParser.ConditionContext cdt = ctx.multiple_condition().condition();
                condition_indices[i] = table.findColumnByName(cdt.expression(0).getText());
                if (condition_indices[i] == -1) {
                    result = String.format("No row named %s", cdt.expression(0).getText());
                    return;
                }
                condition_values[i] = table.stringToValue(
                        table.getMetadata().columns[condition_indices[i]],
                        cdt.expression(1).getText()
                );
            }

            int count = 0;
            Iterator<Pair<Entry, VRow>> it = table.iterator();
            while (it.hasNext()) {
                Pair<Entry, VRow> item = it.next();
                Row row = table.read(item.getValue());
                boolean ok = false;
                for (int i = 0; i < condition_cnt; ++i) {
                    Comparable recordValue = row.getEntries().get(condition_indices[i]).value;
                    if (recordValue.equals(condition_values[i])) {
                        ok = true;
                    }
                }
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
        catch (Exception e) {
            e.printStackTrace();
            result = e.getMessage();
            return;
        }
    }

    @Override
    public final String getResult() {
        return this.result;
    }
}