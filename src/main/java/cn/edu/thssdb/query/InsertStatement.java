package cn.edu.thssdb.query;

import cn.edu.thssdb.parser.SQLParser;
import cn.edu.thssdb.parser.SQLParser.Sql_stmtContext;
import cn.edu.thssdb.schema.*;
import cn.edu.thssdb.type.ColumnInfo;
import javafx.scene.control.Tab;

public class InsertStatement extends Statement {
    private String result = "";
    private SQLParser.Insert_stmtContext ctx;

    public InsertStatement(Manager manager, Sql_stmtContext parseCtx) {
        super(manager, parseCtx);
    }

    @Override
    public final void parse() {
        ctx = this.parseCtx.insert_stmt();
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
                    ctx.column_name(),
                    ctx.value_entry(0).literal_value().get(0)
            );
            return;
        }
        */
        String[] names = new String[ctx.column_name().size()];
        for (int i = 0; i < names.length; ++i) {
            names[i] = ctx.column_name(i).getText();
        }

        String[] values = new String[ctx.value_entry(0).literal_value().size()];
        for (int i = 0; i < values.length; ++i) {
            values[i] = ctx.value_entry(0).literal_value(i).getText();
        }

        Table table = db.getTables().get(tableName);
        try {
            Row row = table.createRow(names, values);
            table.insert(row);
            result = "Insert successfully\n";
        }
        catch (Exception e) {
            e.printStackTrace();
            result = e.toString();
            //result = Integer.toString(values.length) + " " + Integer.toString(names.length);
        }
    }

    @Override
    public final String getResult() {
        return this.result;
    }
}