package cn.edu.thssdb.query;

import cn.edu.thssdb.parser.SQLParser;
import cn.edu.thssdb.parser.SQLParser.Sql_stmtContext;
import cn.edu.thssdb.schema.*;
import cn.edu.thssdb.utils.Global;

import java.util.ArrayList;
import java.util.LinkedList;

public class SelectStatement extends Statement {
    private String result = "";
    private SQLParser.Select_stmtContext ctx;

    public SelectStatement(Manager manager, Sql_stmtContext parseCtx) {
        super(manager, parseCtx);
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
        System.out.println(ctx.table_query(0).getText());
        System.out.println(ctx.table_query(0).table_name().size());
        System.out.println(ctx.table_query(0).table_name(0).getText());
         */

        SQLParser.Table_queryContext tbCtx = ctx.table_query(0);
        SQLParser.ConditionContext onCtx = null;
        String attr1 = null, attr2 = null;
        if (tbCtx.multiple_condition() != null)
            onCtx = tbCtx.multiple_condition().condition();
        if (onCtx != null) {
            attr1 = onCtx.expression(0).getText();
            attr2 = onCtx.expression(1).getText();
        }

        for (int i = 0; i < tbCtx.table_name().size(); ++i) {
            String tableName = tbCtx.table_name(i).getText();
            if (!db.getTables().containsKey(tableName)) {
                result = "Table does not exist\n";
                return;
            }
            if (i == 0)
                resultTable = new QueryTable(db.getTables().get(tableName), tbCtx.table_name().size() > 1);
            else {
                QueryTable newTable = new QueryTable(db.getTables().get(tableName), true);
                resultTable = resultTable.join(newTable, attr1, attr2);
            }
        }

        if (ctx.multiple_condition() != null) {
            MultipleCondition condition = new MultipleCondition(resultTable, ctx.multiple_condition());
            resultTable.filter(condition);
        }

        String columnNames[] = new String[ctx.result_column().size()];
        for (int i = 0; i < ctx.result_column().size(); ++i) {
            columnNames[i] = ctx.result_column(i).getText();
        }
        if (!columnNames[0].equals("*"))
            resultTable = resultTable.project(columnNames);
        resultTable.output();
    }

    @Override
    public final String getResult() {
        return this.result;
    }
}