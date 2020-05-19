package cn.edu.thssdb.query;

import java.beans.Expression;
import java.security.cert.PKIXRevocationChecker.Option;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Iterator;
import javafx.util.Pair;

import javax.swing.Painter;

import cn.edu.thssdb.parser.SQLParser.ComparatorContext;
import cn.edu.thssdb.parser.SQLParser.ConditionContext;
import cn.edu.thssdb.parser.SQLParser.Delete_stmtContext;
import cn.edu.thssdb.parser.SQLParser.ExpressionContext;
import cn.edu.thssdb.parser.SQLParser.Multiple_conditionContext;
import cn.edu.thssdb.parser.SQLParser.ParseContext;
import cn.edu.thssdb.parser.SQLParser.Sql_stmtContext;
import cn.edu.thssdb.schema.Condition;
import cn.edu.thssdb.schema.Entry;
import cn.edu.thssdb.schema.Manager;
import cn.edu.thssdb.schema.Row;
import cn.edu.thssdb.schema.Table;
import cn.edu.thssdb.schema.VRow;
import cn.edu.thssdb.utils.Global.OpType;

public class DeleteStatement extends Statement {
    private String tbname;
    private Table table;
    private ArrayList<Condition> conditions = new ArrayList<Condition>();
    private String result;

    public DeleteStatement(Manager manager, Sql_stmtContext ctx) {
        super(manager, ctx);
    }

    @Override
    public final void parse() {
        Delete_stmtContext ctx = this.parseCtx.delete_stmt();
        this.tbname = ctx.table_name().getText();
        this.table = this.manager.currentDatabase.getTables().get(this.tbname);
        // TODO: condition
        Multiple_conditionContext conditions = ctx.multiple_condition();
        while (true) {
            ConditionContext conditionCtx = conditions.condition();
            // process this condition
            ExpressionContext attrCtx = conditionCtx.expression().get(0);
            ComparatorContext opCtx = conditionCtx.comparator();
            ExpressionContext valueCtx = conditionCtx.expression().get(1);
            OpType type = OpType.EQ;
            if (opCtx.EQ() != null) {
                type = OpType.EQ;
            } else if (opCtx.GE() != null) {
                type = OpType.GE;
            } else if (opCtx.GT() != null) {
                type = OpType.GT;
            } else if (opCtx.LE() != null) {
                type = OpType.LE;
            } else if (opCtx.LT() != null) {
                type = OpType.LT;
            }
            Condition condition = new Condition(this.table, attrCtx.getText(), type, valueCtx.getText());
            this.conditions.add(condition);

            // check for next condition
            if (conditions.multiple_condition().size() == 0)
                break;
        }
    }

    @Override
    public final void execute() {
        Iterator<Pair<Entry, VRow>> it = this.table.iterator();
        int count = 0;
        while (it.hasNext()) {
            Pair<Entry, VRow> item = it.next();
            Row row = this.table.read(item.getValue());
            boolean ok = false;
            // TODO: multiple conditions
            for (int i = 0; i < this.conditions.size(); i++) {
                Condition condition = this.conditions.get(i);
                if (condition.satisfy(row)) {
                    ok = true;
                }
            }
            if (ok) {
                count++;
                Entry entry = item.getKey();
                table.delete(entry);
                result = String.format("Deleted %d rows", count);
            }
        }
    }

    @Override
    public final String getResult() {
        return result;
    }
}