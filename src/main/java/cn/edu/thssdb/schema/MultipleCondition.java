package cn.edu.thssdb.schema;

import java.util.ArrayList;
import cn.edu.thssdb.schema.*;
import cn.edu.thssdb.utils.Global.OpType;
import cn.edu.thssdb.parser.SQLParser.ComparatorContext;
import cn.edu.thssdb.parser.SQLParser.ConditionContext;
import cn.edu.thssdb.parser.SQLParser.Multiple_conditionContext;

public class MultipleCondition {
    public ArrayList<ArrayList<Condition>> conditions = new ArrayList<ArrayList<Condition>>();
    private AbstractTable table;

    public MultipleCondition(AbstractTable table, Multiple_conditionContext ctx) throws Exception {
        this.table = table;
        parseCtx(ctx);
    }

    public static OpType ctxtotype(ComparatorContext opCtx) {
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
        return type;
    }

    private void parseCtx(Multiple_conditionContext ctx) throws Exception {
        if (ctx.OR() == null) { // terminal node
            conditions.add(new ArrayList<Condition>());
            parseTerminalNode(ctx);
        } else {
            parseCtx(ctx.multiple_condition(0));
            parseCtx(ctx.multiple_condition(1));
        }
    }

    private void parseTerminalNode(Multiple_conditionContext ctx) throws Exception {
        if (ctx.AND() == null) {
            ConditionContext cctx = ctx.condition();
            Condition condition = new Condition(table,
                    cctx.expression().get(0).getText(),
                    ctxtotype(cctx.comparator()),
                    cctx.expression().get(1).getText()
            );
            conditions.get(conditions.size() - 1).add(condition);
        } else {
            parseTerminalNode(ctx.multiple_condition(0));
            parseTerminalNode(ctx.multiple_condition(1));
        }
    }

    public boolean satisfy(Row row) throws Exception {
        for (ArrayList<Condition> andConditions : this.conditions) {
            boolean result = true;
            for (Condition condition : andConditions) {
                result = result && condition.satisfy(row);
            }
            if (result) {
                return true;
            }
        }
        return false;
    }
}