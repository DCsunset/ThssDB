package cn.edu.thssdb.query;

import java.beans.Expression;
import java.security.cert.PKIXRevocationChecker.Option;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Iterator;

import cn.edu.thssdb.exception.CurrentDatabaseNullException;
import cn.edu.thssdb.schema.*;
import cn.edu.thssdb.transaction.Transaction;
import javafx.util.Pair;

import javax.swing.*;

import cn.edu.thssdb.parser.SQLParser.ComparatorContext;
import cn.edu.thssdb.parser.SQLParser.ConditionContext;
import cn.edu.thssdb.parser.SQLParser.Delete_stmtContext;
import cn.edu.thssdb.parser.SQLParser.ExpressionContext;
import cn.edu.thssdb.parser.SQLParser.Multiple_conditionContext;
import cn.edu.thssdb.parser.SQLParser.ParseContext;
import cn.edu.thssdb.parser.SQLParser.Sql_stmtContext;
import cn.edu.thssdb.utils.Global.OpType;

public class DeleteStatement extends Statement {
    private String tbname;
    private Table table;
    private MultipleCondition condition;
    private Transaction transaction;

    public DeleteStatement(Manager manager, Sql_stmtContext ctx, Transaction transaction) {
        super(manager, ctx);
        this.transaction = transaction;
    }

    @Override
    public final void parse() throws Exception {
        if (manager.currentDatabase == null) {
            throw new CurrentDatabaseNullException();
        }
        Delete_stmtContext ctx = this.parseCtx.delete_stmt();
        this.tbname = ctx.table_name().getText().toUpperCase();
        this.table = this.manager.currentDatabase.getTables().get(this.tbname);

        condition = new MultipleCondition(this.table, ctx.multiple_condition());
    }

    @Override
    public final void execute() throws Exception {
        this.transaction.acquireLock(this.table.lock);
        Iterator<Pair<Entry, VRow>> it = this.table.iterator();
        int count = 0;
        while (it.hasNext()) {
            Pair<Entry, VRow> item = it.next();
            Row row = this.table.read(item.getValue());
            if (condition.satisfy(row)) {
                count++;
                Entry entry = item.getKey();
                table.delete(transaction.uuid, entry);
            }
        }
        result = constructSuccessResp(String.format("Deleted %d rows", count));
    }
}