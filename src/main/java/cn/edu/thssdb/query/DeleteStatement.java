package cn.edu.thssdb.query;

import cn.edu.thssdb.parser.SQLParser.Sql_stmtContext;
import cn.edu.thssdb.schema.Manager;

public class DeleteStatement extends Statement {
    public DeleteStatement(Manager manager, Sql_stmtContext ctx) {
        super(manager, ctx);
    }

    @Override
    public final void parse() {

    }

    @Override
    public final void execute() {

    }

    @Override
    public final String getResult() {
        return "";
    }
}