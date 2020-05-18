package cn.edu.thssdb.query;

import cn.edu.thssdb.parser.SQLParser.Delete_stmtContext;
import cn.edu.thssdb.parser.SQLParser.Sql_stmtContext;
import cn.edu.thssdb.schema.Manager;

public class DeleteStatement extends Statement {
    private String tbname;

    public DeleteStatement(Manager manager, Sql_stmtContext ctx) {
        super(manager, ctx);
    }

    @Override
    public final void parse() {
        Delete_stmtContext ctx = this.parseCtx.delete_stmt();
        this.tbname = ctx.table_name().getText();
        // TODO: condition
    }

    @Override
    public final void execute() {

    }

    @Override
    public final String getResult() {
        return "";
    }
}