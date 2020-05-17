package cn.edu.thssdb.query;

import cn.edu.thssdb.parser.SQLParser.Show_meta_stmtContext;
import cn.edu.thssdb.parser.SQLParser.Sql_stmtContext;
import cn.edu.thssdb.schema.Manager;

public class ShowTableStatement extends Statement {
    private String tablename;

    public ShowTableStatement(Manager manager, Sql_stmtContext parseCtx) {
        super(manager, parseCtx);
        System.out.println("dbinfo:" + manager.currentDatabase.name + " " + manager.currentDatabase.getTables());
    }

    @Override
    public final void parse() {
        Show_meta_stmtContext ctx = this.parseCtx.show_meta_stmt();
        this.tablename = ctx.table_name().getText();
    }

    @Override
    public final void execute() {

    }

    @Override
    public final String getResult() {
        return "show table";
    }
}