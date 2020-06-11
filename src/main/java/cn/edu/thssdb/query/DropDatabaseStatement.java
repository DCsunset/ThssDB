package cn.edu.thssdb.query;

import cn.edu.thssdb.parser.SQLParser.Drop_db_stmtContext;
import cn.edu.thssdb.parser.SQLParser.Sql_stmtContext;
import cn.edu.thssdb.schema.Manager;

import javax.swing.*;

public class DropDatabaseStatement extends Statement {
    private String dbname;

    public DropDatabaseStatement(Manager manager, Sql_stmtContext ctx) {
        super(manager, ctx);
    }

    @Override
    public final void parse() {
        Drop_db_stmtContext ctx = this.parseCtx.drop_db_stmt();
        this.dbname = ctx.database_name().getText();
    }

    @Override
    public final void execute() {
        this.manager.deleteDatabase(this.dbname);
        result = constructSuccessResp(String.format("Delete database %s success!", this.dbname));
    }
}