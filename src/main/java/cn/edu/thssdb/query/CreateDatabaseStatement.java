package cn.edu.thssdb.query;

import cn.edu.thssdb.parser.SQLParser;
import cn.edu.thssdb.parser.SQLParser.Drop_db_stmtContext;
import cn.edu.thssdb.parser.SQLParser.Sql_stmtContext;
import cn.edu.thssdb.schema.Manager;

public class CreateDatabaseStatement extends Statement {
    private String dbname;

    public CreateDatabaseStatement(Manager manager, Sql_stmtContext ctx) {
        super(manager, ctx);
    }

    @Override
    public final void parse() {
        SQLParser.Create_db_stmtContext ctx = this.parseCtx.create_db_stmt();
        this.dbname = ctx.database_name().getText();
    }

    @Override
    public final void execute() {
        this.manager.createDatabaseIfNotExists(this.dbname);
    }

    @Override
    public final String getResult() {
        return String.format("Create database %s success!", this.dbname);
    }
}