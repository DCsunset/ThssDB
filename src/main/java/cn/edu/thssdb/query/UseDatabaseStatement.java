
package cn.edu.thssdb.query;

import cn.edu.thssdb.parser.SQLParser;
import cn.edu.thssdb.parser.SQLParser.Drop_table_stmtContext;
import cn.edu.thssdb.parser.SQLParser.Sql_stmtContext;
import cn.edu.thssdb.schema.Database;
import cn.edu.thssdb.schema.Manager;
import cn.edu.thssdb.transaction.Transaction;

public class UseDatabaseStatement extends Statement {
    private String dbname;

    public UseDatabaseStatement(Manager manager, Sql_stmtContext ctx) {
        super(manager, ctx);
    }

    @Override
    public final void parse() {
        SQLParser.Use_db_stmtContext ctx = this.parseCtx.use_db_stmt();
        this.dbname = ctx.database_name().getText().toUpperCase();
    }

    @Override
    public final void execute() {
        this.manager.switchDatabase(this.dbname);
        result = constructSuccessResp(String.format("Use db %s success!", this.dbname));
    }
}