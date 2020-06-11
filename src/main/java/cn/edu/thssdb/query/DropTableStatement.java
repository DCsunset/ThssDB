
package cn.edu.thssdb.query;

import cn.edu.thssdb.parser.SQLParser.Drop_table_stmtContext;
import cn.edu.thssdb.parser.SQLParser.Sql_stmtContext;
import cn.edu.thssdb.rpc.thrift.ExecuteStatementResp;
import cn.edu.thssdb.schema.Database;
import cn.edu.thssdb.schema.Manager;
import cn.edu.thssdb.transaction.Transaction;

public class DropTableStatement extends Statement {
    private String tablename;
    private Transaction transaction;

    public DropTableStatement(Manager manager, Sql_stmtContext ctx, Transaction transaction) {
        super(manager, ctx);
        this.transaction = transaction;
    }

    @Override
    public final void parse() {
        Drop_table_stmtContext ctx = this.parseCtx.drop_table_stmt();
        this.tablename = ctx.table_name().getText();
    }

    @Override
    public final void execute() {
        Database db = this.manager.currentDatabase;
        db.dropTable(this.tablename);
        result = constructSuccessResp(String.format("drop table %s success!", this.tablename));
    }
}