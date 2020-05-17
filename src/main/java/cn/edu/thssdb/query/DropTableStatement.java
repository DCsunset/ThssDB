
package cn.edu.thssdb.query;

import cn.edu.thssdb.parser.SQLParser.Drop_table_stmtContext;
import cn.edu.thssdb.parser.SQLParser.Sql_stmtContext;
import cn.edu.thssdb.schema.Database;
import cn.edu.thssdb.schema.Manager;

public class DropTableStatement extends Statement {
    private String tablename;
    private boolean success;

    public DropTableStatement(Manager manager, Sql_stmtContext ctx) {
        super(manager, ctx);
    }

    @Override
    public final void parse() {
        Drop_table_stmtContext ctx = this.parseCtx.drop_table_stmt();
        this.tablename = ctx.table_name().getText();
    }

    @Override
    public final void execute() {
        Database db = this.manager.currentDatabase;
        try {
            db.dropTable(this.tablename);
            success = true;
        } catch (Exception e) {
            success = false;
        }
    }

    @Override
    public final String getResult() {
        if (success) {
            return String.format("drop table %s success!", this.tablename);
        } else {
            return String.format("drop table %s fail!", this.tablename);
        }
    }
}