package cn.edu.thssdb.query;

import cn.edu.thssdb.parser.SQLParser;
import cn.edu.thssdb.parser.SQLParser.Sql_stmtContext;
import cn.edu.thssdb.schema.Manager;
import cn.edu.thssdb.server.ThssDB;

public class DropUserStatement extends Statement {
    private String username;

    public DropUserStatement(Manager manager, Sql_stmtContext parseCtx) {
        super(manager, parseCtx);
    }

    @Override
    public void parse() {
        SQLParser.Drop_user_stmtContext ctx = parseCtx.drop_user_stmt();
        username = ctx.user_name().getText();
    }

    @Override
    public void execute() {
        if (ThssDB.getInstance().users.get(username) == null) {
            result = constructErrorResp("User not exists!");
        } else {
            ThssDB.getInstance().users.remove(username);
            result = constructSuccessResp("Drop user success!");
        }
    }
}