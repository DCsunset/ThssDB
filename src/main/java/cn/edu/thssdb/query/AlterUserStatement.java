package cn.edu.thssdb.query;

import cn.edu.thssdb.parser.SQLParser;
import cn.edu.thssdb.parser.SQLParser.Sql_stmtContext;
import cn.edu.thssdb.schema.Manager;
import cn.edu.thssdb.server.ThssDB;

public class AlterUserStatement extends Statement {
    private String username, password;

    public AlterUserStatement(Manager manager, Sql_stmtContext parseCtx) {
        super(manager, parseCtx);
    }

    @Override
    public void parse() {
        SQLParser.Alter_user_stmtContext ctx = parseCtx.alter_user_stmt();
        username = ctx.user_name().getText();
        password = ctx.password().getText();
    }

    @Override
    public void execute() {
        if (ThssDB.getInstance().users.get(username) == null) {
            result = constructErrorResp("User not exists!");
        } else {
            ThssDB.getInstance().users.put(username, password);
            result = constructSuccessResp("Alter user success!");
        }
    }
}