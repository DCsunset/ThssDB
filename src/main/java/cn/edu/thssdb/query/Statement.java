package cn.edu.thssdb.query;

import cn.edu.thssdb.parser.SQLParser.ParseContext;
import cn.edu.thssdb.parser.SQLParser.Sql_stmtContext;
import cn.edu.thssdb.rpc.thrift.ExecuteStatementResp;
import cn.edu.thssdb.rpc.thrift.Status;
import cn.edu.thssdb.schema.Manager;
import cn.edu.thssdb.utils.Global;

public abstract class Statement {
    protected Manager manager;
    protected Sql_stmtContext parseCtx;
    protected ExecuteStatementResp result = null;

    public static ExecuteStatementResp constructErrorResp(String msg) {
        ExecuteStatementResp resp = new ExecuteStatementResp();
        Status status = new Status();

        status.setCode(Global.FAILURE_CODE);
        status.setMsg(msg);
        resp.setStatus(status);
        return resp;
    }

    public static ExecuteStatementResp constructSuccessResp(String msg) {
        ExecuteStatementResp resp = new ExecuteStatementResp();
        Status status = new Status();

        status.setCode(Global.SUCCESS_CODE);
        status.setMsg(msg);
        resp.setStatus(status);
        return resp;
    }

    public Statement(Manager manager, Sql_stmtContext parseCtx) {
        this.manager = manager;
        this.parseCtx = parseCtx;
    }

    public abstract void parse() throws Exception;

    public abstract void execute() throws Exception;

    public ExecuteStatementResp getResult() {
        return result;
    };
}