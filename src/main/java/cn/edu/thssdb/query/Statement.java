package cn.edu.thssdb.query;

import cn.edu.thssdb.parser.SQLParser.ParseContext;
import cn.edu.thssdb.parser.SQLParser.Sql_stmtContext;
import cn.edu.thssdb.schema.Manager;

public abstract class Statement {
    protected Manager manager;
    protected Sql_stmtContext parseCtx;

    public Statement(Manager manager, Sql_stmtContext parseCtx) {
        this.manager = manager;
        this.parseCtx = parseCtx;
    }

    public abstract void parse() throws Exception;

    public abstract void execute() throws Exception;

    public String getResult() { // string displayed at client's console
        return "";
    }
}