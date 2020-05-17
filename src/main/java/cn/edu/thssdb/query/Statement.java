package cn.edu.thssdb.query;

import cn.edu.thssdb.parser.SQLParser.ParseContext;
import cn.edu.thssdb.schema.Manager;

public class Statement {
    private Manager manager;
    private ParseContext parseCtx;

    public Statement(Manager manager, ParseContext parseCtx) {
        this.manager = manager;
        this.parseCtx = parseCtx;
    }

    public void parse() {
    }

    public void execute() {
    }

    public String getResult() { // string displayed at client's console
        return "";
    }
}