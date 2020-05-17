package cn.edu.thssdb.query;

import cn.edu.thssdb.parser.SQLParser.ParseContext;
import cn.edu.thssdb.schema.Manager;

public class ShowTableStatement extends Statement {
    public ShowTableStatement(Manager manager, ParseContext parseCtx) {
        super(manager, parseCtx);
        System.out.println("dbinfo:" + manager.currentDatabase.name + " " + manager.currentDatabase.getTables());
    }

    @Override
    public final void parse() {

    }

    @Override
    public final void execute() {

    }

    @Override
    public final String getResult() {
        return "show table";
    }
}