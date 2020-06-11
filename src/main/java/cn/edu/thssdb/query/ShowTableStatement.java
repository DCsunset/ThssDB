package cn.edu.thssdb.query;

import cn.edu.thssdb.parser.SQLParser.Show_meta_stmtContext;
import cn.edu.thssdb.parser.SQLParser.Sql_stmtContext;
import cn.edu.thssdb.schema.Column;
import cn.edu.thssdb.schema.Database;
import cn.edu.thssdb.schema.Manager;
import cn.edu.thssdb.schema.Table;

import java.util.ArrayList;
import java.util.List;

public class ShowTableStatement extends Statement {
    private String tablename;

    public ShowTableStatement(Manager manager, Sql_stmtContext parseCtx) {
        super(manager, parseCtx);
    }

    @Override
    public final void parse() {
        Show_meta_stmtContext ctx = this.parseCtx.show_meta_stmt();
        this.tablename = ctx.table_name().getText();
    }

    @Override
    public final void execute() {
        Database db = this.manager.currentDatabase;
        Table tb = db.getTables().get(this.tablename);
        Column[] cls = tb.getMetadata().columns;
        /*
        this.result = String.join("\t", "name", "type", "primary", "not null", "maxlength") + "\n";
        for (int i = 0; i < cls.length; i++) {
            this.result += cls[i].toString() + '\n';
        }
         */
        List<String> columns = new ArrayList<>();
        for (int i = 0; i < cls.length; i++) {
            columns.add(cls[i].toString());
        }

        result = constructSuccessResp("");
        result.setColumnsList(columns);
    }
}