package cn.edu.thssdb.query;

import cn.edu.thssdb.exception.CurrentDatabaseNullException;
import cn.edu.thssdb.parser.SQLParser.Show_meta_stmtContext;
import cn.edu.thssdb.parser.SQLParser.Sql_stmtContext;
import cn.edu.thssdb.rpc.thrift.ExecuteStatementResp;
import cn.edu.thssdb.rpc.thrift.Status;
import cn.edu.thssdb.schema.Column;
import cn.edu.thssdb.schema.Database;
import cn.edu.thssdb.schema.Manager;
import cn.edu.thssdb.schema.Table;
import cn.edu.thssdb.type.ColumnInfo;
import cn.edu.thssdb.utils.Global;

import java.sql.Array;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class ShowTableStatement extends Statement {
    private String tablename;

    public ShowTableStatement(Manager manager, Sql_stmtContext parseCtx) {
        super(manager, parseCtx);
    }

    @Override
    public final void parse() {
        Show_meta_stmtContext ctx = this.parseCtx.show_meta_stmt();
        this.tablename = ctx.table_name().getText().toUpperCase();
        if (manager.currentDatabase == null) {
            throw new CurrentDatabaseNullException();
        }
    }

    @Override
    public final void execute() {
        Database db = this.manager.currentDatabase;
        Table tb = db.getTables().get(this.tablename);
        Column[] cls = tb.getMetadata().columns;
        List<String> header = new ArrayList<>(Arrays.asList("name", "type", "primary", "not null", "maxLength"));
        List<List<String>> columnInfo = new ArrayList<>();
        for (int i = 0; i < cls.length; i++) {
            List<String> info = new ArrayList<>();
            info.add(cls[i].name);
            info.add(ColumnInfo.getColumnType(cls[i].type));
            info.add(cls[i].primary ? "true" : "false");
            info.add(cls[i].notNull ? "true" : "false");
            info.add(String.valueOf(cls[i].maxLength));
            columnInfo.add(info);
        }
        this.result = new ExecuteStatementResp();
        result.setStatus(new Status(Global.SUCCESS_CODE));
        result.setIsAbort(false);
        result.setHasResult(true);
        result.setColumnsList(header);
        result.setRowList(columnInfo);
        List<String> columns = new ArrayList<>();
        for (int i = 0; i < cls.length; i++) {
            columns.add(cls[i].toString());
        }

        // result = constructSuccessResp("");
        // result.setColumnsList(columns);
    }
}