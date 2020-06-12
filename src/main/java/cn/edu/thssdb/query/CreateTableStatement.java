package cn.edu.thssdb.query;

import cn.edu.thssdb.exception.CurrentDatabaseNullException;
import cn.edu.thssdb.parser.SQLParser;
import cn.edu.thssdb.parser.SQLParser.Show_meta_stmtContext;
import cn.edu.thssdb.parser.SQLParser.Sql_stmtContext;
import cn.edu.thssdb.rpc.thrift.ExecuteStatementResp;
import cn.edu.thssdb.schema.Column;
import cn.edu.thssdb.schema.Database;
import cn.edu.thssdb.schema.Manager;
import cn.edu.thssdb.schema.Table;
import cn.edu.thssdb.transaction.Transaction;
import cn.edu.thssdb.type.ColumnInfo;

import java.util.List;

public class CreateTableStatement extends Statement {
    private SQLParser.Create_table_stmtContext ctx;
    private Transaction transaction;

    public CreateTableStatement(Manager manager, Sql_stmtContext parseCtx, Transaction transaction) {
        super(manager, parseCtx);
        this.transaction = transaction;
    }

    @Override
    public final void parse() {
        ctx = this.parseCtx.create_table_stmt();
    }

    @Override
    public final void execute() {
        Database db = this.manager.currentDatabase;
        if (db == null) {
            throw new CurrentDatabaseNullException();
        }

        String tableName = ctx.table_name().getText().toUpperCase();
        if (db.getTables().containsKey(tableName)) {
            result = constructErrorResp("Table already exists");
            return;
        }
        /*
         * else { SQLParser.Column_defContext def = ctx.column_def(1); result =
         * String.format("%s %s %s", def.column_name().getText(),
         * def.type_name().getText(), def.column_constraint(0).getText() ); return; }
         */

        SQLParser.Table_constraintContext tbCst = ctx.table_constraint();
        String primaryKey = "";
        if (tbCst != null && tbCst.column_name().size() > 0)
            primaryKey = tbCst.column_name(0).getText().toUpperCase();

        Column columns[] = new Column[ctx.column_def().size()];
        for (int i = 0; i < ctx.column_def().size(); ++i) {
            SQLParser.Column_defContext def = ctx.column_def(i);
            String type = def.type_name().getText().toUpperCase();
            String name = def.column_name().getText().toUpperCase();
            boolean notnull = false;
            boolean primary = primaryKey.equals(name);
            for (int j = 0; j < def.column_constraint().size(); ++j) {
                String cst = def.column_constraint(j).getText().toUpperCase();
                if (cst.equals("NOTNULL")) {
                    notnull = true;
                } else if (cst.equals("PRIMARYKEY")) {
                    primary = true;
                }
            }
            ColumnInfo.ColumnType colType = null;
            int maxLength = 0;
            if (type.toUpperCase().equals("INT")) {
                colType = ColumnInfo.ColumnType.INT;
            } else if (type.equals("LONG")) {
                colType = ColumnInfo.ColumnType.LONG;
            } else if (type.equals("FLOAT")) {
                colType = ColumnInfo.ColumnType.FLOAT;
            } else if (type.equals("DOUBLE")) {
                colType = ColumnInfo.ColumnType.DOUBLE;
            } else if (type.matches("^STRING\\(\\d+\\)$")) {
                colType = ColumnInfo.ColumnType.STRING;
                maxLength = Integer.parseInt(type.substring(7, type.length() - 1));
            }
            if (maxLength == 0) {
                columns[i] = new Column(name, colType, primary, notnull);
            } else {
                columns[i] = new Column(name, colType, primary, notnull, maxLength);
            }
        }

        db.create(transaction.uuid, tableName, columns);

        Table tb = db.getTables().get(ctx.table_name().getText().toUpperCase());

        Column[] cls = tb.getMetadata().columns;
        this.result = constructSuccessResp(String.format("Table %s created successfully\n", tb.tableName));
        /*
         * for (int i = 0; i < cls.length; i++) { this.result += cls[i].toString() +
         * '\n'; }
         */
    }
}