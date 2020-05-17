package cn.edu.thssdb.query;

import org.antlr.v4.runtime.CommonTokenStream;

import java.util.HashMap;

import org.antlr.v4.runtime.CharStreams;

import cn.edu.thssdb.parser.SQLLexer;
import cn.edu.thssdb.parser.SQLParser;
import cn.edu.thssdb.parser.SQLParser.ParseContext;
import cn.edu.thssdb.parser.SQLParser.Sql_stmtContext;
import cn.edu.thssdb.schema.Column;
import cn.edu.thssdb.schema.Database;
import cn.edu.thssdb.schema.Manager;
import cn.edu.thssdb.schema.Table;
import cn.edu.thssdb.storage.Metadata;
import cn.edu.thssdb.type.ColumnInfo;

public class Test {
    public static void main(String[] args) {
        // Create database
        Manager manager = new Manager();
        manager.createDatabaseIfNotExists("db1");
        manager.switchDatabase("db1");
        Database db = manager.currentDatabase;

        // Create table (commented when restart)
        System.out.println("create table");
        Column[] columns = { new Column("id", ColumnInfo.ColumnType.STRING, true, true, 50),
                new Column("name", ColumnInfo.ColumnType.STRING, false, true, 200) };
        db.create("stu", columns);
        System.out.println("there are " + db.getTables().size() + " tables in database now");

        // // Get databse info
        // HashMap<String, Table> tables = db.getTables();
        // Table tb = tables.get("stu");
        // System.out.println(tb.tableName);
        // Metadata metadata = tb.getMetadata();
        // System.out.println(metadata.getRowSize());
        // System.out.println(metadata.columns[0]);

        String str = "show table stu;drop table stu";

        SQLLexer lexer = new SQLLexer(CharStreams.fromString(str));
        CommonTokenStream tokens = new CommonTokenStream(lexer);
        SQLParser parser = new SQLParser(tokens);
        ParseContext root = parser.parse();
        for (int i = 0; i < root.sql_stmt_list().sql_stmt().size(); i++) {
            Sql_stmtContext stmtCtx = root.sql_stmt_list().sql_stmt().get(i);
            Statement stmt = null;
            int type = stmtCtx.getStart().getType();
            if (type == SQLParser.K_SHOW) {
                stmt = new ShowTableStatement(manager, stmtCtx);
            } else if (type == SQLParser.K_DROP) {
                stmt = new DropTableStatement(manager, stmtCtx);
            }
            stmt.parse();
            stmt.execute();
            System.out.println(stmt.getResult());

        }
        db.quit();

        // check whether stu exists
        manager.switchDatabase("db1");
        db = manager.currentDatabase;
        System.out.println("there are " + db.getTables().size() + " tables in database now");
    }
}