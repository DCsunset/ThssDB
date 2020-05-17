package cn.edu.thssdb.query;

import org.antlr.v4.runtime.CommonTokenStream;
import org.antlr.v4.runtime.CharStreams;

import cn.edu.thssdb.parser.SQLLexer;
import cn.edu.thssdb.parser.SQLParser;
import cn.edu.thssdb.parser.SQLParser.ParseContext;
import cn.edu.thssdb.parser.SQLParser.Sql_stmtContext;
import cn.edu.thssdb.schema.Column;
import cn.edu.thssdb.schema.Database;
import cn.edu.thssdb.schema.Entry;
import cn.edu.thssdb.schema.Manager;
import cn.edu.thssdb.schema.Row;
import cn.edu.thssdb.type.ColumnInfo;
import cn.edu.thssdb.type.ColumnInfo.ColumnType;

public class Test {
    public static void main(String[] args) {
        // Create database
        Manager manager = new Manager();
        manager.createDatabaseIfNotExists("db1");
        manager.switchDatabase("db1");
        Database db = manager.currentDatabase;

        // Create table (commented when table exists)
        System.out.println("create table");
        Column[] columns = { new Column("id", ColumnInfo.ColumnType.STRING, true, true, 50),
                new Column("name", ColumnInfo.ColumnType.STRING, false, true, 200) };
        db.create("stu", columns);
        System.out.println("there are " + db.getTables().size() + " tables in database now");

        // Insert (id='101',name='alice') into table
        Entry[] entries = { new Entry("101", 50), new Entry("alice", 200) };
        Row data = new Row(entries);
        Entry[] entries1 = { new Entry("102", 50), new Entry("bob", 200) };
        Row data1 = new Row(entries1);
        db.getTables().get("stu").insert(data);
        db.getTables().get("stu").insert(data1);
        db.quit();

        // // Get databse info
        // HashMap<String, Table> tables = db.getTables();
        // Table tb = tables.get("stu");
        // System.out.println(tb.tableName);
        // Metadata metadata = tb.getMetadata();
        // System.out.println(metadata.getRowSize());
        // System.out.println(metadata.columns[0]);

        // String str = "show table stu;drop table stu; drop database db1;";
        String str = "delete from stu where name='alice';";
        // String str = "create TABLE person (name String(256), ID Int not null, PRIMARY
        // KEY(ID))";
        // String str = "show table stu;drop table stu; drop database db1;";

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
                if (stmtCtx.drop_db_stmt() != null) {
                    System.out.println("drop database");
                    stmt = new DropDatabaseStatement(manager, stmtCtx);
                } else {
                    System.out.println("drop table");
                    stmt = new DropTableStatement(manager, stmtCtx);
                }
            } else if (type == SQLParser.K_CREATE) {
                stmt = new CreateTableStatement(manager, stmtCtx);
            }
            // stmt.parse();
            // stmt.execute();
            // System.out.println(stmt.getResult());

        }
        // db.quit();

        // check whether stu exists
        // manager.switchDatabase("db1");
        // db = manager.currentDatabase;
        // System.out.println("there are " + db.getTables().size() + " tables in
        // database now");
    }
}