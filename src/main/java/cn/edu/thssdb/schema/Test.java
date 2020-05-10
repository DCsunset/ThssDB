package cn.edu.thssdb.schema;

import java.util.HashMap;
import cn.edu.thssdb.type.*;
import cn.edu.thssdb.storage.*;

public class Test {
    public static void main(String[] args) {
        Manager manager = new Manager();
        manager.createDatabaseIfNotExists("db1");
        manager.switchDatabase("db1");
        Database db = manager.currentDatabase;
        // Column[] columns = { new Column("height", ColumnInfo.ColumnType.INT, false,
        // true) };
        // db.create("tb1", columns);
        // db.create("tb2", columns);
        // db.quit();
        HashMap<String, Table> tables = db.getTables();
        Table tb = tables.get("tb1");
        System.out.println(tb.tableName);
        Metadata metadata = tb.getMetadata();
        System.out.println(metadata.getRowSize());
        System.out.println(metadata.columns[0]);

        Table tb2 = tables.get("tb2");
        System.out.println(tb2.tableName);
        Metadata metadata1 = tb2.getMetadata();
        System.out.println(metadata1.getRowSize());
        System.out.println(metadata1.columns[0]);
        db.quit();
    }
}