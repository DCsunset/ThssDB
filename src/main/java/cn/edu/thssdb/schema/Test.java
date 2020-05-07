package cn.edu.thssdb.schema;

import java.util.HashMap;
import cn.edu.thssdb.type.*;

public class Test {
    public static void main(String[] args) {
        Database db = new Database("test");
        Column[] columns = { new Column("name", ColumnInfo.ColumnType.STRING, false, true, 100) };
        db.create("tb1", columns);
        HashMap<String, Table> tables = db.getTables();
        Table tb = tables.get("tb1");
        System.out.println(tb.tableName);
    }
}