package cn.edu.thssdb.schema;

import cn.edu.thssdb.type.ColumnInfo;

import javax.script.ScriptEngine;
import javax.script.ScriptEngineManager;

public abstract class AbstractTable {
    public Column[] columns;

    static ScriptEngineManager manager = new ScriptEngineManager();
    static ScriptEngine engine = manager.getEngineByName("JavaScript");

    static public Comparable stringToValue(Column col, String str) throws Exception {
        // System.out.println("Value " + str);
        if (str.equals("null")) {
            if (col.notNull)
                throw new Exception(String.format("%s cannot be null", col.name));
            return null;
        }
        if (col.type == ColumnInfo.ColumnType.INT) {
            return ((Number) engine.eval(str)).intValue();
        } else if (col.type == ColumnInfo.ColumnType.FLOAT) {
            return ((Number) engine.eval(str)).floatValue();
        } else if (col.type == ColumnInfo.ColumnType.DOUBLE) {
            return ((Number) engine.eval(str)).doubleValue();
        } else if (col.type == ColumnInfo.ColumnType.LONG) {
            return ((Number) engine.eval(str)).longValue();
        }
        // String
        else {
            if (str.length() < 2) {
                throw new Exception(String.format("Invalid String %s", str));
            }
            if (str.charAt(0) != '\'' || str.charAt(str.length() - 1) != '\'') {
                throw new Exception(String.format("Invalid String %s", str));
            }
            return str.substring(1, str.length() - 1);
        }

    }

    public int findColumnByName(String name) throws Exception {
        for (int i = 0; i < this.columns.length; i++) {
            if (this.columns[i].name.equals(name)) {
                return i;
            }
        }
        return -1;
    }
}
