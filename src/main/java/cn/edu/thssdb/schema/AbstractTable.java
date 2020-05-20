package cn.edu.thssdb.schema;

import cn.edu.thssdb.type.ColumnInfo;

import javax.script.ScriptEngine;
import javax.script.ScriptEngineManager;

public abstract class AbstractTable {
    public Column[] columns;

    static ScriptEngineManager manager = new ScriptEngineManager();
    static ScriptEngine engine = manager.getEngineByName("JavaScript");

    public Comparable stringToValue(Column col, String str) throws Exception {
        // System.out.println("Value " + str);
        if (str.equals("null")) {
            if (col.notNull)
                throw new Exception(String.format("%s cannot be null", col.name));
            return null;
        }
        if (col.type == ColumnInfo.ColumnType.INT) {
            return (Integer) engine.eval(str);
        } else if (col.type == ColumnInfo.ColumnType.FLOAT) {
            return (Float) engine.eval(str);
        } else if (col.type == ColumnInfo.ColumnType.DOUBLE) {
            return (Double) engine.eval(str);
        } else if (col.type == ColumnInfo.ColumnType.LONG) {
            return (Long) engine.eval(str);
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
        throw new Exception(String.format("No column named %s", name));
    }
}
