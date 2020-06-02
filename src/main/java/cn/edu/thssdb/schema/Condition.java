package cn.edu.thssdb.schema;

import cn.edu.thssdb.exception.ColumnNotExistException;
import cn.edu.thssdb.schema.Table;
import cn.edu.thssdb.type.ColumnInfo;
import cn.edu.thssdb.utils.Global.OpType;

import javax.script.ScriptEngine;
import javax.script.ScriptEngineManager;

public class Condition {
    private String attrOrValue1;
    private String attrOrValue2;
    private OpType op;
    private AbstractTable table;

    public AbstractTable getTable() {
        return table;
    }

    // TODO: constructor for querytable
    public Condition(AbstractTable table, String attrOrValue1, OpType op, String attrOrValue2) {
        this.table = table;
        this.attrOrValue1 = attrOrValue1;
        this.attrOrValue2 = attrOrValue2;

        this.op = op;
    }

    public boolean satisfy(Row row) throws Exception {
        int index1 = table.findColumnByName(attrOrValue1);
        int index2 = table.findColumnByName(attrOrValue2);
        if (index1 == -1 && index2 == -1) {
            String message = String.format("%s or %s", attrOrValue1, attrOrValue2);
            throw new ColumnNotExistException(message);
        }

        Comparable value1 = null, value2 = null;
        if (index1 != -1) {
            Entry entry = row.getEntries().get(index1);
            value1 = entry.value;
        }
        else {
            Column col = table.columns[index2];
            value1 = AbstractTable.stringToValue(col, attrOrValue1);
        }

        if (index2 != -1) {
            Entry entry = row.getEntries().get(index2);
            value2 = entry.value;
        }
        else {
            Column col = table.columns[index1];
            value2 = AbstractTable.stringToValue(col, attrOrValue2);
        }


        int ret = value1.compareTo(value2);
        if (this.op == OpType.EQ) {
            return ret == 0;
        } else if (this.op == OpType.LT) {
            return ret < 0;
        } else if (this.op == OpType.GT) {
            return ret > 0;
        } else if (this.op == OpType.LE) {
            return ret <= 0;
        } else if (this.op == OpType.GE) {
            return ret >= 0;
        } else if (this.op == OpType.NE) {
            return ret != 0;
        }
        return false;
    }
}