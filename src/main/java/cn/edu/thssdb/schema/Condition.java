package cn.edu.thssdb.schema;

import cn.edu.thssdb.schema.Table;
import cn.edu.thssdb.utils.Global.OpType;

public class Condition {
    private String attr;
    private Comparable value;
    private OpType op;
    private Table table;
    private Table queryTable;

    public String getAttr() {
        return attr;
    }

    public Table getTable() {
        return table;
    }

    // TODO: constructor for querytable
    public Condition(Table table, String attr, OpType op, String value) {
        this.table = table;
        this.attr = attr;
        int idx = table.findColumnByName(attr);
        try {
            this.value = table.stringToValue(table.getMetadata().columns[idx], value);
        } catch (Exception e) {
        }
        this.op = op;
    }

    public boolean satisfy(Row row) {
        int colIdx = table.findColumnByName(attr);
        Entry entry = row.getEntries().get(colIdx);
        int ret = entry.value.compareTo(this.value);
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