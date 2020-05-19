package cn.edu.thssdb.query;

import cn.edu.thssdb.schema.Column;
import cn.edu.thssdb.schema.Condition;
import cn.edu.thssdb.schema.Entry;
import cn.edu.thssdb.schema.Row;
import cn.edu.thssdb.schema.Table;
import cn.edu.thssdb.schema.VRow;

import java.util.ArrayList;
import java.util.Iterator;
import javafx.util.Pair;

public class QueryTable implements Iterator<Row> {
  private Table table; // not necessary
  private ArrayList<Row> data = new ArrayList<Row>();
  private Column[] cls;

  public Column[] getCls() {
    return cls;
  }

  public QueryTable(QueryTable tb1, QueryTable tb2) {
    // combine cls
    Column[] cls1 = tb1.getCls();
    Column[] cls2 = tb2.getCls();
    this.cls = new Column[cls1.length + cls2.length];
    System.arraycopy(cls1, 0, this.cls, 0, cls1.length);
    System.arraycopy(cls2, 0, this.cls, cls1.length, cls2.length);
  }

  public void insertRow(Row row) {
    this.data.add(row);
  }

  public static Row combineRow(Row row1, Row row2) {
    Row result = new Row();
    result.appendEntries(row1.getEntries());
    result.appendEntries(row2.getEntries());
    return result;
  }

  public QueryTable(Table table) { // leaf node
    this.table = table;
    // copy data from table
    Iterator<Pair<Entry, VRow>> it = this.table.iterator();
    while (it.hasNext()) {
      Pair<Entry, VRow> item = it.next();
      Row row = this.table.read(item.getValue());
      data.add(row);
    }
    cls = table.getMetadata().columns;
  }

  public int findColumnByName(String name) {
    for (int i = 0; i < this.cls.length; i++) {
      if (this.cls[i].name.equals(name)) {
        return i;
      }
    }
    return -1;
  }

  public Table getTable() {
    return table;
  }

  // another must be constructed by a table
  public QueryTable join(QueryTable another, String attr1, String attr2, Condition condition) {
    QueryTable me = this;
    if (condition != null && condition.getTable() == another.getTable()) { // condition is for the caller
      QueryTable temp = me;
      me = another;
      another = temp;
    }
    QueryTable result = new QueryTable(me, another);
    for (Row row : me.data) {
      if (condition == null || condition.satisfy(row)) {
        for (Row row1 : another.data) {
          Entry entry = row.getEntries().get(me.findColumnByName(attr1));
          Entry entry1 = row1.getEntries().get(another.findColumnByName(attr2));
          if (entry.equals(entry1)) {
            result.insertRow(combineRow(row, row1));
          }
        }
      }
    }
    return result;
  }

  @Override
  public boolean hasNext() {
    // TODO
    return true;
  }

  @Override
  public Row next() {
    // TODO
    return null;
  }
}