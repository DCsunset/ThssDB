package cn.edu.thssdb.query;

import cn.edu.thssdb.schema.Column;
import cn.edu.thssdb.schema.Condition;
import cn.edu.thssdb.schema.Entry;
import cn.edu.thssdb.schema.Row;
import cn.edu.thssdb.schema.Table;
import cn.edu.thssdb.schema.VRow;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import javafx.util.Pair;

public class QueryTable implements Iterator<Row> {
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

  public QueryTable(Table table, boolean joinable) { // leaf node
    // copy data from table
    Iterator<Pair<Entry, VRow>> it = table.iterator();
    while (it.hasNext()) {
      Pair<Entry, VRow> item = it.next();
      Row row = table.read(item.getValue());
      data.add(row);
    }

    Column[] columns = table.getMetadata().columns;
    this.cls = new Column[columns.length];
    System.arraycopy(columns, 0, this.cls, 0, columns.length);
    if (joinable) {
      for (int i = 0; i < cls.length; ++i) {
        cls[i].name = table.tableName + "." + cls[i].name;
      }
    }
  }

  public int findColumnByName(String name) {
    for (int i = 0; i < this.cls.length; i++) {
      if (this.cls[i].name.equals(name)) {
        return i;
      }
    }
    return -1;
  }

  public void filter(Condition condition) {
    if (condition == null)
      return;
    for (int i = 0; i < data.size(); ++i) {
      if (!condition.satisfy(data.get(i))) {
        data.remove(i);
        --i;
      }
    }
  }

  // another must be constructed by a table
  public QueryTable join(QueryTable another, String attr1, String attr2) {
    QueryTable me = this;
    QueryTable result = new QueryTable(me, another);
    for (Row row : me.data) {
      for (Row row1 : another.data) {
        if (attr1 != null) {
          int index1 = me.findColumnByName(attr1);
          int index2 = another.findColumnByName(attr2);
          if (index1 != -1 && index2 != -1) {
            Entry entry = row.getEntries().get(index1);
            Entry entry1 = row1.getEntries().get(index2);
            if (entry.equals(entry1)) {
              result.insertRow(combineRow(row, row1));
            }
          }
          else {
            result.insertRow(combineRow(row, row1));
          }
        }
        else
          result.insertRow(combineRow(row, row1));
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