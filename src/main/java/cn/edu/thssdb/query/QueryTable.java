package cn.edu.thssdb.query;

import cn.edu.thssdb.parser.SQLParser;
import cn.edu.thssdb.schema.*;

import java.util.*;

import javafx.util.Pair;

public class QueryTable extends AbstractTable implements Iterator<Row> {
  private ArrayList<Row> data = new ArrayList<Row>();

  public QueryTable(QueryTable tb1, QueryTable tb2) {
    // combine cls
    Column[] cls1 = tb1.columns;
    Column[] cls2 = tb2.columns;
    this.columns = new Column[cls1.length + cls2.length];
    System.arraycopy(cls1, 0, this.columns, 0, cls1.length);
    System.arraycopy(cls2, 0, this.columns, cls1.length, cls2.length);
  }

  private QueryTable() {

  }

  public void insertRow(Row row) {
    this.data.add(row);
  }

  public QueryTable project(String[] columnNames) {
    QueryTable result = new QueryTable();
    result.columns = new Column[columnNames.length];
    int[] indices = new int[columnNames.length];
    // columns
    for (int i = 0; i < columnNames.length; i++) {
      for (int j = 0; j < this.columns.length; j++) {
        if (columnNames[i].equals(columns[j].name)) {
          indices[i] = j;
          result.columns[i] = this.columns[j];
          break;
        }
      }
    }
    // data
    for (Row row : data) {
      Row rowslice = new Row();
      for (int index : indices) {
        rowslice.appendEntry(row.getEntries().get(index));
      }
      result.data.add(rowslice);
    }
    return result;
  }

  public void output() {
    StringJoiner sj = new StringJoiner("\t");
    for (Column c : columns) {
      sj.add(c.name);
    }
    System.out.println(sj.toString());

    for (Row row : data) {
      System.out.println(row.toString());
    }
  }

  public List<String> getColumns() {
    List<String> result = new ArrayList<>();
    for (Column c : columns) {
      result.add(c.name);
    }
    return result;
  }

  public List<List<String>> getRows() {
    List<List<String>> result = new ArrayList<>();
    for (Row row : data) {
        result.add(row.toStringList());
    }
    return result;
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
    this.columns = new Column[columns.length];
    System.arraycopy(columns, 0, this.columns, 0, columns.length);
    if (joinable) {
      for (int i = 0; i < columns.length; ++i) {
        columns[i].name = table.tableName + "." + columns[i].name;
      }
    }
  }

  public void filter(MultipleCondition condition) throws Exception {
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
  public QueryTable join(QueryTable another) {
    QueryTable me = this;
    QueryTable result = new QueryTable(me, another);
    for (Row row : me.data) {
      for (Row row1 : another.data) {
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