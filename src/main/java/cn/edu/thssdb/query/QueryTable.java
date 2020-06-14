package cn.edu.thssdb.query;

import cn.edu.thssdb.parser.SQLParser;
import cn.edu.thssdb.schema.*;

import java.util.*;

import javafx.scene.control.Tab;
import javafx.util.Pair;

public class QueryTable extends AbstractTable implements Iterator<Row> {
  private ArrayList<Row> data = new ArrayList<Row>();

  public QueryTable(List<Table> tables) {
    // combine cls
    int length = 0;
    for (int i = 0; i < tables.size(); ++i) {
        length += tables.get(i).columns.length;
    }
    this.columns = new Column[length];

    int current = 0;
    for (int i = 0; i < tables.size(); ++i) {
      Column[] columns = tables.get(i).getMetadata().columns;
      for (int j = 0; j < columns.length; ++j) {
        this.columns[current + j] = new Column(columns[i]);
        if (tables.size() > 1)
          this.columns[current + j].name = tables.get(i).tableName + "." + columns[j].name;
        else
          this.columns[current + j].name = columns[j].name;
        this.columns[current + j].type = columns[j].type;
        this.columns[current + j].maxLength = columns[j].maxLength;
        this.columns[current + j].notNull = columns[j].notNull;
        this.columns[current + j].primary = columns[j].primary;
      }
      current += columns.length;
    }
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

  public static QueryTable join(List<Table> tables, SQLParser.Multiple_conditionContext onConditionCtx, SQLParser.Multiple_conditionContext conditionCtx) throws Exception {
    QueryTable result = new QueryTable(tables);

    MultipleCondition onCondition = null, condition = null;
    if (onConditionCtx != null)
      onCondition = new MultipleCondition(result, onConditionCtx);
    if (conditionCtx != null)
      condition = new MultipleCondition(result, conditionCtx);

    // Cartesian product
    ArrayList<Iterator<Pair<Entry, VRow>>> itArray = new ArrayList<>();
    ArrayList<Row> currentValue = new ArrayList<>();
    for (Table t : tables) {
      Iterator<Pair<Entry, VRow>> it = t.iterator();
      // empty
      if (!it.hasNext())
          return result;
      itArray.add(it);
      Pair<Entry, VRow> item = it.next();
      Row row = t.read(item.getValue());
      currentValue.add(row);
    }

    boolean done = false;
    while (!done) {
      Row row = new Row();
      for (int i = 0; i < tables.size(); ++i) {
          row = combineRow(row, currentValue.get(i));
      }
      if ((onCondition == null || onCondition.satisfy(row))
        && (condition == null || condition.satisfy(row)))
        result.insertRow(row);

      // Increment index
      int currentIndex = 0;
      while (true) {
          Iterator<Pair<Entry, VRow>> it = itArray.get(currentIndex);
          if (it.hasNext()) {
            Pair<Entry, VRow> item = it.next();
            Row trow = tables.get(currentIndex).read(item.getValue());
            currentValue.set(currentIndex, trow);
            break;
          }

          it = tables.get(currentIndex).iterator();
          Pair<Entry, VRow> item = it.next();
          Row trow = tables.get(currentIndex).read(item.getValue());
          currentValue.set(currentIndex, trow);
          itArray.set(currentIndex, it);
          ++currentIndex;

          if (currentIndex == tables.size()) {
              done = true;
              break;
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