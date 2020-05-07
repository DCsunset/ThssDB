package cn.edu.thssdb.schema;

import cn.edu.thssdb.type.ColumnInfo;
import java.io.Serializable;

public class Column implements Comparable<Column>, Serializable {
  private String name;
  private ColumnInfo.ColumnType type;
  private boolean primary;
  private boolean notNull;
  private int maxLength;

  public Column(String name, ColumnInfo.ColumnType type, boolean primary, boolean notNull, int... maxLength) {
    this.name = name;
    this.type = type;
    this.primary = primary;
    this.notNull = notNull;
    this.maxLength = maxLength.length > 0 ? maxLength[0] : ColumnInfo.getColumnWidth(this.type);
  }

  @Override
  public int compareTo(Column e) {
    return name.compareTo(e.name);
  }

  public String toString() {
    return name + ',' + type + ',' + primary + ',' + notNull + ',' + maxLength + ',';
  }

  public int getMaxLength() {
    return this.maxLength;
  }
}
