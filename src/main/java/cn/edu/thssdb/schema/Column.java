package cn.edu.thssdb.schema;

import cn.edu.thssdb.type.ColumnInfo;
import java.io.Serializable;

public class Column implements Comparable<Column>, Serializable {
  public String name;
  public ColumnInfo.ColumnType type;
  public boolean primary;
  public boolean notNull;
  public int maxLength;

  public Column(Column col) {
      this(col.name, col.type, col.primary, col.notNull,  col.maxLength);
  }

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
    return name + '\t' + type + '\t' + (primary ? "Primary" : "") + '\t' + (notNull ? "not Null" : "") + '\t'
        + maxLength;
  }

  public int getMaxLength() {
    return this.maxLength;
  }

  public boolean isPrimary() {
    return primary;
  }
}
