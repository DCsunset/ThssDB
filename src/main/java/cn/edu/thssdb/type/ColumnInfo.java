package cn.edu.thssdb.type;

import java.util.Map;
import java.util.HashMap;

public class ColumnInfo {
  public static enum ColumnType {
    INT, LONG, FLOAT, DOUBLE, STRING
  }

  private static Map<ColumnType, Integer> ColumnWidth = new HashMap<ColumnType, Integer>() {
    {
      put(ColumnType.INT, 4);
      put(ColumnType.LONG, 8);
      put(ColumnType.FLOAT, 4);
      put(ColumnType.DOUBLE, 8);
    }
  };

  public static int getColumnWidth(ColumnType type) {
    return ColumnWidth.get(type);
  }
};
