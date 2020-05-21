package cn.edu.thssdb.type;

import java.util.Map;
import java.util.HashMap;

public class ColumnInfo {
  public static enum ColumnType {
    INT, LONG, FLOAT, DOUBLE, STRING
  }

  private static Map<ColumnType, String> ColumnString = new HashMap<ColumnType, String>() {
    {
      put(ColumnType.INT, "Integer");
      put(ColumnType.LONG, "Long");
      put(ColumnType.FLOAT, "Float");
      put(ColumnType.DOUBLE, "Double");
      put(ColumnType.STRING, "String");
    }
  };

  public static String getColumnType(ColumnType type) {
    return ColumnString.get(type);
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
