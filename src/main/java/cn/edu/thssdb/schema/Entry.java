package cn.edu.thssdb.schema;

import java.io.EOFException;
import java.io.Serializable;
import java.lang.reflect.Array;
import java.nio.Buffer;
import java.nio.ByteBuffer;
import java.util.Arrays;

import cn.edu.thssdb.type.ColumnInfo;

public class Entry implements Comparable<Entry>, Serializable {
  private static final long serialVersionUID = -5809782578272943999L;
  public Comparable value;
  private int maxLength = -1;
  private String type = "";

  // p[0]: type; p[1]:
  public Entry(Comparable value, Object... p) throws Exception {
    this.value = value;
    // minint/ minlong is not allowed
    if (value != null && value.getClass().getSimpleName().equals("Integer")
        && value.compareTo(Integer.MIN_VALUE) == 0) {
      throw new Exception("Input Integer.min_value is not allowed!");
    }
    if (value != null && value.getClass().getSimpleName().equals("Long") && value.compareTo(Long.MIN_VALUE) == 0) {
      throw new Exception("Input Long.min_value is not allowed!");
    }
    int l = p.length;
    if (l == 2) {
      type = p[0].toString();
      maxLength = (Integer) p[1];
    } else if (l == 1) {
      if (String.class.isInstance(p[0])) {
        type = p[0].toString();
      } else {
        maxLength = (Integer) p[0];
      }
    }
  }

  public byte[] toBytes() {
    String type = value != null ? value.getClass().getSimpleName() : this.type;
    if (type.equals("String")) {
      byte[] b = null;
      if (value == null) {
        b = new byte[this.maxLength];
        Arrays.fill(b, (byte) 0xff);
      } else {
        b = value.toString().getBytes();
      }
      return ByteBuffer.allocate(this.maxLength).put(b).array();
    } else if (type.equals("Integer")) {
      int num = value == null ? Integer.MIN_VALUE : Integer.parseInt(value.toString());
      return ByteBuffer.allocate(4).putInt(num).array();
    } else if (type.equals("Long")) {
      Long num = value == null ? Long.MIN_VALUE : Long.parseLong(value.toString());
      return ByteBuffer.allocate(8).putLong(num).array();
    } else if (type.equals("Double")) {
      Double num = value == null ? Double.NaN : Double.parseDouble(value.toString());
      return ByteBuffer.allocate(8).putDouble(num).array();
    } else {
      Float num = value == null ? Float.NaN : Float.parseFloat(value.toString());
      return ByteBuffer.allocate(4).putFloat(num).array();
    }
  }

  @Override
  public int compareTo(Entry e) {
    return value.compareTo(e.value);
  }

  @Override
  public boolean equals(Object obj) {
    if (obj == null)
      return false;
    if (this.getClass() != obj.getClass())
      return false;
    Entry e = (Entry) obj;
    return value.equals(e.value);
  }

  public String toString() {
    return value.toString();
  }

  @Override
  public int hashCode() {
    return value.hashCode();
  }
}
