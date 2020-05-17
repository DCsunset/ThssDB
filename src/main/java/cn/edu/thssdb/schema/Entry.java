package cn.edu.thssdb.schema;

import java.io.Serializable;
import java.nio.Buffer;
import java.nio.ByteBuffer;

import cn.edu.thssdb.type.ColumnInfo;

public class Entry implements Comparable<Entry>, Serializable {
  private static final long serialVersionUID = -5809782578272943999L;
  public Comparable value;
  private int maxLength = -1;

  public Entry(Comparable value, int... maxLength) {
    System.out.println(value.getClass().getSimpleName());
    this.value = value;
    if (maxLength.length > 0) {
      this.maxLength = maxLength[0];
    }
  }

  public byte[] toBytes() {
    String type = value.getClass().getSimpleName();
    if (type == "String") {
      byte[] b = value.toString().getBytes();
      return ByteBuffer.allocate(this.maxLength).put(b).array();
    } else if (type == "Integer") {
      int num = Integer.parseInt(value.toString());
      return ByteBuffer.allocate(4).putInt(num).array();
    } else if (type == "Long") {
      Long num = Long.parseLong(value.toString());
      return ByteBuffer.allocate(8).putLong(num).array();
    } else if (type == "Double") {
      Double num = Double.parseDouble(value.toString());
      return ByteBuffer.allocate(8).putDouble(num).array();
    } else {
      Float num = Float.parseFloat(value.toString());
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
