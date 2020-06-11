package cn.edu.thssdb.schema;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.StringJoiner;

public class Row implements Serializable {
  private static final long serialVersionUID = -5809782578272943999L;
  protected ArrayList<Entry> entries;

  public Row() {
    this.entries = new ArrayList<>();
  }

  public Row(Entry[] entries) {
    this.entries = new ArrayList<>(Arrays.asList(entries));
  }

  public ArrayList<Entry> getEntries() {
    return entries;
  }

  public void appendEntries(ArrayList<Entry> entries) {
    this.entries.addAll(entries);
  }

  public void appendEntry(Entry entry) {
    this.entries.add(entry);
  }

  public String toString() {
    if (entries == null)
      return "EMPTY";
    StringJoiner sj = new StringJoiner("\t");
    for (Entry e : entries)
      sj.add(e.toString());
    return sj.toString();
  }

  public List<String> toStringList() {
    List<String> result = new ArrayList<>();
    for (Entry e : entries)
      result.add(e.toString());
    return result;
  }

  public byte[] toBytes() {
    ByteArrayOutputStream os = new ByteArrayOutputStream();
    for (Entry entry : entries) {
      byte[] b = entry.toBytes();
      try {
        os.write(b);
      } catch (IOException e) {
      }
    }
    return os.toByteArray();
  }
}
