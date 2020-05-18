package cn.edu.thssdb.schema;

import cn.edu.thssdb.index.BPlusTree;
import cn.edu.thssdb.type.ColumnInfo;
import javafx.util.Pair;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.ArrayList;

import cn.edu.thssdb.storage.DbCache;
import cn.edu.thssdb.storage.Metadata;
import cn.edu.thssdb.storage.Page;
import jdk.internal.org.objectweb.asm.tree.MultiANewArrayInsnNode;

import java.util.Arrays;
import java.util.Iterator;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.BitSet;
import javax.script.ScriptEngine;
import javax.script.ScriptEngineManager;
import javax.script.ScriptException;

public class Table implements Iterable<Row>, Serializable {
  ReentrantReadWriteLock lock;
  private String databaseName;
  public String tableName;
  public BPlusTree<Entry, Row> index;
  private Metadata metadata;
  private DbCache cache;
  private int primaryIndex;

  public Table(String databaseName, String tableName, Column[] columns) {
    this.databaseName = databaseName;
    this.tableName = tableName;
    this.metadata = new Metadata(columns);
    try {
      this.cache = new DbCache(Manager.baseDir + "/" + databaseName + "/" + tableName, metadata.getRowSize());
    } catch (IOException e) {
      System.out.println("create cache error");
    }
    for (int i = 0; i < columns.length; i++) {
      if (columns[i].isPrimary()) {
        primaryIndex = i;
      }
    }
  }

  public Metadata getMetadata() {
    return metadata;
  }

  public DbCache getCache() {
    return cache;
  }

  private void recover() {
    // TODO
  }

  static ScriptEngineManager manager = new ScriptEngineManager();
  static ScriptEngine engine = manager.getEngineByName("JavaScript");

  public int findColumnByName(String name) {
    for (int i = 0; i < metadata.columns.length; ++i) {
      if (metadata.columns[i].name == name)
        return i;
    }
    return -1;
  }

  public Comparable stringToValue(Column col, String str) throws Exception {
    //System.out.println("Value " + str);
    if (str.equals("null")) {
      if (col.notNull)
        throw new Exception(String.format("%s cannot be null", col.name));
      return null;
    }
    if (col.type == ColumnInfo.ColumnType.INT) {
      return (Integer) engine.eval(str);
    }
    else if (col.type == ColumnInfo.ColumnType.FLOAT) {
      return (Float) engine.eval(str);
    }
    else if (col.type == ColumnInfo.ColumnType.DOUBLE) {
      return (Double) engine.eval(str);
    }
    else if (col.type == ColumnInfo.ColumnType.LONG) {
      return (Long) engine.eval(str);
    }
    // String
    else {
      if (str.length() < 2) {
        throw new Exception("");
      }
      if (str.charAt(0) != '\'' || str.charAt(str.length() - 1) != '\'') {
        throw new Exception("");
      }
      return str.substring(1, str.length() - 1);
    }
  }

  public Row createRow(String[] names, String[] values) throws Exception {
    // All cols
    Entry entries[] = new Entry[values.length];
    Arrays.fill(entries, null);
    if (names.length == 0) {
      if (values.length != metadata.columns.length)
        throw new Exception("Wrong number of values");
      for (int i = 0; i < values.length; ++i) {
        Column col = metadata.columns[i];
        entries[i] = new Entry(stringToValue(col, values[i]), col.maxLength);
      }
    }
    else {
      if (values.length != names.length)
        throw new Exception("Wrong number of values");
      for (int i = 0; i < names.length; ++i) {
        int index = findColumnByName(names[i]);
        if (index == -1)
          throw new Exception(String.format("Column %s does not exist", names[i]));
        Column col = metadata.columns[index];
        entries[i] = new Entry(stringToValue(col, values[i]), col.maxLength);
      }
    }
    return new Row(entries);
  }

  public void insert(Row row) {
    int id = metadata.freePageList.get(0);
    Page page = cache.readPage(id);
    BitSet bitmap = page.bitmap;
    int index = bitmap.nextClearBit(0);
    page.writeRow(index, row.toBytes());
    if (page.isFull()) {
      metadata.freePageList.remove(0);
    }
    cache.writePage(id, page);
    // cache.writeBackPage(id);   // just for file inspect in test
  }

  public void delete(int pageId, int rowIndex) {
    Page page = cache.readPage(pageId);
    if (page.isFull()) {
      metadata.freePageList.add(pageId);
    }
    page.bitmap.clear(rowIndex);
    cache.writePage(pageId, page);
  }

  public void update(Row row, int pageID, int rowIndex) {
    Page page = cache.readPage(pageID);
    page.writeRow(rowIndex, row.toBytes());
    cache.writePage(pageID, page);
  }

  private void readObject(ObjectInputStream input) throws Exception {
    this.databaseName = input.readUTF();
    this.tableName = input.readUTF();
    this.metadata = (Metadata) input.readObject();
    this.primaryIndex = input.readInt();
    this.cache = new DbCache(this.tableName, metadata.getRowSize());
  }

  private void writeObject(ObjectOutputStream output) {
    try {
      output.writeUTF(databaseName);
      output.writeUTF(tableName);
      output.writeObject(this.metadata);
      output.writeInt(primaryIndex);

    } catch (IOException e) {
    }
  }

  private class TableIterator implements Iterator<Row> {
    private Iterator<Pair<Entry, Row>> iterator;

    TableIterator(Table table) {
      this.iterator = table.index.iterator();
    }

    @Override
    public boolean hasNext() {
      return iterator.hasNext();
    }

    @Override
    public Row next() {
      return iterator.next().getValue();
    }
  }

  @Override
  public Iterator<Row> iterator() {
    return new TableIterator(this);
  }
}
