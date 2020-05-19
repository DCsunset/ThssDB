package cn.edu.thssdb.schema;

import cn.edu.thssdb.index.BPlusTree;
import cn.edu.thssdb.query.QueryTable;
import cn.edu.thssdb.type.ColumnInfo;
import cn.edu.thssdb.utils.Global.OpType;
import javafx.util.Pair;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.nio.ByteBuffer;
import java.security.cert.PKIXRevocationChecker.Option;
import java.util.ArrayList;

import cn.edu.thssdb.storage.DbCache;
import cn.edu.thssdb.storage.Metadata;
import cn.edu.thssdb.storage.Page;
import jdk.internal.org.objectweb.asm.tree.MultiANewArrayInsnNode;

import java.util.Arrays;
import java.util.Iterator;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.BitSet;
import javax.script.ScriptEngine;
import javax.script.ScriptEngineManager;
import javax.script.ScriptException;

public class Table implements Iterable<Pair<Entry, VRow>>, Serializable {
  ReentrantReadWriteLock lock;
  private String databaseName;
  public String tableName;
  public BPlusTree<Entry, VRow> index;
  private Metadata metadata;
  private DbCache cache;
  private int primaryIndex;

  public Table(String databaseName, String tableName, Column[] columns) {
    this.databaseName = databaseName;
    this.tableName = tableName;
    this.metadata = new Metadata(columns);
    this.index = new BPlusTree<>();
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
      if (metadata.columns[i].name.equals(name))
        return i;
    }
    return -1;
  }

  public Comparable stringToValue(Column col, String str) throws Exception {
    // System.out.println("Value " + str);
    if (str.equals("null")) {
      if (col.notNull)
        throw new Exception(String.format("%s cannot be null", col.name));
      return null;
    }
    if (col.type == ColumnInfo.ColumnType.INT) {
      return (Integer) engine.eval(str);
    } else if (col.type == ColumnInfo.ColumnType.FLOAT) {
      return (Float) engine.eval(str);
    } else if (col.type == ColumnInfo.ColumnType.DOUBLE) {
      return (Double) engine.eval(str);
    } else if (col.type == ColumnInfo.ColumnType.LONG) {
      return (Long) engine.eval(str);
    }
    // String
    else {
      if (str.length() < 2) {
        throw new Exception(String.format("Invalid String %s", str));
      }
      if (str.charAt(0) != '\'' || str.charAt(str.length() - 1) != '\'') {
        throw new Exception(String.format("Invalid String %s", str));
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
    } else {
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

  public Row read(VRow vrow) {
    return read(vrow.pageID, vrow.rowIndex);
  }

  public Row read(int pageID, int rowIndex) {
    Page page = cache.readPage(pageID);
    byte[] bytes = page.readRow(rowIndex);
    int pos = 0;
    Entry[] entries = new Entry[metadata.columns.length];
    for (int i = 0; i < metadata.columns.length; ++i) {
      Column col = metadata.columns[i];
      if (col.type == ColumnInfo.ColumnType.STRING) {
        entries[i] = new Entry(new String(Arrays.copyOfRange(bytes, pos, pos + col.maxLength)), col.maxLength);
        pos += col.maxLength;
      } else if (col.type == ColumnInfo.ColumnType.INT) {
        entries[i] = new Entry(ByteBuffer.wrap(Arrays.copyOfRange(bytes, pos, pos + 4)).getInt());
        pos += 4;
      } else if (col.type == ColumnInfo.ColumnType.LONG) {
        entries[i] = new Entry(ByteBuffer.wrap(Arrays.copyOfRange(bytes, pos, pos + 8)).getLong());
        pos += 8;
      } else if (col.type == ColumnInfo.ColumnType.FLOAT) {
        entries[i] = new Entry(ByteBuffer.wrap(Arrays.copyOfRange(bytes, pos, pos + 4)).getFloat());
        pos += 4;
      } else if (col.type == ColumnInfo.ColumnType.DOUBLE) {
        entries[i] = new Entry(ByteBuffer.wrap(Arrays.copyOfRange(bytes, pos, pos + 8)).getDouble());
        pos += 8;
      }
    }

    return new Row(entries);
  }

  public void insert(Row row) {
    int id = metadata.freePageList.get(0);
    Page page = cache.readPage(id);
    BitSet bitmap = page.bitmap;
    int index = bitmap.nextClearBit(0);
    byte[] bytes = row.toBytes();
    page.writeRow(index, bytes);
    if (page.isFull()) {
      metadata.freePageList.remove(0);
    }
    cache.writePage(id, page);
    // cache.writeBackPage(id); // just for file inspect in test
    this.index.put(row.entries.get(primaryIndex), new VRow(id, index));
  }

  public void delete(Entry key) {
    VRow row = this.index.get(key);
    Page page = cache.readPage(row.pageID);
    if (page.isFull()) {
      metadata.freePageList.add(row.pageID);
    }
    page.bitmap.clear(row.rowIndex);
    cache.writePage(row.pageID, page);
    this.index.remove(key);
  }

  public void update(Row row, Entry key) {
    VRow vrow = this.index.get(key);
    int pageID = vrow.pageID;
    int rowIndex = vrow.rowIndex;
    Page page = cache.readPage(pageID);
    page.writeRow(rowIndex, row.toBytes());
    cache.writePage(pageID, page);
    this.index.remove(key);
    this.index.put(row.getEntries().get(primaryIndex), vrow);
  }

  private void readObject(ObjectInputStream input) throws Exception {
    this.databaseName = input.readUTF();
    this.tableName = input.readUTF();
    this.metadata = (Metadata) input.readObject();
    this.primaryIndex = input.readInt();
    this.cache = new DbCache(this.tableName, metadata.getRowSize());
    this.index = new BPlusTree<>();
    int size = input.readInt();
    for (int i = 0; i < size; ++i) {
      Entry entry = (Entry) input.readObject();
      int pageID = input.readInt();
      int rowIndex = input.readInt();
      VRow vrow = new VRow(pageID, rowIndex);
      index.put(entry, vrow);
    }
  }

  private void writeObject(ObjectOutputStream output) {
    try {
      output.writeUTF(databaseName);
      output.writeUTF(tableName);
      output.writeObject(this.metadata);
      output.writeInt(primaryIndex);
      output.writeInt(index.size());
      Iterator<Pair<Entry, VRow>> it = index.iterator();
      while (it.hasNext()) {
        Pair<Entry, VRow> node = it.next();
        Entry entry = node.getKey();
        VRow vrow = node.getValue();
        output.writeObject(entry);
        output.writeInt(vrow.pageID);
        output.writeInt(vrow.rowIndex);
      }
    } catch (IOException e) {
    }
  }

  private class TableIterator implements Iterator<Pair<Entry, VRow>> {
    private Iterator<Pair<Entry, VRow>> iterator;

    TableIterator(Table table) {
      this.iterator = table.index.iterator();
    }

    @Override
    public boolean hasNext() {
      return iterator.hasNext();
    }

    @Override
    public Pair<Entry, VRow> next() {
      return iterator.next();
    }
  }

  @Override
  public Iterator<Pair<Entry, VRow>> iterator() {
    return new TableIterator(this);
  }
}
