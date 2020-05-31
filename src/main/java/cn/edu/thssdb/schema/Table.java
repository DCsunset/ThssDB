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
import java.security.spec.ECGenParameterSpec;
import java.util.ArrayList;

import cn.edu.thssdb.storage.DbCache;
import cn.edu.thssdb.storage.Metadata;
import cn.edu.thssdb.storage.Page;
import cn.edu.thssdb.transaction.DeleteLog;
import cn.edu.thssdb.transaction.InsertLog;
import cn.edu.thssdb.transaction.Log;
import cn.edu.thssdb.transaction.Transaction;
import cn.edu.thssdb.transaction.UpdateLog;
import cn.edu.thssdb.transaction.Log.LogType;

import java.util.Arrays;
import java.util.Iterator;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.BitSet;
import java.util.Dictionary;
import java.util.Hashtable;

import javax.script.ScriptEngine;
import javax.script.ScriptEngineManager;
import javax.script.ScriptException;

public class Table extends AbstractTable implements Iterable<Pair<Entry, VRow>>, Serializable {
  public ReentrantLock lock;
  private String databaseName;
  public String tableName;
  public BPlusTree<Entry, VRow> index;
  private Metadata metadata;
  private DbCache cache;
  private int primaryIndex;

  public Table(String databaseName, String tableName, Column[] columns) {
    lock = new ReentrantLock();
    this.columns = columns;
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

  public Row createRow(String[] names, String[] values) throws Exception {
    // All cols
    Entry entries[] = new Entry[columns.length];
    Arrays.fill(entries, null);
    if (names.length == 0) {
      if (values.length != columns.length)
        throw new Exception("Wrong number of values");
      for (int i = 0; i < values.length; ++i) {
        Column col = columns[i];
        entries[i] = new Entry(stringToValue(col, values[i]), col.maxLength);
      }
    } else {
      if (values.length != names.length)
        throw new Exception("Wrong number of values");

      boolean inserted[] = new boolean[columns.length];
      Arrays.fill(inserted, false);
      for (int i = 0; i < names.length; ++i) {
        int index = findColumnByName(names[i]);
        if (index == -1)
          throw new Exception(String.format("Column %s does not exist", names[i]));
        inserted[index] = true;
        Column col = metadata.columns[index];
        entries[index] = new Entry(stringToValue(col, values[i]), col.maxLength);
      }

      for (int i = 0; i < columns.length; ++i) {
        if (!inserted[i]) {
          if (columns[i].isPrimary())
            throw new Exception(String.format("Primary key %s can't be null", columns[i].name));
          Column col = metadata.columns[i];
          String type = ColumnInfo.getColumnType(col.type);
          if (type.equals("String")) {
            entries[i] = new Entry(null, type, columns[i].maxLength);
          } else {
            entries[i] = new Entry(null, type);
          }
        }
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
      try {
        Column col = metadata.columns[i];
        if (col.type == ColumnInfo.ColumnType.STRING) {
          for (int j = 0; j <= col.maxLength; ++j) {
            if (j == col.maxLength || bytes[j + pos] == 0) {
              entries[i] = new Entry(new String(Arrays.copyOfRange(bytes, pos, pos + j)), col.maxLength);
              break;
            }
          }
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
      } catch (Exception e) {
        e.printStackTrace();
        System.out.println(e.getMessage());
      }
    }

    return new Row(entries);
  }

  public void insert(UUID uuid, Row row) {
    int id = metadata.freePageList.get(0);
    Page page = cache.readPage(id);
    BitSet bitmap = page.bitmap;
    int index = bitmap.nextClearBit(0);
    byte[] bytes = row.toBytes();

    Dictionary dic = new Hashtable<>();
    dic.put("tableName", this.tableName);
    dic.put("pageNumber", id);
    dic.put("rowIndex", index);
    dic.put("oldData", page.readRow(index));
    dic.put("newData", bytes);
    try {
      new InsertLog(uuid, dic).serialize();
    } catch (IOException e) {
      e.printStackTrace();
    }

    page.writeRow(index, bytes);
    if (page.isFull()) {
      metadata.freePageList.remove(0);
    }
    cache.writePage(id, page);
    // cache.writeBackPage(id); // just for file inspect in test
    this.index.put(row.entries.get(primaryIndex), new VRow(id, index));
  }

  public void delete(UUID uuid, Entry key) {
    VRow row = this.index.get(key);
    Page page = cache.readPage(row.pageID);
    Dictionary dic = new Hashtable<>();
    dic.put("tableName", this.tableName);
    dic.put("pageNumber", row.pageID);
    dic.put("rowIndex", row.rowIndex);
    try {
      new DeleteLog(uuid, dic).serialize();
    } catch (IOException e) {
      e.printStackTrace();
    }
    if (page.isFull()) {
      metadata.freePageList.add(row.pageID);
    }
    page.bitmap.clear(row.rowIndex);
    cache.writePage(row.pageID, page);
    this.index.remove(key);
  }

  public void update(UUID uuid, Row row, Entry key) {
    VRow vrow = this.index.get(key);
    int pageID = vrow.pageID;
    int rowIndex = vrow.rowIndex;
    Page page = cache.readPage(pageID);

    Dictionary dic = new Hashtable<>();
    dic.put("tableName", this.tableName);
    dic.put("pageNumber", pageID);
    dic.put("rowIndex", rowIndex);
    dic.put("oldData", page.readRow(rowIndex));
    dic.put("newData", row.toBytes());
    try {
      new UpdateLog(uuid, dic).serialize();
    } catch (IOException e) {
      e.printStackTrace();
    }

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
