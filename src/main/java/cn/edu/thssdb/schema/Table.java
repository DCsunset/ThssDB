package cn.edu.thssdb.schema;

import cn.edu.thssdb.index.BPlusTree;
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

import java.util.Iterator;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.BitSet;

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

  public void insert(Row data) {
    int id = cache.metadata.freePageList.get(0);
    Page page = cache.readPage(id);
    BitSet bitmap = page.bitmap;
    int index = bitmap.nextClearBit(0);
    // page.writeRow(index, data);
    if (page.isFull()) {
      cache.metadata.freePageList.remove(0);
    }
    cache.writePage(id, page);
  }

  public void delete() {
    // TODO
  }

  public void update() {
    // TODO
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
