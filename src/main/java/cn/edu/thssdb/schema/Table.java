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
import jdk.internal.org.objectweb.asm.tree.MultiANewArrayInsnNode;

import java.util.Iterator;
import java.util.concurrent.locks.ReentrantReadWriteLock;

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

  public void insert() {
    // TODO
  }

  public void delete() {
    // TODO
  }

  public void update() {
    // TODO
  }

  private void readObject(ObjectInputStream input) throws Exception {
    System.out.println("1");
    this.databaseName = input.readUTF();
    System.out.println("2");
    this.tableName = input.readUTF();
    System.out.println("3");
    this.metadata = (Metadata) input.readObject();
    System.out.println("4");
    this.primaryIndex = input.readInt();
    System.out.println("5");
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
