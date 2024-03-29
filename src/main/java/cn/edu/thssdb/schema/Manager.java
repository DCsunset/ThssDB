package cn.edu.thssdb.schema;

import cn.edu.thssdb.exception.DatabaseNotExistException;
import cn.edu.thssdb.server.ThssDB;
import cn.edu.thssdb.transaction.Savepoint;

import javax.xml.crypto.Data;
import java.awt.image.DataBuffer;
import java.io.File;
import java.util.HashMap;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class Manager {
  public static String baseDir = "test";
  private HashMap<String, Database> databases;
  private static ReentrantReadWriteLock lock = new ReentrantReadWriteLock();
  public Database currentDatabase = null;
  private static Boolean flag = false;

  public static Manager getInstance() {
    return Manager.ManagerHolder.INSTANCE;
  }

  public Manager() {
    this.databases = new HashMap<>();
    recover();
  }

  public boolean hasDatabase(String name) {
    return databases.containsKey(name);
  }

  public void createDatabaseIfNotExists(String name) {
    if (!hasDatabase(name)) {
      Database db = new Database(name);
      if (flag == false) { // start checkpoint only after first database
        System.out.println("start checkpoint");
        Savepoint sp = new Savepoint();
        sp.start();
        flag = true;
      }
      databases.put(name, db);
    }
  }

  public void recover() {
    File index = new File(baseDir);
    if (index.isDirectory()) {
      String[] entries = index.list();
      for (String s : entries) {
        createDatabaseIfNotExists(s);
      }
      index.delete();
    } else {
      boolean ok = index.mkdir();
      if (!ok) {
        System.err.println(String.format("create db error"));
        System.exit(-1);
      }
    }
  }

  public void persist() {
    for (Database db : databases.values()) {
      db.persist();
    }
  }

  public void deleteDatabase(String name) {
    if (databases.containsKey(name)) {
      databases.get(name).drop();
      databases.remove(name);
      if (currentDatabase.name == name)
        currentDatabase = null;
    } else {
      throw new DatabaseNotExistException(name);
    }
  }

  public void switchDatabase(String name) {
    if (databases.containsKey(name)) {
      this.currentDatabase = databases.get(name);
    } else {
      throw new DatabaseNotExistException(name);
    }
  }

  private static class ManagerHolder {
    private static final Manager INSTANCE = new Manager();

    private ManagerHolder() {

    }
  }
}
