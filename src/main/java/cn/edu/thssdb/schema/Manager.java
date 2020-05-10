package cn.edu.thssdb.schema;

import cn.edu.thssdb.server.ThssDB;

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

  public static Manager getInstance() {
    return Manager.ManagerHolder.INSTANCE;
  }

  public Manager() {
    this.databases = new HashMap<>();
    recover();
  }

  public void createDatabaseIfNotExists(String name) {
    if (!databases.containsKey(name)) {
      Database db = new Database(name);
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
    }
    else {
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

  private void deleteDatabase(String name) {
    if (databases.containsKey(name)) {
      databases.get(name).drop();
      databases.remove(name);
      if (currentDatabase.name == name)
        currentDatabase = null;
    }
    else {
      System.err.println(String.format("Database %s doesn't exist", name));
    }
  }

  public void switchDatabase(String name) {
    if (databases.containsKey(name)) {
      this.currentDatabase = databases.get(name);
    }
    else {
      System.err.println(String.format("Database %s doesn't exist", name));
    }
  }

  private static class ManagerHolder {
    private static final Manager INSTANCE = new Manager();
    private ManagerHolder() {

    }
  }
}
