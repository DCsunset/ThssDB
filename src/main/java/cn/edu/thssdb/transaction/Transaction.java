package cn.edu.thssdb.transaction;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.UUID;
import java.util.concurrent.locks.ReentrantLock;

import cn.edu.thssdb.schema.Database;
import cn.edu.thssdb.schema.Manager;
import cn.edu.thssdb.schema.Table;
import cn.edu.thssdb.schema.VRow;
import cn.edu.thssdb.transaction.Log.LogType;

public class Transaction {
    public UUID uuid;
    public ArrayList<ReentrantLock> locks = new ArrayList<ReentrantLock>();
    public ArrayList<Log> logs = new ArrayList<>();
    public static HashMap<UUID, Transaction> id2tr = new HashMap<>();

    public Transaction() {
        uuid = UUID.randomUUID();
        id2tr.put(uuid, this);
    }

    public Boolean acquired(ReentrantLock lock) {
        Boolean acquired = false;
        for (ReentrantLock _lock : this.locks) {
            if (_lock == lock) {
                acquired = true;
                break;
            }
        }
        return acquired;
    }

    public void acquireLock(ReentrantLock lock) {
        if (!acquired(lock)) {
            lock.lock(); // try to acquire the lock
            locks.add(lock);
        }
    }

    public void releaseLock(ReentrantLock lock) {
        if (!acquired(lock)) {
            System.err.println("Attempting to release a lock not acquired!");
            return;
        } else {
            lock.unlock();
            locks.remove(lock);
        }
    }

    public void commit() {
        // release all locks
        for (ReentrantLock lock : locks) {
            lock.unlock();
        }
    }

    public void rollback() throws IOException {
        // Write compensation logs
        // Do real compensation
        Manager manager = Manager.getInstance();
        Database db = manager.currentDatabase;
        for (int i = logs.size() - 1; i >= 0; i--) {
            Log log = logs.get(i);
            if (log.type == LogType.Update) {
                String tablename = ((UpdateLog) log).tableName;
                Table table = db.getTables().get(tablename);
                byte[] oldData = ((UpdateLog) log).oldData;
                byte[] newData = ((UpdateLog) log).newData;
                table.update(uuid, table.createRow(oldData), table.getKey(table.createRow(newData)));
            } else if (log.type == LogType.Insert) {
                String tablename = ((InsertLog) log).tableName;
                Table table = db.getTables().get(tablename);
                byte[] newData = ((InsertLog) log).newData;
                table.delete(uuid, table.getKey(table.createRow(newData)));
            } else if (log.type == LogType.Delete) {
                String tablename = ((DeleteLog) log).tableName;
                Table table = db.getTables().get(tablename);
                byte[] oldData = ((DeleteLog) log).oldData;
                int id = ((DeleteLog) log).pageNumber;
                int index = ((DeleteLog) log).rowIndex;
                table.insert(uuid, table.createRow(oldData), new VRow(id, index));
            } else if (log.type == LogType.Create) {
                String tablename = ((CreateLog) log).tableName;
                db.dropTable(tablename);
                new DropLog(uuid, tablename).serialize();
            } else if (log.type == LogType.Drop) {
                // TODO:
            }
        }
        new SimpleLog(uuid, LogType.Rollback).serialize();
        commit(); // release all locks
    }
}