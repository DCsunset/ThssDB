package cn.edu.thssdb.schema;

import cn.edu.thssdb.exception.TableDataFileNotExistException;
import cn.edu.thssdb.exception.TableNotExistException;
import cn.edu.thssdb.executor.SQLExecutor;
import cn.edu.thssdb.query.QueryResult;
import cn.edu.thssdb.query.QueryTable;
import cn.edu.thssdb.rpc.thrift.IService;
import cn.edu.thssdb.transaction.CreateLog;
import cn.edu.thssdb.transaction.DropLog;
import cn.edu.thssdb.transaction.InsertLog;
import cn.edu.thssdb.transaction.Log;

import java.io.*;
import java.util.HashMap;
import java.util.UUID;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class Database {

    public String name;
    private HashMap<String, Table> tables;
    ReentrantReadWriteLock lock;
    public RandomAccessFile logFileHandler;

    public HashMap<String, Table> getTables() {
        return tables;
    }

    public Database(String name) {
        this.name = name;
        this.tables = new HashMap<>();
        this.lock = new ReentrantReadWriteLock();
        recover();
    }

    public void persist() {
        try {
            FileOutputStream metaFile = new FileOutputStream(Manager.baseDir + "/" + name + "/" + name + ".meta");
            ObjectOutputStream obj = new ObjectOutputStream(metaFile);
            obj.writeObject(tables);
            obj.close();
            metaFile.close();
        } catch (IOException e) {
            System.err.println(String.format("Serialize metadata failed"));
        }
    }

    public void create(String name, Column[] columns) {
        if (tables.containsKey(name)) {
            System.err.println(String.format("Table %s already exists", name));
            return;
        }
        Table table = new Table(this.name, name, columns);
        tables.put(name, table);
    }

    public void create(UUID uuid, String name, Column[] columns) {
        create(name, columns);
        try {
            new CreateLog(uuid, name, columns).serialize();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void dropTable(String name) {
        if (tables.containsKey(name)) {
            // remove file
            File tbFile = new File(Manager.baseDir + "/" + this.name + "/" + name + ".data");
            if (tbFile.exists()) {
                tbFile.delete();
                // remove cache and metadata
                tables.remove(name);
            } else {
                throw new TableDataFileNotExistException(tbFile.getPath());
            }
        } else {
            throw new TableNotExistException(name);
        }
    }

    public void dropTable(UUID uuid, String name) throws Exception {
        dropTable(name);
        try {
            new DropLog(uuid, name).serialize();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void drop() {
        File index = new File(Manager.baseDir + "/" + name);
        if (index.isDirectory()) {
            String[] entries = index.list();
            for (String s : entries) {
                File currentFile = new File(index.getPath(), s);
                currentFile.delete();
            }
            index.delete();
        }
    }

    public String select(QueryTable[] queryTables) {
        // TODO
        QueryResult queryResult = new QueryResult(queryTables);
        return null;
    }

    private void recover() {
        File file = new File(Manager.baseDir + "/" + name);
        if (file.isDirectory()) {
            try {
                FileInputStream metaFile = new FileInputStream(Manager.baseDir + "/" + name + "/" + name + ".meta");
                ObjectInputStream obj = new ObjectInputStream(metaFile);
                tables = (HashMap) obj.readObject();
                obj.close();
                metaFile.close();

                logFileHandler = new RandomAccessFile(Manager.baseDir + "/" + name + "/" + name + ".log", "rwd");
                recoverFromLog();
            } catch (Exception e) {
                e.printStackTrace();
                System.err.println(String.format("De-Serialize metadata failed"));
            }
        } else {
            boolean ok = file.mkdir();
            persist();
            // Create log
            try {
                logFileHandler = new RandomAccessFile(Manager.baseDir + "/" + name + "/" + name + ".log", "rwd");
            } catch (Exception e) {
                System.err.println(e.getMessage());
                e.printStackTrace();
            }
            if (!ok) {
                System.err.println(String.format("create db error"));
                System.exit(-1);
            }
        }
    }

    private long lastCheckpoint() throws Exception {
        long last = 0;
        while (true) {
            try {
                byte bytes[] = new byte[16];
                logFileHandler.read(bytes);
                UUID id = Log.uuidFromBytes(bytes);
                Log.LogType type = Log.LogType.getLog(logFileHandler.readInt());

                if (type == Log.LogType.Insert || type == Log.LogType.Update) {
                    int tableNameLength = logFileHandler.readInt();
                    logFileHandler.skipBytes(tableNameLength);
                    logFileHandler.skipBytes(8);
                    int rowLength = logFileHandler.readInt();
                    logFileHandler.skipBytes(rowLength * 2);
                } else if (type == Log.LogType.Delete) {
                    int tableNameLength = logFileHandler.readInt();
                    logFileHandler.skipBytes(tableNameLength);
                    logFileHandler.skipBytes(8);
                    int rowLength = logFileHandler.readInt();
                    logFileHandler.skipBytes(rowLength);
                } else if (type == Log.LogType.Create) {
                    int tableNameLength = logFileHandler.readInt();
                    logFileHandler.skipBytes(tableNameLength);
                    int length = logFileHandler.readInt();
                    logFileHandler.skipBytes(length);
                } else if (type == Log.LogType.Drop) {
                    int tableNameLength = logFileHandler.readInt();
                    logFileHandler.skipBytes(tableNameLength);
                } else if (type == Log.LogType.Savepoint) {
                    last = logFileHandler.getFilePointer();
                }
            } catch (IOException e) {
                break;
            }
        }
        return last;
    }

    private void recoverFromLog() throws Exception {
        logFileHandler.seek(lastCheckpoint());
        while (true) {
            long currentPos = logFileHandler.getFilePointer();
            try {
                byte bytes[] = new byte[16];
                logFileHandler.read(bytes);
                UUID id = Log.uuidFromBytes(bytes);
                Log.LogType type = Log.LogType.getLog(logFileHandler.readInt());

                if (type == Log.LogType.Insert || type == Log.LogType.Update) {
                    int tableNameLength = logFileHandler.readInt();
                    bytes = new byte[tableNameLength];
                    logFileHandler.read(bytes);
                    String tableName = new String(bytes);
                    Table table = tables.get(tableName);

                    int pageId = logFileHandler.readInt();
                    int rowIndex = logFileHandler.readInt();
                    int rowLength = logFileHandler.readInt();
                    bytes = new byte[rowLength];
                    // old data
                    logFileHandler.read(bytes);
                    // new data
                    logFileHandler.read(bytes);
                    // REDO
                    if (type == Log.LogType.Insert)
                        table.write(pageId, rowIndex, table.createRow(bytes));
                    else
                        table.update(pageId, rowIndex, table.createRow(bytes));
                } else if (type == Log.LogType.Delete) {
                    int tableNameLength = logFileHandler.readInt();
                    bytes = new byte[tableNameLength];
                    logFileHandler.read(bytes);
                    String tableName = new String(bytes);
                    Table table = tables.get(tableName);

                    int pageId = logFileHandler.readInt();
                    int rowIndex = logFileHandler.readInt();
                    int rowLength = logFileHandler.readInt();
                    bytes = new byte[rowLength];
                    // old data
                    logFileHandler.read(bytes);
                    Row oldRow = table.createRow(bytes);
                    Entry key = oldRow.getEntries().get(table.primaryIndex);
                    table.delete(pageId, rowIndex, key);
                } else if (type == Log.LogType.Create) {
                    int tableNameLength = logFileHandler.readInt();
                    bytes = new byte[tableNameLength];
                    logFileHandler.read(bytes);
                    String tableName = new String(bytes);

                    int length = logFileHandler.readInt();
                    bytes = new byte[length];
                    logFileHandler.read(bytes);
                    ObjectInputStream is = new ObjectInputStream(new ByteArrayInputStream(bytes));
                    Column[] columns = (Column[]) is.readObject();
                    create(tableName, columns);
                } else if (type == Log.LogType.Drop) {
                    int tableNameLength = logFileHandler.readInt();
                    bytes = new byte[tableNameLength];
                    logFileHandler.read(bytes);
                    String tableName = new String(bytes);
                    dropTable(tableName);
                }
            } catch (IOException e) {
                logFileHandler.seek(currentPos);
                break;
            }
        }
    }

    public void quit() {
        persist();
        for (Table table : this.getTables().values()) {
            table.getCache().writeBack();
        }
        // Clear log
        try {
            this.logFileHandler.setLength(0);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
