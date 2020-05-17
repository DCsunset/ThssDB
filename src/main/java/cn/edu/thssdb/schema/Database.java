package cn.edu.thssdb.schema;

import cn.edu.thssdb.query.QueryResult;
import cn.edu.thssdb.query.QueryTable;
import com.sun.xml.internal.fastinfoset.tools.FI_DOM_Or_XML_DOM_SAX_SAXEvent;

import java.io.*;
import java.util.HashMap;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class Database {

    public String name;
    private HashMap<String, Table> tables;
    ReentrantReadWriteLock lock;

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

    public void dropTable(String name) throws Exception {
        if (tables.containsKey(name)) {
            // remove file
            File tbFile = new File(Manager.baseDir + "/" + this.name + "/" + name + ".data");
            if (tbFile.exists()) {
                tbFile.delete();
                // remove cache and metadata
                tables.remove(name);
            } else {
                throw new Exception("no such file:" + tbFile.getPath());
            }
        } else {
            throw new Exception("no such table:" + name);
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
            } catch (IOException e) {
                System.err.println(String.format("De-Serialize metadata failed"));
            } catch (ClassNotFoundException c) {
                System.err.println(String.format("De-Serialize metadata failed"));
            }
        } else {
            boolean ok = file.mkdir();
            if (!ok) {
                System.err.println(String.format("create db error"));
                System.exit(-1);
            }
            persist();
        }
    }

    public void quit() {
        persist();
    }
}
