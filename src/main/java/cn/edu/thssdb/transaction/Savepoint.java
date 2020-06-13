package cn.edu.thssdb.transaction;

import java.io.IOError;
import java.io.IOException;

import cn.edu.thssdb.schema.Database;
import cn.edu.thssdb.schema.Manager;
import cn.edu.thssdb.schema.Table;
import cn.edu.thssdb.utils.Global;

public class Savepoint extends Thread {
    public static void save() {
        System.out.println("savepoint");
        // Write back data page
        Manager manager = Manager.getInstance();
        Database database = manager.currentDatabase;
        if (database != null) {
            for (Table table : database.getTables().values()) {
                table.getCache().writeBack();
            }
            // Write back metadata
            database.persist();
            // Log checkpoint
            try {
                new SavepointLog().serialize();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    @Override
    public void run() {
        // Globally savepoint every 2s
        while (true) {
            try {
                Thread.sleep(Global.checkInterval);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            save();
        }
    }
}