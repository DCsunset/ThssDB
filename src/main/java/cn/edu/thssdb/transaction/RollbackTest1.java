package cn.edu.thssdb.transaction;

import cn.edu.thssdb.executor.SQLExecutor;
import cn.edu.thssdb.schema.Manager;

public class RollbackTest1 {
    public static void main(String[] args) {
        Manager manager = Manager.getInstance();
        manager.createDatabaseIfNotExists("db1");
        manager.switchDatabase("db1");
        SQLExecutor exec = new SQLExecutor();

        exec.execute("select * from person;");
        manager.currentDatabase.quit();
        System.exit(0);
    }
}