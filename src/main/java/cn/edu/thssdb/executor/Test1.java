package cn.edu.thssdb.executor;

import cn.edu.thssdb.schema.Manager;

public class Test1 {
    public static void main(String[] args) {
        // reopen database
        Manager manager = Manager.getInstance();
        manager.createDatabaseIfNotExists("db1");
        manager.switchDatabase("db1");
        SQLExecutor newexec = new SQLExecutor();
        System.out.println(manager.currentDatabase.getTables().size());
        newexec.execute("select * from person;");
        System.exit(0);
    }
}