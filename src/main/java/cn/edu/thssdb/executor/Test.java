package cn.edu.thssdb.executor;

import cn.edu.thssdb.schema.Manager;

public class Test {
    public static void main(String[] args) {
        Manager manager = Manager.getInstance();
        manager.createDatabaseIfNotExists("db1");
        manager.switchDatabase("db1");
        SQLExecutor exec = new SQLExecutor();
        exec.execute("begin transaction;" +
                "create TABLE person (name String(256), id Int not null, PRIMARY KEY(ID));" +
                "insert into person values('test-1', 1);" +
                "insert into person values('hello', 2);" +
                "commit;");
    }
}
