package cn.edu.thssdb.transaction;

import cn.edu.thssdb.executor.SQLExecutor;
import cn.edu.thssdb.schema.Manager;

public class RollbackTest {
    public static void main(String[] args) {
        Manager manager = Manager.getInstance();
        manager.createDatabaseIfNotExists("db1");
        manager.switchDatabase("db1");
        SQLExecutor exec = new SQLExecutor();

        exec.execute("create TABLE person (name String(16), id Int not null, PRIMARY KEY(ID));"
                + "insert into person values('test-1', 1);" + "begin transaction;"
                + "insert into person values('hello', 2);" + "delete from person where name='test-1';"
                + "update person set name='world' where id=2;" + "rollback;");
        manager.currentDatabase.quit();
        System.exit(0);
    }
}