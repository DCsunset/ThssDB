package cn.edu.thssdb.executor;

import cn.edu.thssdb.schema.Manager;
import cn.edu.thssdb.transaction.Savepoint;

public class Test {
    public static void main(String[] args) {
        Manager manager = Manager.getInstance();
        manager.createDatabaseIfNotExists("db1");
        manager.switchDatabase("db1");
        SQLExecutor exec = new SQLExecutor();

        exec.execute("begin transaction;" + "create TABLE person (name String(16), id Int not null, PRIMARY KEY(ID));"
                + "insert into person values('test-1', 1);" + "insert into person values('hello', 2);"
                + "delete from person where name='test-1';" + "update person set name='world' where id=2;"
                + "commit; checkpoint;" + "insert into person values('test-3', 3);");

        manager.currentDatabase.quit();
        System.exit(0);
    }
}
