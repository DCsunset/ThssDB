package cn.edu.thssdb.executor;

import cn.edu.thssdb.schema.Manager;
import cn.edu.thssdb.transaction.Savepoint;

public class AutoCheckpointTest {
    public static void main(String[] args) {
        // Main thread
        (new Thread() {
            public void run() {
                Manager manager = Manager.getInstance();
                manager.createDatabaseIfNotExists("db1");
                manager.switchDatabase("db1");
                SQLExecutor exec = new SQLExecutor();

                exec.execute("begin transaction;"
                        + "create TABLE person (name String(256), id Int not null, PRIMARY KEY(ID));"
                        + "insert into person values('test-1', 1);" + "insert into person values('hello', 2);"
                        + "delete from person where name='test-1';" + "update person set name='world' where id=2;"
                        + "commit;");

                manager.currentDatabase.quit();
                System.exit(0);

            }
        }).start();
        Savepoint sp = new Savepoint();
        sp.setName("savepoint");
        sp.start();
    }
}
