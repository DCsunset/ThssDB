package cn.edu.thssdb.transaction;

import java.io.IOException;
import java.util.UUID;

public class SavepointLog extends Log {
    public SavepointLog() {
        super(UUID.randomUUID(), LogType.Savepoint);
    }

    @Override
    public void persist() throws IOException {
        super.persist();
        persist();
    }
}