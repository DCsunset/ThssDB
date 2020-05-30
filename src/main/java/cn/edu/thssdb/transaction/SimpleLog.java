package cn.edu.thssdb.transaction;

import java.io.IOException;
import java.util.UUID;

// Log for begin transaction and commit
public class SimpleLog extends Log {
    public SimpleLog(UUID id, Log.LogType type) {
        super(id, type);
    }

    @Override
    public void serialize() throws IOException {
        super.serialize();
    }
}