package cn.edu.thssdb.transaction;

import java.io.IOException;

public class SavepointLog extends Log {
    public SavepointLog() {
        super(null, LogType.Savepoint);
    }

    @Override
    public void serialize() throws IOException {
        handler.writeInt(this.type.value);
    }

}