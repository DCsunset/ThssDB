package cn.edu.thssdb.transaction;

import java.io.IOException;
import java.util.Dictionary;
import java.util.UUID;

public class DeleteLog extends Log {
    public int offset;
    public String tableName;

    public DeleteLog(UUID id, Dictionary data) {
        super(id, LogType.Delete);
        this.tableName = (String) data.get("tableName");
        this.offset = (int) data.get("offset");
    }

    @Override
    public void serialize() throws IOException {
        super.serialize();
        handler.writeInt(this.tableName.length());
        handler.write(this.tableName.getBytes());
        handler.writeInt(this.offset);
    }
}