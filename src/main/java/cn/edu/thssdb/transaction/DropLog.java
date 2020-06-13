package cn.edu.thssdb.transaction;

import cn.edu.thssdb.schema.Column;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.util.UUID;

public class DropLog extends Log {
    private String tableName;

    public DropLog(UUID id, String tableName) {
        super(id, LogType.Drop);
        this.tableName = tableName;
    }

    @Override
    public void serialize() throws IOException {
        super.serialize();
        handler.writeInt(tableName.length());
        handler.write(tableName.getBytes());
    }
}
