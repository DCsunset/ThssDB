package cn.edu.thssdb.transaction;

import cn.edu.thssdb.schema.Column;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.util.UUID;

public class CreateLog extends Log {
    private Column[] columns;
    public String tableName;

    public CreateLog(UUID id, String tableName, Column[] columns) {
        super(id, LogType.Create);
        this.tableName = tableName;
        this.columns = columns;
    }

    @Override
    public void serialize() throws IOException {
        super.serialize();
        handler.writeInt(tableName.length());
        handler.write(tableName.getBytes());
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        ObjectOutputStream os = new ObjectOutputStream(bos);
        os.writeObject(columns);
        os.close();
        byte[] bytes = bos.toByteArray();
        handler.writeInt(bytes.length);
        handler.write(bytes);
    }
}
