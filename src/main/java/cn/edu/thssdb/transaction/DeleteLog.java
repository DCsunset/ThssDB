package cn.edu.thssdb.transaction;

import java.io.IOException;
import java.util.Dictionary;
import java.util.UUID;

public class DeleteLog extends Log {
    public String tableName;
    public int pageNumber;
    public int rowIndex;

    public DeleteLog(UUID id, Dictionary data) {
        super(id, LogType.Delete);
        this.tableName = (String) data.get("tableName");
        this.pageNumber = (int) data.get("pageNumber");
        this.rowIndex = (int) data.get("rowIndex");
    }

    @Override
    public void serialize() throws IOException {
        super.serialize();
        handler.writeInt(this.tableName.length());
        handler.write(this.tableName.getBytes());
        handler.writeInt(this.pageNumber);
        handler.writeInt(this.rowIndex);
    }
}