package cn.edu.thssdb.transaction;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.Dictionary;
import java.util.UUID;

public class InsertLog extends Log {

    public int pageNumber;
    public int rowIndex;
    public byte[] oldData;
    public byte[] newData;
    public String tableName;

    public int offset; // set bit in bitmap

    public InsertLog(UUID id, Dictionary data) {
        super(id, LogType.Insert);
        this.tableName = (String) data.get("tableName");
        this.pageNumber = (int) data.get("pageNumber");
        this.rowIndex = (int) data.get("rowIndex");
        this.oldData = (byte[]) data.get("oldData");
        this.newData = (byte[]) data.get("newData");
        this.offset = (int) data.get("offset");
    }

    @Override
    public void serialize() throws IOException {
        super.serialize();
        handler.writeInt(this.tableName.length());
        handler.write(this.tableName.getBytes());
        handler.writeInt(this.pageNumber);
        handler.writeInt(this.rowIndex);
        handler.write(this.oldData);
        handler.write(this.newData);
        handler.writeInt(this.offset);
    }
}