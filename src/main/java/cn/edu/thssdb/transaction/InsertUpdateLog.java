
package cn.edu.thssdb.transaction;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.Dictionary;
import java.util.UUID;

public class InsertUpdateLog extends Log {

    public int pageNumber;
    public int rowIndex;
    public byte[] oldData;
    public byte[] newData;
    public String tableName;

    public InsertUpdateLog(UUID id, Dictionary data) {
        super(id, LogType.Insert);
        this.tableName = (String) data.get("tableName");
        this.pageNumber = (int) data.get("pageNumber");
        this.rowIndex = (int) data.get("rowIndex");
        this.oldData = (byte[]) data.get("oldData");
        this.newData = (byte[]) data.get("newData");
    }

    @Override
    public void serialize() throws IOException {
        super.serialize();
        handler.writeInt(this.tableName.length());
        handler.write(this.tableName.getBytes());
        handler.writeInt(this.pageNumber);
        handler.writeInt(this.rowIndex);
        handler.writeInt(this.oldData.length);
        handler.write(this.oldData);
        handler.write(this.newData);
    }
}