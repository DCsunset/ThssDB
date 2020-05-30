package cn.edu.thssdb.transaction;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.util.Dictionary;
import java.util.UUID;

import cn.edu.thssdb.schema.Manager;
import cn.edu.thssdb.schema.Table;

public class Log {
    public static enum LogType {
        Start, Commit, Normal, Compensation
    }

    public UUID transactionId;
    public LogType type;
    public int pageNumber;
    public int rowIndex;
    public byte[] oldData;
    public byte[] newData;
    public String tableName;

    private static byte[] uuid2Bytes(UUID id) {
        ByteBuffer bb = ByteBuffer.wrap(new byte[16]);
        bb.putLong(id.getMostSignificantBits());
        bb.putLong(id.getLeastSignificantBits());
        return bb.array();
    }

    public void serialize() throws IOException {
        Manager manager = Manager.getInstance();
        RandomAccessFile handler = manager.currentDatabase.logFileHandler;
        handler.write(uuid2Bytes(this.transactionId));
        handler.write(this.type.name().getBytes());
        if (type == Log.LogType.Normal) {
            handler.writeInt(this.tableName.length());
            handler.write(this.tableName.getBytes());
            handler.writeInt(this.pageNumber);
            handler.writeInt(this.rowIndex);
            handler.write(this.oldData);
            handler.write(this.newData);
        } else if (type == Log.LogType.Compensation) {
            // TODO:
        }
    }

    public Log(UUID id, Log.LogType type, Dictionary data) {
        transactionId = id;
        this.type = type;

        if (type == Log.LogType.Normal) {
            this.tableName = (String) data.get("tableName");
            this.pageNumber = (int) data.get("pageNumber");
            this.rowIndex = (int) data.get("rowIndex");
            this.oldData = (byte[]) data.get("oldData");
            this.newData = (byte[]) data.get("newData");
        } else if (type == Log.LogType.Compensation) {
            // TODO:
        }
    }
}