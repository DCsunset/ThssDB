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
        Start, Commit, Update, Delete, Insert, Compensation
    }

    public UUID transactionId;
    public LogType type;

    protected RandomAccessFile handler;

    private static byte[] uuid2Bytes(UUID id) {
        ByteBuffer bb = ByteBuffer.wrap(new byte[16]);
        bb.putLong(id.getMostSignificantBits());
        bb.putLong(id.getLeastSignificantBits());
        return bb.array();
    }

    public void serialize() throws IOException {
        Manager manager = Manager.getInstance();
        handler = manager.currentDatabase.logFileHandler;
        handler.write(uuid2Bytes(this.transactionId));
        handler.write(this.type.name().getBytes());
        if (type == Log.LogType.Insert || type == LogType.Update) {
        } else if (type == Log.LogType.Delete) {
            handler.writeInt(offset);
            handler.writeBoolean(newBitValue);
        } else if (type == Log.LogType.Compensation) {
            // TODO:
        }
    }

    public Log(UUID id, Log.LogType type) {
        transactionId = id;
        this.type = type;

        if (type == Log.LogType.Insert || type == LogType.Update) {
        } else if (type == LogType.Delete) {
            this.offset = (int) data.get("offset");
            this.newBitValue = (Boolean) data.get("newBitValue");
        } else if (type == Log.LogType.Compensation) {
            // TODO:
        }
    }
}