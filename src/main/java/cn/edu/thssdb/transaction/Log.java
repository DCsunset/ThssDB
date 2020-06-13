package cn.edu.thssdb.transaction;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Dictionary;
import java.util.UUID;

import cn.edu.thssdb.exception.CurrentDatabaseNullException;
import cn.edu.thssdb.schema.Manager;
import cn.edu.thssdb.schema.Table;

public abstract class Log {
    public enum LogType {
        Start(1), Commit(2), Update(3), Delete(4), Insert(5), Compensation(6), Create(7), Drop(8), Savepoint(9),
        Rollback(10);

        public final int value;

        LogType(int value) {
            this.value = value;
        }

        public static LogType getLog(int value) {
            for (LogType l : LogType.values()) {
                if (l.value == value)
                    return l;
            }
            throw new IllegalArgumentException("Invalid type");
        }
    }

    public UUID transactionId;
    public LogType type;

    protected RandomAccessFile handler;

    public static byte[] uuid2Bytes(UUID id) {
        ByteBuffer bb = ByteBuffer.wrap(new byte[16]);
        bb.putLong(id.getMostSignificantBits());
        bb.putLong(id.getLeastSignificantBits());
        return bb.array();
    }

    public static UUID uuidFromBytes(byte[] bytes) {
        ByteBuffer bb = ByteBuffer.wrap(new byte[16]);
        long firstLong = bb.getLong();
        long secondLong = bb.getLong();
        return new UUID(firstLong, secondLong);
    }

    public void serialize() throws IOException {
        handler.write(uuid2Bytes(this.transactionId));
        handler.writeInt(this.type.value);
    }

    public Log(UUID id, Log.LogType type) {
        transactionId = id;
        this.type = type;
        Manager manager = Manager.getInstance();
        if (manager.currentDatabase == null) {
            throw new CurrentDatabaseNullException();
        }
        handler = manager.currentDatabase.logFileHandler;
        Transaction ts;
        if ((ts = Transaction.id2tr.get(id)) != null) {
            ts.logs.add(this);
        }
    }
}