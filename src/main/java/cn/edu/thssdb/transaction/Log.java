package cn.edu.thssdb.transaction;

import java.util.Dictionary;
import java.util.UUID;

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

    public Log(UUID id, Log.LogType type, Dictionary data) {
        transactionId = id;
        this.type = type;

        if (type == Log.LogType.Normal) {
            this.pageNumber = (int) data.get("pageNumber");
            this.rowIndex = (int) data.get("rowIndex");
            this.oldData = (byte[]) data.get("oldData");
            this.newData = (byte[]) data.get("newData");
        } else if (type == Log.LogType.Compensation) {
            // TODO:
        }
    }
}