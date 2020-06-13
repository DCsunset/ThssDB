package cn.edu.thssdb.transaction;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.Dictionary;
import java.util.UUID;

public class InsertLog extends InsertUpdateLog {
    public InsertLog(UUID id, Dictionary data) {
        super(id, data);
    }
}