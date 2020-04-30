
package cn.edu.thssdb.storage;

import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.RandomAccess;

public class DbFile {
    // protected FileOutputStream outputStream;
    // protected FileInputStream inputStream;
    protected String filename;
    RandomAccessFile writer = null;

    public DbFile(String filename) {
        this.filename = filename;
    }

    public void init() {
        try {
            writer = new RandomAccessFile(this.filename, "rwd");
        } catch (IOException e) {
            System.err.println(String.format("Create file error:%s", this.filename));
            System.exit(-1);
        }
    }

    public void close() {
        try {
            writer.close();
        } catch (IOException e) {
            System.err.println(String.format("Close %s error", this.filename));
            System.exit(-1);
        }
    }

}