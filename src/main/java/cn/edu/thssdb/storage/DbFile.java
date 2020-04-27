
package cn.edu.thssdb.storage;

import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;

public class DbFile {
    protected FileOutputStream outputStream;
    protected FileInputStream inputStream;
    protected String filename;

    public DbFile(String filename) {
        this.filename = filename;
    }

    public void createFile() {
        try {
            outputStream = new FileOutputStream(this.filename, true);
            inputStream = new FileInputStream(this.filename);
        } catch (IOException e) {
            System.err.println(String.format("Create file error:%s", this.filename));
            System.exit(-1);
        }
    }

    public void close() {
        try {
            inputStream.close();
            outputStream.close();
        } catch (IOException e) {
            System.err.println(String.format("Close %s error", this.filename));
            System.exit(-1);
        }
    }

}