package cn.edu.thssdb.storage;

import java.io.IOException;
import cn.edu.thssdb.utils.Global;

public class DataFile extends DbFile {

    public DataFile(String filename) throws IOException {
        super(filename);
        this.filename += ".data";
    }

    @Override
    public void createFile() {
        super.createFile();
        // Write 0s to datafile
        try {
            byte[] b = new byte[Global.INIT_FILE_SIZE];
            // outputStream.write(b);
            writer.write(b);
        } catch (IOException e) {
            System.err.println(String.format("create file error:%s", this.filename));
            System.exit(-1);
        }
    }

    public void writePage(int id, byte[] content) {
        try {
            assert content.length == Global.PAGE_SIZE;
            // outputStream.write(content, id * Global.PAGE_SIZE, content.length);
            writer.seek(id * Global.PAGE_SIZE);
            writer.write(content);
        } catch (IOException e) {
            System.err.println(String.format("write page to file error:%s,id=%d", this.filename, id));
            System.exit(-1);
        }
    }

    public byte[] readPage(int id) {
        byte[] buffer = new byte[Global.PAGE_SIZE];
        try {
            // inputStream.read(buffer, id * Global.PAGE_SIZE, Global.PAGE_SIZE);
            reader.seek(id * Global.PAGE_SIZE);
            reader.read(buffer);
        } catch (IOException e) {
            System.err.println(String.format("write page to file error:%s,id=%d", this.filename, id));
            System.exit(-1);
        }
        return buffer;
    }

}