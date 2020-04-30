package cn.edu.thssdb.storage;

import java.io.IOException;
import cn.edu.thssdb.utils.Global;

public class DataFile extends DbFile {

    public DataFile(String filename) throws IOException {
        super(filename);
        this.filename += ".data";
    }

    @Override
    public void init() {
        super.init();
        // Write 0s to datafile if newly created
        try {
            if (writer.length() == 0) {
                byte[] b = new byte[Global.INIT_FILE_SIZE];
                writer.write(b);
            }
        } catch (IOException e) {
            System.err.println(String.format("create file error:%s", this.filename));
            System.exit(-1);
        }
    }

    public void writePage(int id, byte[] content) {
        try {
            assert content.length == Global.PAGE_SIZE;
            // outputStream.write(content, id * Global.PAGE_SIZE, content.length);
            if (id >= writer.length() / Global.PAGE_SIZE) {
                System.out.println("Allocate new 16k for " + this.filename);
                writer.seek(writer.length());
                byte[] b = new byte[Global.INIT_FILE_SIZE];
                writer.write(b);
            }
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
            writer.seek(id * Global.PAGE_SIZE);
            writer.read(buffer);
        } catch (IOException e) {
            System.err.println(String.format("write page to file error:%s,id=%d", this.filename, id));
            System.exit(-1);
        }
        return buffer;
    }

}