package cn.edu.thssdb.storage;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.FileOutputStream;
import java.io.ObjectOutputStream;
import java.io.ObjectInputStream;

public class MetaFile {
    private String filename;
    private FileInputStream in;
    private FileOutputStream out;

    public MetaFile(String filename) throws IOException {
        this.filename = filename + ".meta";
    }

    public void createFile() {
        try {
            out = new FileOutputStream(this.filename);
            in = new FileInputStream(this.filename);
        } catch (IOException e) {
            System.err.println(String.format("Create file error:%s", this.filename));
            System.exit(-1);
        }
    }

    public void writeMetadata(Metadata data) {
        try {
            ObjectOutputStream objout = new ObjectOutputStream(this.out);
            objout.writeObject(data);
        } catch (IOException e) {
            System.err.println(String.format("Serialize metadata failed:%s", this.filename));
        }
    }

    public Metadata readMetadata() {
        Metadata result = null;
        try {
            ObjectInputStream objin = new ObjectInputStream(this.in);
            result = (Metadata) objin.readObject();
        } catch (Exception e) {
            System.err.println(String.format("Serialize metadata failed:%s", this.filename));
        }
        return result;
    }
}