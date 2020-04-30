package cn.edu.thssdb.storage;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.FileOutputStream;
import java.io.ObjectOutputStream;
import java.io.ObjectInputStream;

public class MetaFile {
    private String filename;
    private Metadata data; // newest content of file
    private FileInputStream in;
    private FileOutputStream out;

    public MetaFile(String filename) throws IOException {
        this.filename = filename + ".meta";
    }

    public void init() {
        try {
            if (!(new File(this.filename).isFile())) {
                out = new FileOutputStream(this.filename);
                in = new FileInputStream(this.filename);
            } else {
                // Could not instanitiate fileoutputstream as it will create a new empty file
                in = new FileInputStream(this.filename);
                ObjectInputStream objin = new ObjectInputStream(this.in);
                this.data = (Metadata) objin.readObject();
            }
        } catch (Exception e) {
            System.err.println(String.format("Create file error:%s", this.filename));
            System.exit(-1);
        }
    }

    public void writeMetadata(Metadata data) {
        try {
            this.data = data;
            if (this.out == null) {
                this.out = new FileOutputStream(this.filename);
            }
            ObjectOutputStream objout = new ObjectOutputStream(this.out);
            objout.writeObject(data);
        } catch (IOException e) {
            System.err.println(String.format("Serialize metadata failed:%s", this.filename));
        }
    }

    public Metadata readMetadata() {
        Metadata result = this.data;
        if (result == null) {
            try {
                ObjectInputStream objin = new ObjectInputStream(this.in);
                result = (Metadata) objin.readObject();
            } catch (Exception e) {
                System.err.println(String.format("DeSerialize metadata failed:%s", this.filename));
            }
        }
        return result;
    }
}