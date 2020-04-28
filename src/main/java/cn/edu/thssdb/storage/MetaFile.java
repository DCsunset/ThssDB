package cn.edu.thssdb.storage;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;

public class MetaFile extends DbFile {
    public MetaFile(String filename) throws IOException {
        super(filename);
        this.filename += ".meta";
    }

    @Override
    public void createFile() {
        super.createFile();
    }

    public void writeMetadata(Metadata data) {
        // TODO:
        // try {
        // ObjectOutputStream out = new ObjectOutputStream(this.outputStream);
        // out.writeObject(data);
        // } catch (IOException e) {
        // System.err.println(String.format("Serialize metadata failed:%s",
        // this.filename));
        // }
    }

    public Metadata readMetadata() {
        Metadata result = null;
        // TODO:
        // try {
        // ObjectInputStream in = new ObjectInputStream(this.inputStream);
        // result = (Metadata) in.readObject();
        // } catch (Exception e) {
        // System.err.println(String.format("Serialize metadata failed:%s",
        // this.filename));
        // }
        return result;
    }
}