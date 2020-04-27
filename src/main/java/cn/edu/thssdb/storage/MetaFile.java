package cn.edu.thssdb.storage;

import java.io.IOException;

public class MetaFile extends DbFile {
    public MetaFile(String filename) throws IOException {
        super(filename);
        this.filename += ".meta";
    }

    @Override
    public void createFile() {
        super.createFile();
    }

}