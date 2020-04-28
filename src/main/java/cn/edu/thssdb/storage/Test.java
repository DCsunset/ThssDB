
package cn.edu.thssdb.storage;

import java.io.IOException;

import cn.edu.thssdb.utils.Global;

public class Test {
    public static void main(String[] args) throws IOException {
        String tbName = "table1";
        DataFile data = new DataFile(tbName);
        data.createFile();
        byte[] content = new byte[Global.PAGE_SIZE];
        for (byte i = 0; i < 5; i++)
            content[i] = i;
        data.writePage(0, content);
        data.writePage(1, content);
        byte[] content1 = data.readPage(1);
        for (byte c : content1) {
            System.out.format("%d", c);
        }

        MetaFile metafile = new MetaFile(tbName);
        metafile.createFile();
        Metadata meta = new Metadata(10);
        metafile.writeMetadata(meta);
        Metadata meta1 = metafile.readMetadata();
        System.out.print(meta1.getRowSize());
        for (short id : meta1.getFreePageList()) {
            System.out.print(id);
        }
    }
}