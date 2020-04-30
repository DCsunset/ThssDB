
package cn.edu.thssdb.storage;

import java.io.IOException;

import cn.edu.thssdb.utils.Global;

public class Test {
    public static void main(String[] args) throws IOException {
        String tbName = "table1";
        DataFile data = new DataFile(tbName);
        data.init();
        byte[] content = new byte[Global.PAGE_SIZE];
        for (byte i = 0; i < 5; i++)
            content[i] = i;
        data.writePage(0, content);
        content[5] = 5;
        data.writePage(1, content);
        data.writePage(4, content);
        byte[] content1 = data.readPage(4);
        // for (byte c : content1) {
        // System.out.format("%d", c);
        // }
        data.close();
        DataFile data1 = new DataFile(tbName);
        data1.init();
        byte[] content2 = data1.readPage(4);
        // for (byte c : content2) {
        // System.out.format("%d", c);
        // }

        MetaFile metafile = new MetaFile(tbName);
        metafile.init();
        Metadata meta = new Metadata(40);
        metafile.writeMetadata(meta);
        Metadata meta2 = new Metadata(50);
        metafile.writeMetadata(meta2);
        metafile.readMetadata();
        metafile.writeMetadata(meta);
        Metadata meta1 = metafile.readMetadata();
        System.out.print(meta1.getRowSize());
        for (int id : meta1.freePageList) {
            System.out.print(id);
        }
    }
}