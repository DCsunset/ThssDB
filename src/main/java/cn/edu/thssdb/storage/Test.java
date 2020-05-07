
package cn.edu.thssdb.storage;

import java.io.IOException;
import java.util.ArrayList;

import cn.edu.thssdb.schema.*;
import cn.edu.thssdb.type.ColumnInfo.ColumnType;
import cn.edu.thssdb.utils.Global;

public class Test {
    public static void main(String[] args) throws IOException {
        String tbName = "table1";
        // DataFile data = new DataFile(tbName);
        // data.init();
        // byte[] content = new byte[Global.PAGE_SIZE];
        // for (byte i = 0; i < 5; i++)
        // content[i] = i;
        // data.writePage(0, content);
        // content[5] = 5;
        // data.writePage(1, content);
        // data.writePage(4, content);
        // byte[] content1 = data.readPage(4);
        // for (byte c : content1) {
        // System.out.format("%d", c);
        // }
        // data.close();
        // DataFile data1 = new DataFile(tbName);
        // data1.init();
        // byte[] content2 = data1.readPage(4);
        // byte[] content3 = data1.readPage(1);
        // byte[] content4 = data1.readPage(0);
        // System.out.println("content2");
        // for (byte c : content2) {
        // System.out.format("%d", c);
        // }
        // System.out.println("content3");
        // for (byte c : content3) {
        // System.out.format("%d", c);
        // }
        // System.out.println("content4");
        // for (byte c : content4) {
        // System.out.format("%d", c);
        // }

        MetaFile metafile = new MetaFile(tbName);
        metafile.init();
        // ArrayList<Column> columns = new ArrayList<>();
        // columns.add(new Column("name", ColumnType.STRING, true, true, 250));
        // columns.add(new Column("age", ColumnType.INT, true, true));
        // Metadata meta = new Metadata(columns);
        // metafile.writeMetadata(meta);
        // Metadata meta1 = metafile.readMetadata();
        // System.out.print(meta1.getRowSize());
        // for (int id : meta1.freePageList) {
        // System.out.print(id);
        // }
        // for (Column c : meta1.columns) {
        // System.out.println(c);
        // }
    }
}