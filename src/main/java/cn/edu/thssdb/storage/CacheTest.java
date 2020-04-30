package cn.edu.thssdb.storage;

import cn.edu.thssdb.utils.Global;

import java.io.IOException;

public class CacheTest {
    public static void main(String[] args) throws IOException {
        String tbName = "table1";
        DbCache cache = new DbCache(tbName, 10);

        byte[] content = new byte[Global.PAGE_SIZE];
        for (byte i = 0; i < 5; i++)
            content[i] = i;
        cache.writePage(0, content);
        content[5] = 5;
        cache.writePage(1, content);
        cache.writePage(4, content);
        byte[] content1 = cache.readPage(4);
        for (byte c : content1) {
            System.out.format("%d", c);
        }
        cache.writeBack();
    }
}
