package cn.edu.thssdb.storage;

import cn.edu.thssdb.utils.Global;

import java.io.IOException;

public class CacheTest {
    public static void main(String[] args) throws IOException {
        String tbName = "table1";
        DbCache cache = new DbCache(tbName, 10);

        int id = cache.metadata.freePageList.get(0);
        Page page = cache.readPage(id);
        assert page.isFull() == false;

        byte[] row = page.readRow(0);
        for (byte i = 0; i < 5; i++)
            row[i] = i;
        page.writeRow(0, row);
        page.writeRow(1, row);
        if (page.isFull())
            cache.metadata.freePageList.remove(0);

        byte[] content1 = page.readRow(0);
        for (byte c : content1) {
            System.out.format("%d", c);
        }
        cache.writeBack();
    }
}
