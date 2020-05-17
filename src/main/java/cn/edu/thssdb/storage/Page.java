package cn.edu.thssdb.storage;

import cn.edu.thssdb.utils.Global;
import sun.security.util.ArrayUtil;

import javax.xml.bind.annotation.XmlElementDecl;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.BitSet;
import java.util.stream.Stream;

public class Page {
    public BitSet bitmap;
    private int recordNum;
    private int rowSize;
    private final BitSet fullBitSet;
    private byte[] rowData;

    public Page(byte[] rawData, int rowSize) {
        this.rowSize = rowSize;
        recordNum = Integer.min(Global.PAGE_SIZE / rowSize, Global.BITMAP_SIZE);
        bitmap = BitSet.valueOf(Arrays.copyOfRange(rawData, 0, Global.BITMAP_SIZE / 8));
        fullBitSet = new BitSet(bitmap.length());
        fullBitSet.set(0, recordNum);
        rowData = Arrays.copyOfRange(rawData, Global.BITMAP_SIZE / 8, rawData.length);
    }

    public byte[] rawData() {
        byte[] bitmapBytes = Arrays.copyOf(bitmap.toByteArray(), Global.BITMAP_SIZE / 8);
        ByteBuffer buffer = ByteBuffer.allocate(bitmapBytes.length + rowData.length);
        buffer.put(bitmapBytes);
        buffer.put(rowData);
        return buffer.array();
    }

    public byte[] readRow(int index) {
        return Arrays.copyOfRange(rowData, index * rowSize, (index + 1) * rowSize);
    }

    public void writeRow(int index, byte[] data) {
        assert data.length == rowSize;
        for (int i = 0; i < data.length; ++i)
            rowData[index * rowSize + i] = data[i];
        bitmap.set(index);
        System.out.println("after set index, bitmap=" + bitmap);
    }

    public boolean isFull() {
        if (bitmap.length() == 65) {
            System.out.print("fullbitset=" + fullBitSet);
        }
        return fullBitSet.equals(bitmap);
    }
}
