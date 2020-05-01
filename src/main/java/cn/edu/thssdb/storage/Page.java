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
        recordNum = Integer.max(Global.PAGE_SIZE / rowSize, Global.BITMAP_SIZE);
        System.out.println("rawbitmap=");
        for (byte c : Arrays.copyOfRange(rawData, 0, Global.BITMAP_SIZE / 8)) {
            System.out.print(c + " ");
        }
        bitmap = BitSet.valueOf(Arrays.copyOfRange(rawData, 0, Global.BITMAP_SIZE / 8));
        fullBitSet = new BitSet(bitmap.length());
        fullBitSet.set(0, recordNum);
        rowData = Arrays.copyOfRange(rawData, Global.BITMAP_SIZE / 8, rawData.length);
    }

    public byte[] rawData() {
        byte[] bitmapBytes = Arrays.copyOf(bitmap.toByteArray(), Global.BITMAP_SIZE / 8);
        System.out.print("bitmapBytes=");
        for (byte c : bitmapBytes) {
            System.out.print(c + " ");
        }
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
    }

    public boolean isFull() {
        return fullBitSet.equals(bitmap);
    }
}
