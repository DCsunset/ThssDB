package cn.edu.thssdb.storage;

import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.stream.IntStream;

import cn.edu.thssdb.utils.Global;

public class Metadata implements java.io.Serializable {
    private int rowSize; // number of bytes in one row
    private short[] freePageList = null; // each element is id of page with free rows
    private static final long serialVersionUID = 42L;

    public Metadata(int size) {
        this.rowSize = size;
        freePageList = new short[Global.INIT_FILE_SIZE / Global.PAGE_SIZE];
        for (int i = 0; i < freePageList.length; i++)
            freePageList[i] = (short) i;
    }

    public void setRowSize(short size) {
        this.rowSize = size;
    }

    public void setFreePageList(short[] list) {
        this.freePageList = list;
    }

    public int getRowSize() {
        return this.rowSize;
    }

    public short[] getFreePageList() {
        return this.freePageList;
    }

    public Metadata getObject() {
        return this;
    }

    public void writeObject(Metadata data) {
        this.rowSize = data.rowSize;
        this.freePageList = data.freePageList;
    }
}