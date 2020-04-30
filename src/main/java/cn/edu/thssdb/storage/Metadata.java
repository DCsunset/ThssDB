package cn.edu.thssdb.storage;

import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.stream.IntStream;

import cn.edu.thssdb.utils.Global;

public class Metadata implements java.io.Serializable {
    private int rowSize; // number of bytes in one row
    private static final long serialVersionUID = 42L;

    public List<Integer> freePageList; // each element is id of page with free rows

    public Metadata(int size) {
        this.rowSize = size;
        int listSize = Global.INIT_FILE_SIZE / Global.PAGE_SIZE;
        freePageList = new LinkedList<>();
        for (int i = 0; i < listSize; i++)
            freePageList.add(i);
    }

    public void setRowSize(short size) {
        this.rowSize = size;
    }

    public int getRowSize() {
        return this.rowSize;
    }

    public Metadata getObject() {
        return this;
    }

    public void writeObject(Metadata data) {
        this.rowSize = data.rowSize;
        this.freePageList = data.freePageList;
    }
}