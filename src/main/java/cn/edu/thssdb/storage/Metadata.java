package cn.edu.thssdb.storage;

import java.util.LinkedList;
import java.util.List;
import cn.edu.thssdb.schema.*;

import cn.edu.thssdb.utils.Global;

// Metadata for a table
public class Metadata implements java.io.Serializable {
    private int rowSize; // number of bytes in one row
    private static final long serialVersionUID = 42L;

    public Column[] columns;
    public List<Integer> freePageList; // each element is id of page with free rows

    public Metadata(Column[] cls) {
        this.columns = cls;
        this.rowSize = 0;
        for (int i = 0; i < cls.length; i++) {
            rowSize += cls[i].getMaxLength();
        }
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