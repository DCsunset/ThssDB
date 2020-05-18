package cn.edu.thssdb.schema;

public class VRow {
    public int pageID;
    public int rowIndex;
    public VRow(int pageID, int rowIndex) {
        this.pageID = pageID;
        this.rowIndex = rowIndex;
    }
}
