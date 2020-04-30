package cn.edu.thssdb.storage;

import cn.edu.thssdb.utils.Global;
import com.sun.org.apache.xpath.internal.operations.Bool;
import com.sun.xml.internal.ws.api.message.ExceptionHasMessage;
import com.sun.xml.internal.ws.api.wsdl.parser.MetaDataResolver;

import java.io.IOException;
import java.util.*;

import javafx.util.Pair;

public class DbCache {
    private byte cache[][];
    private List<Integer> freeCacheList;
    // Map id to index, Boolean to mark if dirty
    private Map<Integer, Pair<Integer, Boolean>> idIndex;
    private DataFile dataFile;
    private MetaFile metaFile;
    private Metadata metadata;

    // LFU replacement
    private int[] freqTable;

    public DbCache(String filename, int rowSize) throws IOException {
        dataFile = new DataFile(filename);
        // FIXME
        dataFile.createFile();
        metaFile = new MetaFile(filename);
        metaFile.createFile();
        metadata = new Metadata(rowSize);
        cache = new byte[Global.CACHE_SIZE][Global.PAGE_SIZE];
        idIndex = new HashMap<>();
        freqTable = new int[Global.CACHE_SIZE];
        freeCacheList = new LinkedList<>();
        for (int i = 0; i < Global.CACHE_SIZE; ++i)
            freeCacheList.add(i);
    }

    // Fetch from disk to cache if necessary
    private void fetchPage(int id) {
        // Hit
        if (idIndex.containsKey(id)) {
            return;
        }
        else {
            int newIndex = 0;
            // full, LFU
            if (freeCacheList.isEmpty()) {
                int min = Integer.MAX_VALUE;
                int minId = 0;
                int minIndex = 0;
                for (int origId : idIndex.keySet()) {
                    Pair<Integer, Boolean> indexPair = idIndex.get(origId);
                    int index = indexPair.getKey();
                    if (min > freqTable[index]) {
                        min = freqTable[index];
                        minId = origId;
                        minIndex = index;
                    }
                }

                // Replace
                writeBackPage(minId);
                idIndex.remove(minId);
                newIndex = minIndex;
            }
            else {
                newIndex = freeCacheList.get(0);
                freeCacheList.remove(0);
            }

            idIndex.put(id, new Pair(newIndex, false));
            byte[] page = dataFile.readPage(id);
            cache[newIndex] = page;
            freqTable[newIndex] = 0;
        }
    }

    public byte[] readPage(int id) {
        // Fetch from disk if necessary
        fetchPage(id);
        Pair<Integer, Boolean> indexPair = idIndex.get(id);
        int index = indexPair.getKey();
        ++freqTable[index];
        return cache[index];
    }

    public void writePage(int id, byte content[]) {
        assert content.length == Global.PAGE_SIZE;
        // Fetch from disk if necessary
        fetchPage(id);
        Pair<Integer, Boolean> indexPair = idIndex.get(id);
        int index = indexPair.getKey();
        cache[index] = content;
        ++freqTable[index];
        idIndex.replace(id, new Pair<>(index, true));
    }

    public void removePage(int id) {
        metadata.freePageList.add(id);
        if (idIndex.containsKey(id)) {
            Pair<Integer, Boolean> indexPair = idIndex.get(id);
            int index = indexPair.getKey();
            freeCacheList.add(index);
            idIndex.remove(id);
        }
    }

    // Allocate a new page id
    public int newPage() throws Exception {
        if (metadata.freePageList.isEmpty())
            throw new Exception("No free pages");
        int id = metadata.freePageList.get(0);
        metadata.freePageList.remove(0);
        return id;
    }

    public void writeBackPage(int id) {
        Pair<Integer, Boolean> indexPair = idIndex.get(id);
        int index = indexPair.getKey();
        boolean dirty = indexPair.getValue();
        if (dirty) {
            dataFile.writePage(id, cache[index]);
            idIndex.replace(id, new Pair<>(index, false));
        }
    }

    // Write back to disk
    public void writeBack() {
        for (int id : idIndex.keySet()) {
            writeBackPage(id);
        }
        metaFile.writeMetadata(metadata);
    }
}
