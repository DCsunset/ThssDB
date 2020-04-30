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
    private int rowSize;

    public Metadata metadata;

    // LFU replacement
    private int[] freqTable;

    public DbCache(String filename, int rowSize) throws IOException {
        this.rowSize = rowSize;
        dataFile = new DataFile(filename);
        dataFile.init();
        metaFile = new MetaFile(filename);
        metaFile.init();
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
        } else {
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
            } else {
                newIndex = freeCacheList.get(0);
                freeCacheList.remove(0);
            }

            idIndex.put(id, new Pair(newIndex, false));
            byte[] page = dataFile.readPage(id);
            cache[newIndex] = page;
            freqTable[newIndex] = 0;
        }
    }

    public Page readPage(int id) {
        // Fetch from disk if necessary
        fetchPage(id);
        Pair<Integer, Boolean> indexPair = idIndex.get(id);
        int index = indexPair.getKey();
        ++freqTable[index];
        return new Page(cache[index], rowSize);
    }

    public void writePage(int id, Page page) {
        // Fetch from disk if necessary
        fetchPage(id);
        Pair<Integer, Boolean> indexPair = idIndex.get(id);
        int index = indexPair.getKey();
        cache[index] = page.rawData();
        ++freqTable[index];
        idIndex.replace(id, new Pair<>(index, true));
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
