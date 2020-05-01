package cn.edu.thssdb.utils;

import java.util.Arrays;
import java.util.BitSet;

public class Test {
    public static void main(String[] args) {
        byte[] bytes = { 0, 0, 0, 0 };
        BitSet set = BitSet.valueOf(bytes);
        byte[] bytes1 = Arrays.copyOf(set.toByteArray(), 4);
        for (byte c : bytes1) {
            System.out.println(c);
        }
    }
}