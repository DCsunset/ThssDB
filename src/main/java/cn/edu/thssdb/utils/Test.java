package cn.edu.thssdb.utils;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.BitSet;

public class Test {
    public static void main(String[] args) {
        String value = "Bob";
        byte[] b = value.toString().getBytes();
        byte[] b1 = ByteBuffer.allocate(256).put(b).array();
        String value1 = new String(b1);
        System.out.println(value.equals(value1));
    }
}