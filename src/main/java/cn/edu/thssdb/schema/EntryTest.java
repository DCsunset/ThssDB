package cn.edu.thssdb.schema;

import java.util.Arrays;

public class EntryTest {
    public static void main(String[] args) {
        byte[] b = new byte[10];
        Arrays.fill(b, (byte) 0xff);
        boolean result = true;
        for (int i = 0; i < 10; i++) {
            if (b[i] != (byte) 0xff) {
                result = false;
                break;
            }
        }
        System.out.println((result));
    }
}