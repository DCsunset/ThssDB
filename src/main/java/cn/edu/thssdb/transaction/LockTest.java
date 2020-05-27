package cn.edu.thssdb.transaction;

import java.util.concurrent.locks.ReentrantLock;

public class LockTest {
    private static class Thread1 extends Thread {
        ReentrantLock lock;

        public Thread1(ReentrantLock lock) {
            this.lock = lock;
        }

        public void run() {
            lock.lock();
            System.out.println("lock 1");
            try {
                Thread.sleep(2000);
            } catch (Exception e) {

            }
            lock.unlock();
        }
    }

    private static class Thread2 extends Thread {
        ReentrantLock lock;

        public Thread2(ReentrantLock lock) {
            this.lock = lock;
        }

        public void run() {
            lock.lock();
            System.out.println("lock 2");
        }
    }

    public static void main(String[] args) {
        ReentrantLock lock = new ReentrantLock();
        new Thread1(lock).start();
        new Thread2(lock).start();
    }
}