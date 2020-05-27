package cn.edu.thssdb.transaction;

import java.util.ArrayList;
import java.util.UUID;
import java.util.concurrent.locks.ReentrantLock;

public class Transaction {
    public UUID uuid;
    public ArrayList<ReentrantLock> locks = new ArrayList<ReentrantLock>();

    public Transaction() {
        uuid = UUID.randomUUID();
    }

    public Boolean acquired(ReentrantLock lock) {
        Boolean acquired = false;
        for (ReentrantLock _lock : this.locks) {
            if (_lock == lock) {
                acquired = true;
                break;
            }
        }
        return acquired;
    }

    public void acquireLock(ReentrantLock lock) {
        if (!acquired(lock)) {
            lock.lock(); // try to acquire the lock
            locks.add(lock);
        }
    }

    public void releaseLock(ReentrantLock lock) {
        if (!acquired(lock)) {
            System.err.println("Attempting to release a lock not acquired!");
            return;
        } else {
            lock.unlock();
            locks.remove(lock);
        }
    }

    public void commit() {
        // release all locks
        for (ReentrantLock lock : locks) {
            lock.unlock();
        }
    }

    public void rollback() {
        // TODO:
    }
}