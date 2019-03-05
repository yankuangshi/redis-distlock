package com.kyan.rdl;

import java.time.LocalTime;
import java.util.UUID;

/**
 * 基于Redis单节点的分布式锁
 *
 * 获取锁
 * SET resource_name unique_value NX EX|PX max-lock-time
 * <link>ref: https://redis.io/commands/set</link>
 *
 * 释放锁（lua脚本，比较value，以防误解锁）
 * <code>
 * if redis.call("get",KEYS[1]) == ARGV[1]
 * then
 *     return redis.call("del",KEYS[1])
 * else
 *     return 0
 * end
 * </code>
 *
 */
public class RedisDistLock {

    private static final String NOT_EXIST = "NX";
    private static final String EXPIRE = "PX";
    private static final String SUCCESS = "OK";
    private static final int DEFAULT_EXPIRE_TIME = 30000;
    private static final int DEFAULT_ACQUIRY_RESOLUTION_MILLIS = 100;
    private RedisTool rt;
    private String lockKey;
    private String lockValue;

    public RedisDistLock(String lockKey, String lockValue) {
        this.rt = new RedisTool();
        this.lockKey = lockKey;
        this.lockValue = lockValue;
    }

    //Initiate lock value with random UUID + thread ID
    public RedisDistLock(String lockKey) {
        this(lockKey, UUID.randomUUID().toString() + Thread.currentThread().getId());
    }

    /**
     * Acquires the lock if it's available; If the lock isn't available
     * a thread gets blocked until the lock is released
     *
     * 阻塞式获取锁
     */
    public void lock() {
        while (true) {
            String ret = rt.set(lockKey, lockValue, NOT_EXIST, EXPIRE, DEFAULT_EXPIRE_TIME);//expire in 30sec
            if (SUCCESS.equals(ret)) {
                System.out.println(String.format("Thread: %s acquires lock with success; Time at: %s",
                        Thread.currentThread().getName(), LocalTime.now()));
                break;
            }
            System.out.println(String.format("Thread: %s acquire lock failed, sleep 10secs; Time at: %s",
                    Thread.currentThread().getName(), LocalTime.now()));
            sleepBySec(10);
        }
    }


    /**
     * Non-blocking version of lock() method; It attempts to acquire the lock
     * immediately, return true if locking succeeds
     *
     * 获取锁，如果无法获取立即返回
     * @return
     */
    public boolean tryLock() {
        String ret = rt.set(lockKey, lockValue, NOT_EXIST, EXPIRE, DEFAULT_EXPIRE_TIME);
        if (SUCCESS.equals(ret)) {
            System.out.println(String.format("Thread: %s acquires lock with success; Time at: %s",
                    Thread.currentThread().getName(), LocalTime.now()));
            return true;
        }
        System.out.println(String.format("Thread: %s acquires lock failed, return; Time at: %s",
                Thread.currentThread().getName(), LocalTime.now()));
        return false;
    }

    /**
     * Similar to tryLock(), except it waits up the given timeout
     * before giving up trying to acquire the lock
     *
     * 获取锁，并且设置一个获取锁的timeout时间
     * @param timeout milliseconds
     * @return
     */
    public boolean tryLock(long timeout) {
        long end = System.currentTimeMillis() + timeout;
        while (System.currentTimeMillis() < end) {
            String ret = rt.set(lockKey, lockValue, NOT_EXIST, EXPIRE, DEFAULT_EXPIRE_TIME);
            if (SUCCESS.equals(ret)) {
                System.out.println(String.format("Thread: %s acquires lock with success; Time at: %s",
                        Thread.currentThread().getName(), LocalTime.now()));
                return true;
            }
            try {
                System.out.println(String.format("Thread: %s acquires lock failed, sleep 100msecs; Time at: %s",
                        Thread.currentThread().getName(), LocalTime.now()));
                Thread.sleep(DEFAULT_ACQUIRY_RESOLUTION_MILLIS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
        System.out.println(String.format("Thread: %s acquires lock failed, timeout; Time at: %s",
                Thread.currentThread().getName(), LocalTime.now()));
        return false;
    }

    public void unlock() {
        String checkAndDelScript =
                "if redis.call('get',KEYS[1]) == ARGV[1] then " +
                " return redis.call('del',KEYS[1])" +
                " else" +
                " return 0" +
                " end";
        rt.eval(checkAndDelScript, 1, lockKey, lockValue);
        System.out.println(String.format("Thread %s releases lock; Time at: %s", Thread.currentThread().getName(), LocalTime.now()));
    }

    private void sleepBySec(int sec) {
        try {
            Thread.sleep(sec * 1000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args) {
        RedisDistLock redisDistLock = new RedisDistLock("lock");
        Thread threaA = new Thread("A") {
            @Override
            public void run() {
                redisDistLock.lock();
                try {
                    sleep(5000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
                redisDistLock.unlock();
            }
        };
        Thread threaB = new Thread("B") {
            @Override
            public void run() {
                try {
                    sleep(100);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
                redisDistLock.lock();
            }
        };
        threaA.start();
        threaB.start();
    }

}
