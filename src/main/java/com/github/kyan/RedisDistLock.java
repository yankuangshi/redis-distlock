package com.github.kyan;

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
        this(lockKey, UUID.randomUUID().toString() + ":" + Thread.currentThread().getId());
    }

    /**
     * Acquires the lock.
     *
     * If it's available then return;
     * If the lock isn't available the current thread gets blocked until the lock is released
     *
     * 阻塞式获取锁
     * @param lockTimeout 锁的过期时间
     */
    public void lock(long lockTimeout) {
        while (true) {
            String ret = rt.set(lockKey, lockValue, NOT_EXIST, EXPIRE, lockTimeout);//expire in lockTimeout
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
     * @param lockTimeout 锁的过期时间
     * @return
     */
    public boolean tryLock(long lockTimeout) {
        String ret = rt.set(lockKey, lockValue, NOT_EXIST, EXPIRE, lockTimeout);
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
     * Similar to tryLock(), except it waits up the given waitTime
     * before giving up trying to acquire the lock
     *
     * 获取锁，如果无法获取，一直尝试直到waitTime结束
     * @param waitTime 尝试获取锁的等待时间
     * @param lockTimeout 锁的过期时间
     * @return
     */
    public boolean tryLock(long waitTime, long lockTimeout) {
        long end = System.currentTimeMillis() + waitTime;
        while (System.currentTimeMillis() < end) {
            String ret = rt.set(lockKey, lockValue, NOT_EXIST, EXPIRE, lockTimeout);
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
        System.out.println(String.format("Thread: %s releases lock; Time at: %s", Thread.currentThread().getName(), LocalTime.now()));
    }

    private void sleepBySec(int sec) {
        try {
            Thread.sleep(sec * 1000);
        } catch (InterruptedException e) {
        }
    }

    public static void main(String[] args) {
        RedisDistLock redisDistLock = new RedisDistLock("lock");
        Thread threadA = new Thread("A") {
            @Override
            public void run() {
                redisDistLock.lock(30000);
                try {
                    Thread.sleep(5000);
                } catch (InterruptedException e) {
                }
                redisDistLock.unlock();
            }
        };
        Thread threadB = new Thread("B") {
            @Override
            public void run() {
                try {
                    Thread.sleep(100);
                } catch (InterruptedException e) {
                }
                redisDistLock.lock(30000);
            }
        };
        threadA.start();
        threadB.start();
    }

}
