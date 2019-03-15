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
public class RedisLock {

    private static final String SET_IF_NOT_EXIST = "NX";
    private static final String SET_WITH_EXPIRE = "PX";
    private static final String LOCK_SUCCESS = "OK";
    private static final Long UNLOCK_SUCCESS = 1L;
    private static final int DEFAULT_LOCK_EXPIRE_TIME_MILLIS = 3000;    //3s
    private static final int DEFAULT_SLEEP_TIME = 100;      //100ms
    private RedisTool rt;
    private String lockKey;
    private String lockValue;
    private int lockExpireTime;

    public RedisLock(String lockKey, String lockValue, int lockExpireTime) {
        this.rt = new RedisTool();
        this.lockKey = lockKey;
        this.lockValue = lockValue;
        this.lockExpireTime = lockExpireTime;
    }

    public RedisLock(String lockKey, int lockExpireTime) {
        this(lockKey, UUID.randomUUID().toString() + ":" + Thread.currentThread().getId(), lockExpireTime);
    }

    //Initiate lock value with random UUID + thread ID
    public RedisLock(String lockKey) {
        this(lockKey, UUID.randomUUID().toString() + ":" + Thread.currentThread().getId(), DEFAULT_LOCK_EXPIRE_TIME_MILLIS);
    }

    /**
     * Acquires the lock.
     *
     * If it's available then return;
     * If the lock isn't available the current thread gets blocked until the lock is released
     *
     * 阻塞式获取锁
     */
    public void lock() throws InterruptedException {
        for (;;){
            String ret = rt.set(lockKey, lockValue, SET_IF_NOT_EXIST, SET_WITH_EXPIRE, lockExpireTime);//expire in lockExpireTime
            if (LOCK_SUCCESS.equals(ret)) {
                System.out.println(String.format("Thread: %s acquires lock with value: %s; Time at: %s",
                        Thread.currentThread().getId(), lockValue, LocalTime.now()));
                break;
            }
            System.out.println(String.format("Thread: %s acquire lock failed, sleep 10secs; Time at: %s",
                    Thread.currentThread().getId(), LocalTime.now()));
            Thread.sleep(DEFAULT_SLEEP_TIME);
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
        String ret = rt.set(lockKey, lockValue, SET_IF_NOT_EXIST, SET_WITH_EXPIRE, lockExpireTime);
        long threadId = Thread.currentThread().getId();
        if (LOCK_SUCCESS.equals(ret)) {
            System.out.println(String.format("Thread: %s acquires lock with value %s; Time at: %s",
                    threadId, lockValue, LocalTime.now()));
            return true;
        }
        System.out.println(String.format("Thread: %s acquires lock failed, return; Time at: %s",
                threadId, LocalTime.now()));
        return false;
    }

    /**
     * Similar to tryLock(), except it waits up the given waitTime
     * before giving up trying to acquire the lock
     *
     * 获取锁，如果无法获取，一直尝试直到waitTime结束
     * @param waitTime 尝试获取锁的等待时间
     * @return
     */
    public boolean tryLock(long waitTime) throws InterruptedException {
        long end = System.currentTimeMillis() + waitTime;
        long threadId = Thread.currentThread().getId();
        while (System.currentTimeMillis() < end) {
            String ret = rt.set(lockKey, lockValue, SET_IF_NOT_EXIST, SET_WITH_EXPIRE, lockExpireTime);
            if (LOCK_SUCCESS.equals(ret)) {
                System.out.println(String.format("Thread: %s acquires lock with value %s; Time at: %s",
                        threadId, lockValue, LocalTime.now()));
                return true;
            }
                System.out.println(String.format("Thread: %s acquires lock failed, sleep 100msecs; Time at: %s",
                        threadId, LocalTime.now()));
                Thread.sleep(DEFAULT_SLEEP_TIME);
        }
        System.out.println(String.format("Thread: %s acquires lock failed, timeout; Time at: %s",
                threadId, LocalTime.now()));
        return false;
    }

    /**
     * Release the lock
     *
     * 利用lua脚本保证删除key时的原子性
     */
    public boolean unlock() {
        String checkAndDelScript =
                "if redis.call('get',KEYS[1]) == ARGV[1] then " +
                " return redis.call('del',KEYS[1])" +
                " else" +
                " return 0" +
                " end";
        long threadId = Thread.currentThread().getId();
        System.out.println(String.format("Thread: %s want to release lock with value: %s", threadId, lockValue));
        Object ret = rt.eval(checkAndDelScript, 1, lockKey, lockValue);
        if (UNLOCK_SUCCESS.equals(ret)) {
            System.out.println(String.format("Thread: %s releases lock with success; Time at: %s",
                    threadId, LocalTime.now()));
            return true;
        }
        System.out.println(String.format("Thread: %s releases lock failed; Time at: %s",
                threadId, LocalTime.now()));
        return false;
    }

}
