package com.kyan.rdl;

import java.time.LocalTime;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

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

    private static final String NXXX = "NX";
    private static final String EXPX = "PX";
    private static final String SUCCESS = "OK";
    private RedisTool rt;
    private String lockKey;
    private String lockValue;

    public RedisDistLock(String lockKey, String lockValue) {
        this.rt = new RedisTool();
        this.lockKey = lockKey;
        this.lockValue = lockValue;
    }

    public RedisDistLock(String lockKey) {
        this(lockKey, UUID.randomUUID().toString() + Thread.currentThread().getId());
    }

    /**
     * 阻塞式获取锁，获取不到锁一直等待
     */
    public void lock() {
        while (true) {
            String ret = rt.set(lockKey, lockValue, NXXX, EXPX, 30000);//expire in 30sec
            if (SUCCESS.equals(ret)) {
                System.out.println(String.format("Thread: %s acquire lock success Time: %s",
                        Thread.currentThread().getName(), LocalTime.now()));
                break;
            }
            System.out.println(String.format("Thread: %s acquire lock failed, sleep 10sec Time: %s",
                    Thread.currentThread().getName(), LocalTime.now()));
            sleep(10);
        }
    }

    private void sleep(int sec) {
        try {
            Thread.sleep(sec * 1000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    /**
     * 获取锁，并指定timeout时间，如果timeout时间到了仍然无法获得，则返回
     * @return
     */
    public boolean tryLock() {
        return false;
    }

    /**
     * 获取锁，并且设置一个获取锁的timeout时间
     * @param timeout
     * @param unit
     * @return
     */
    public boolean tryLock(long timeout, TimeUnit unit) {
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
    }

    public static void main(String[] args) {
        RedisDistLock redisDistLock = new RedisDistLock("lock");
        Thread threaA = new Thread("A") {
            @Override
            public void run() {
                redisDistLock.lock();
            }
        };
        Thread threaB = new Thread("B") {
            @Override
            public void run() {
                redisDistLock.lock();
            }
        };
        threaA.start();
        threaB.start();
    }

}
