package com.github.kyan;

import org.junit.Before;
import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class RedisLockTest {

    private final String LOCK_KEY = "foobar";
    private RedisTool rt;

    @Before
    public void setBefore() {
        rt = new RedisTool();
    }

    /**
     * Should lock and then lease lock
     * @throws InterruptedException
     */
    @Test
    public void testLockUnlock() throws InterruptedException {
        RedisLock redisLock = new RedisLock(LOCK_KEY);
        redisLock.lock();
        try {
            Thread.sleep(1000);//sleep 1sec
        } catch (InterruptedException e) {
        }
        Long ttl = rt.ttl(LOCK_KEY);
        System.out.println(String.format("key: %s, ttl: %s", LOCK_KEY, ttl));
        assertThat(ttl).isBetween(0L, 3L);
        redisLock.unlock();
        ttl = rt.ttl(LOCK_KEY).longValue();
        System.out.println(String.format("key: %s, ttl: %s", LOCK_KEY, ttl));
        assertThat(ttl).isEqualTo(-2);
    }

    /**
     * Should return false when one use non-blocking tryLock and the other one has acquired the lock
     *
     * @throws InterruptedException
     */
    @Test
    public void testTryLockFail() throws InterruptedException {
        Thread t = new Thread(() -> {
            try {
                RedisLock lock1 = new RedisLock(LOCK_KEY);
                lock1.lock();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        });
        t.start();
        t.join();
        RedisLock lock2 = new RedisLock(LOCK_KEY);
        assertThat(lock2.tryLock()).isEqualTo(false);
    }

    /**
     * Should wait when one use blocking tryLock and the other one has acquired the lock
     *
     * @throws InterruptedException
     */
    @Test
    public void testTryLockWait() throws InterruptedException {
        Thread t = new Thread(() -> {
            try {
                RedisLock lock1 = new RedisLock(LOCK_KEY);
                lock1.lock();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        });
        t.start();
        t.join();
        RedisLock lock2 = new RedisLock(LOCK_KEY);
        long startTime = System.currentTimeMillis();
        lock2.tryLock(2000);   //wait for 2sec
        assertThat(System.currentTimeMillis() - startTime).isBetween(1900L, 2100L);
    }

    /**
     * Should wait when one use blocking lock and the other one has acquired the lock
     * @throws InterruptedException
     */
    @Test
    public void testBlockingLock() throws InterruptedException {
        Thread t = new Thread(() -> {
            try {
                RedisLock lock1 = new RedisLock(LOCK_KEY);
                lock1.lock();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        });
        t.start();
        t.join();
        RedisLock lock2 = new RedisLock(LOCK_KEY);
        long startTime = System.currentTimeMillis();
        lock2.lock();
        assertThat(System.currentTimeMillis() - startTime).isBetween(2900L, 3100L);
    }

    /**
     * Should return true when unlock one's own lock
     *
     * @throws InterruptedException
     */
    @Test
    public void testUnlockOwnLockSuccess() throws InterruptedException {
        RedisLock lock = new RedisLock(LOCK_KEY);
        lock.lock();
        assertTrue(lock.unlock());
    }

    /**
     * Should return false when unlock one's own lock but expired
     *
     * @throws InterruptedException
     */
    @Test
    public void testUnlockExpiredLockFail() throws InterruptedException {
        RedisLock lock = new RedisLock(LOCK_KEY);
        lock.lock();
        Thread.sleep(5000);
        assertFalse(lock.unlock());
    }

    /**
     * Should return false when lease other one's lock
     *
     * @throws InterruptedException
     */
    @Test
    public void testUnlockOthersLockFail() throws InterruptedException {
        Thread t = new Thread(() -> {
            try {
                RedisLock lock1 = new RedisLock(LOCK_KEY);
                lock1.lock();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        });
        t.start();
        t.join();
        RedisLock lock2 = new RedisLock(LOCK_KEY);
        assertFalse(lock2.unlock());
    }


}


