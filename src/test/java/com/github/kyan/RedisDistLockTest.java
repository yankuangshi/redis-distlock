package com.github.kyan;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class RedisDistLockTest {

    protected RedisTool rt;

    @Before
    public void before() {
        rt = new RedisTool();
    }

    @Test
    public void testLockUnlock() {
        String lockKey = "lock1";
        RedisDistLock lock = new RedisDistLock(lockKey);
        lock.lock(10000);
        try {
            Thread.sleep(1000);//睡1秒
        } catch (InterruptedException e) {
        }
        Long ttl = rt.ttl(lockKey);
        System.out.println(String.format("key: %s, ttl: %s", lockKey, ttl));
        assertThat(ttl).isBetween(0L, 10L);
        lock.unlock();
        ttl = rt.ttl(lockKey).longValue();
        System.out.println(String.format("key: %s, ttl: %s", lockKey, ttl));
        assertThat(ttl).isEqualTo(-2);
    }



}
