package com.kyan.rdl;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

public class RedisTool {

    private static final Logger logger = LoggerFactory.getLogger(RedisTool.class);

    private static JedisPool pool;

    //初始化
    public RedisTool() {
        if (pool == null) {
            String host = "127.0.0.1";
            int port = 6379;
            JedisPoolConfig config = new JedisPoolConfig();
            //最大空闲连接数
            config.setMaxIdle(200);
            config.setMaxTotal(300);
            config.setTestOnBorrow(false);//TestOnBorrow - Sends a PING request when you ask for the resource
            config.setTestOnReturn(false);//TestOnReturn - Sends a PING when you return a resource to the pool
            pool = new JedisPool(config, host, port, 10000);
        }
    }

    private Jedis getJedis() {
        return pool.getResource();
    }

    public String get(final String key) {
        Jedis jedis = null;
        String value = null;
        try {
            jedis = getJedis();
            value = jedis.get(key);
        } catch (Exception ex) {
            logger.error(ex.getMessage());
        } finally {
            jedis.close();
        }
        return value;
    }

    public String set(final String key, String value) {
        Jedis jedis = null;
        try {
            jedis = getJedis();
            return jedis.set(key, value);
        } catch (Exception ex) {
            logger.error(ex.getMessage());
            return "0";
        } finally {
            jedis.close();
        }
    }

    public String set(final String key, final String value, final String nxxx, final String expx,
                      final long time) {
        Jedis jedis = null;
        try {
            jedis = getJedis();
            return jedis.set(key, value, nxxx, expx, time);
        } catch (Exception ex) {
            logger.error(ex.getMessage());
            return "0";
        } finally {
            jedis.close();
        }
    }

    public Object eval(String script, int keyCount, String... params) {
        Jedis jedis = null;
        try {
            jedis = getJedis();
            return jedis.eval(script, keyCount, params);
        } catch (Exception ex) {
            logger.error(ex.getMessage());
            return null;
        } finally {
            jedis.close();
        }
    }

    public static void main(String[] args) {

    }
}
