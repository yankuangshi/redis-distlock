package com.github.kyan;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

/**
 * A customized jedis wrapper using jedis pool connection
 *
 * <code>
 *     public void foobar(){
 *         Jedis jedis = null;
 *         try {
 *             jedis = pool.getResource();  //borrow jedis from pool
 *             // ... do stuff here ... for example
 *             jedis.set("foo", "bar");
 *         } catch(Exception e) {
 *             logger.error(e.getMessage());
 *         } finally {
 *             if (jedis != null) {         //release jedis back to pool
 *                 jedis.close();
 *             }
 *         }
 *     }
 * </code>
 */
public class RedisTool {

    private static final Logger logger = LoggerFactory.getLogger(RedisTool.class);

    private JedisPool pool;

    //constructor initiate jedis pool
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

    public String get(final String key) {
        Jedis jedis = null;
        String ret = null;
        try {
            jedis = pool.getResource();
            ret = jedis.get(key);
        } catch (Exception ex) {
            logger.error(ex.getMessage());
        } finally {
            if (jedis != null) {
                jedis.close();
            }
        }
        return ret;
    }

    /**
     * OK if SET was executed correctly, otherwise "0" will be returned
     */
    public String set(final String key, String value) {
        Jedis jedis = null;
        try {
            jedis = pool.getResource();
            return jedis.set(key, value);
        } catch (Exception ex) {
            logger.error(ex.getMessage());
            return "0";
        } finally {
            if (jedis != null) {
                jedis.close();
            }
        }
    }

    /**
     * since redis 2.6.12 support "set key value nx|xx ex|px time"
     * OK if SET was executed correctly, otherwise "0" will be returned
     */
    public String set(final String key, final String value, final String nxxx, final String expx,
                      final long time) {
        Jedis jedis = null;
        try {
            jedis = pool.getResource();
            return jedis.set(key, value, nxxx, expx, time);
        } catch (Exception ex) {
            logger.error(ex.getMessage());
            return "0";
        } finally {
            if (jedis != null) {
                jedis.close();
            }
        }
    }

    public Long ttl(final String key) {
        Jedis jedis = null;
        Long ret = null;
        try {
            jedis = pool.getResource();
            ret = jedis.ttl(key);
        } catch (Exception ex) {
            logger.error(ex.getMessage());
        } finally {
            if (jedis != null) {
                jedis.close();
            }
        }
        return ret;
    }

    //eval lua script
    public Object eval(String script, int keyCount, String... params) {
        Jedis jedis = null;
        Object ret = null;
        try {
            jedis = pool.getResource();
            ret = jedis.eval(script, keyCount, params);
        } catch (Exception ex) {
            logger.error(ex.getMessage());
        } finally {
            if (jedis != null) {
                jedis.close();
            }
        }
        return ret;
    }

}
