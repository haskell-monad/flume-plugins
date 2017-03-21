package com.bigdata.flume.utils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

import java.util.Map;

/**
 * redis工具类
 */
final public class RedisUtil {
    private static final Logger logger = LoggerFactory.getLogger(RedisUtil.class);
    private JedisPool pool = null;
    /**
     * <p>传入ip和端口号构建redis 连接池</p>
     * @param ip ip
     * @param port 端口
     */
    public RedisUtil(String ip, int port,int db) {
        if (pool == null) {
            JedisPoolConfig config = new JedisPoolConfig();
            // 控制一个pool可分配多少个jedis实例，通过pool.getResource()来获取；
            // 如果赋值为-1，则表示不限制；如果pool已经分配了maxActive个jedis实例，则此时pool的状态为exhausted(耗尽)。
            config.setMaxTotal(500);
            // 控制一个pool最多有多少个状态为idle(空闲的)的jedis实例。
            config.setMaxIdle(5);
            // 表示当borrow(引入)一个jedis实例时，最大的等待时间，如果超过等待时间，则直接抛出JedisConnectionException；
            config.setMaxWaitMillis(1000 * 100);
            // 在borrow一个jedis实例时，是否提前进行validate操作；如果为true，则得到的jedis实例均是可用的；
            config.setTestOnBorrow(true);
            // pool = new JedisPool(config, "192.168.0.121", 6379, 100000);
            pool = new JedisPool(config, ip, port,100000,null,db);
            logger.info("成功连接到Redis服务器: ip: {},port: {},db: "+db+"",ip,port);
        }
    }

    /**
     * <p>通过配置对象 ip 端口 构建连接池</p>
     * @param config 配置对象
     * @param ip ip
     * @param prot 端口
     */
    public RedisUtil(JedisPoolConfig config, String ip, int prot){
        if (pool == null) {
            pool = new JedisPool(config,ip,prot,10000);
        }
    }

    /**
     * <p>通过配置对象 ip 端口 超时时间 构建连接池</p>
     * @param config 配置对象
     * @param ip ip
     * @param prot 端口
     * @param timeout 超时时间
     */
    public RedisUtil(JedisPoolConfig config, String ip, int prot, int timeout){
        if (pool == null) {
            pool = new JedisPool(config,ip,prot,timeout);
        }
    }

    /**
     * <p>通过连接池对象 构建一个连接池</p>
     * @param pool 连接池对象
     */
    public RedisUtil(JedisPool pool){
        if (this.pool == null) {
            this.pool = pool;
        }
    }

    /**
     * <p>通过key给field设置指定的值,如果key不存在,则先创建</p>
     * @param key
     * @param field 字段
     * @param value
     * @return 如果存在返回0 异常返回null
     */
    public Long hset(String key,String field,String value) {
        Jedis jedis = null;
        Long res = null;
        try {
            jedis = pool.getResource();
            res = jedis.hset(key, field, value);
        } catch (Exception e) {
            pool.returnBrokenResource(jedis);
            e.printStackTrace();
        } finally {
            returnResource(pool, jedis);
        }
        return res;
    }

    /**
     * <p>通过key同时设置 hash的多个field</p>
     * @param key
     * @param hash
     * @return 返回OK 异常返回null
     */
    public String hmset(String key,Map<String, String> hash){
        Jedis jedis = null;
        String res = null;
        try {
            jedis = pool.getResource();
            res = jedis.hmset(key, hash);
        } catch (Exception e) {
            pool.returnBrokenResource(jedis);
            e.printStackTrace();
        } finally {
            returnResource(pool, jedis);
        }
        return res;
    }

    /**
     * <p>通过key 和 field 获取指定的 value</p>
     * @param key
     * @param field
     * @return 没有返回null
     */
    public String hget(String key, String field){
        Jedis jedis = null;
        String res = null;
        try {
            jedis = pool.getResource();
            res = jedis.hget(key, field);
        } catch (Exception e) {
            pool.returnBrokenResource(jedis);
            e.printStackTrace();
        } finally {
            returnResource(pool, jedis);
        }
        return res;
    }

    /**
     * 返还到连接池
     * @param pool
     * @param jedis
     */
    public static void returnResource(JedisPool pool, Jedis jedis) {
        if (jedis != null) {
            pool.returnResource(jedis);
        }
    }
}
