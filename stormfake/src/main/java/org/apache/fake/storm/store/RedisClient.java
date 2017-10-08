package org.apache.fake.storm.store;

import org.apache.fake.storm.utils.RedisConfigUtil;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

import java.util.Map;

/**
 * Created by yilong on 2017/7/5.
 */
public class RedisClient {
    private JedisPool jedisPool;//非切片连接池

    private static RedisClient redisClient = null;

    private RedisClient(Map<String, Object> conf) {
        initialPool(conf);
    }

    public static RedisClient getRedisClient(Map<String, Object> conf){
        if(redisClient==null){
            synchronized (RedisResourceDb.class) {
                if(redisClient==null) redisClient = new RedisClient(conf);
            }
        }

        return redisClient;
    }

    public Jedis getJedis() {
        return jedisPool.getResource();
    }

    public void close(Jedis jedis) {
        jedisPool.returnResource(jedis);
    }

    /**
     * 初始化非切片池
     */
    private void initialPool(Map<String, Object> conf) {
        // 池基本配置
        JedisPoolConfig config = new JedisPoolConfig();
        config.setMaxTotal(20);
        config.setMaxIdle(5);
        config.setMaxWaitMillis(1000l);
        config.setTestOnBorrow(false);

        jedisPool = new JedisPool(config, RedisConfigUtil.getHost(conf),RedisConfigUtil.getPort(conf));
    }

    public void Close() {
        jedisPool.close();
    }

}
