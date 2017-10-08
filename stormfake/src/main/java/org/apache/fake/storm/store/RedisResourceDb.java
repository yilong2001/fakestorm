package org.apache.fake.storm.store;

import org.apache.fake.storm.utils.RedisConfigUtil;
import redis.clients.jedis.*;
import redis.clients.jedis.Protocol.Command;
import redis.clients.jedis.exceptions.JedisConnectionException;

import java.util.*;

/**
 * Created by yilong on 2017/7/5.
 */
public class RedisResourceDb {
    RedisClient client = null;
    public RedisResourceDb(Map<String, Object> conf) {
        client = RedisClient.getRedisClient(conf);
    }

    public boolean acquireOptimisticLock(String key, String val) {
        //String val = String.valueOf(System.currentTimeMillis());
        Jedis jedis = client.getJedis();
        long setnx = jedis.setnx(key, val);
        client.close(jedis);

        return (setnx > 0);
    }

    public boolean acquireOptimisticLockWithExpire(String key, String val, int seconds) {
        Jedis jedis = client.getJedis();
        String out = jedis.set(key, val, "NX", "EX", seconds);
        client.close(jedis);

        if (out != null && out.equals("OK")) {
            return true;
        }

        return false;
    }

    public boolean setValue(String key, String val) {
        Jedis jedis = client.getJedis();
        String result = jedis.set(key, val);
        client.close(jedis);

        return result != null && result.equals("OK");
    }

    public String getValue(String key) {
        Jedis jedis = client.getJedis();
        String out = jedis.get(key);
        client.close(jedis);

        return out;
    }

    public boolean setMapValue(String key, String mkey, String mval) {
        Jedis jedis = client.getJedis();
        Map<String,String> m = new HashMap<>();
        m.put(mkey, mval);
        String result = jedis.hmset(key, m);
        client.close(jedis);

        return result != null && result.equals("OK");
    }

    public Map<String, String> getMapAllValue(String key) {
        Jedis jedis = client.getJedis();
        Map<String, String> out = jedis.hgetAll(key);
        client.close(jedis);

        return out;
    }

    public String getMapValue(String key, String field) {
        Jedis jedis = client.getJedis();
        String out = jedis.hget(key, field);
        client.close(jedis);

        return out;
    }

    public boolean removeMapField(String key, String field) {
        Jedis jedis = client.getJedis();
        long res = jedis.hdel(key, field);
        client.close(jedis);

        return (res > 0);
    }

    public boolean setListValue(String key, String val) {
        Jedis jedis = client.getJedis();
        long result = jedis.lpush(key, val);
        client.close(jedis);

        return (result > 0);
    }

    public Set<String> getSetAllValue(String key) {
        Jedis jedis = client.getJedis();
        Set<String> out = jedis.smembers(key);
        client.close(jedis);

        return out;
    }

    public boolean setSetValue(String key, String val) {
        Jedis jedis = client.getJedis();
        long result = jedis.sadd(key, val);
        client.close(jedis);

        return (result > 0);
    }

    public boolean removeSetValue(String key, String val) {
        Jedis jedis = client.getJedis();
        long result = jedis.srem(key, val);
        client.close(jedis);

        return (result > 0);
    }

    public List<String> getListAllValue(String key) {
        Jedis jedis = client.getJedis();
        List<String> out = jedis.lrange(key, 0, -1);
        client.close(jedis);

        return out;
    }

    public List<String> getListValueByPattern(String pattern) {
        Jedis jedis = client.getJedis();

        List<String> out = new ArrayList<>();
        Set<String> keys = jedis.keys(pattern);
        for(String key : keys) {
            String tmp = jedis.get(key);
            if (tmp != null) {
                out.add(tmp);
            }
        }

        client.close(jedis);

        return out;
    }

    public String getListValue(String key, int index) {
        Jedis jedis = client.getJedis();

        String out = jedis.lindex(key, index);
        client.close(jedis);

        return out;
    }

    public boolean removeListValue(String key, String val) {
        Jedis jedis = client.getJedis();

        long res = jedis.lrem(key, 0, val);
        client.close(jedis);

        return (res > 0);
    }

    public boolean removeKey(String key) {
        Jedis jedis = client.getJedis();
        long res = jedis.del(key);
        client.close(jedis);

        return (res > 0);
    }
}
