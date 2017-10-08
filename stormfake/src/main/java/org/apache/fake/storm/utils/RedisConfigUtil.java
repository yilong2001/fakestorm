package org.apache.fake.storm.utils;

import java.util.Map;

/**
 * Created by yilong on 2017/7/5.
 */
public class RedisConfigUtil {
    public static String getHost(Map<String, Object> conf) {
        return "localhost";
    }

    public static int getPort(Map<String, Object> conf) {
        return 6379;
    }
}
