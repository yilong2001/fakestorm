package org.apache.fake.storm.utils;

import org.apache.storm.utils.RotatingMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

/**
 * Created by yilong on 2017/6/29.
 */
public class TimeoutCleanMap<K,V> {
    public static final Logger LOG = LoggerFactory.getLogger(TimeoutCleanMap.class);

    private RotatingMap<K,V> rotatingMap;
    private Object lock;
    private long timeoutPeriodMs;
    private Thread cleanThread;

    public TimeoutCleanMap(int timeoutPeriodMs) {
        this(timeoutPeriodMs, 3, null);
    }

    public TimeoutCleanMap(int timeoutPeriodMs, int buckets) {
        this(timeoutPeriodMs, buckets, null);
    }

    public TimeoutCleanMap(final int timeoutPeriodMs, int buckets, RotatingMap.ExpiredCallback<K,V> callback) {
        rotatingMap = new RotatingMap<K, V>(buckets, callback);
        lock = new Object();
        this.timeoutPeriodMs = timeoutPeriodMs;
        cleanThread = new Thread(new Runnable() {
            @Override
            public void run() {
                try {
                    while(true) {
                        Thread.sleep(timeoutPeriodMs);
                        synchronized (lock) {
                            Map<K,V> out = rotatingMap.rotate();
                            for(Map.Entry<K,V> entry : out.entrySet()) {
                                LOG.info(entry.getKey().toString() + " is timeout!");
                            }
                        }
                    }
                } catch (InterruptedException e) {
                    LOG.error(e.getMessage(), e);
                }
            }
        });
        cleanThread.setDaemon(true);
        cleanThread.start();
    }

    public boolean containsKey(K key) {
        synchronized(lock) {
            return rotatingMap.containsKey(key);
        }
    }

    public V get(K key) {
        synchronized(lock) {
            if (containsKey(key)) {
                return rotatingMap.get(key);
            }
            return null;
        }
    }

    public void put(K key, V value) {
        synchronized(lock) {
            rotatingMap.put(key, value);
        }
    }

    public Object remove(K key) {
        synchronized(lock) {
            return rotatingMap.remove(key);
        }
    }

    public int size() {
        synchronized(lock) {
            return rotatingMap.size();
        }
    }

    public void stop() {
        cleanThread.interrupt();
    }
}
