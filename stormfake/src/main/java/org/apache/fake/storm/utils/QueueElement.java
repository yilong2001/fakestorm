package org.apache.fake.storm.utils;

/**
 * Created by yilong on 2017/7/7.
 */
public class QueueElement {
    public final Long expiredMs;
    public final Runnable call;
    public final Integer priority;
    public final String id;

    public QueueElement(String id, Long expiredMs, Runnable call, Integer priority) {
        this.id = id;
        this.expiredMs = expiredMs;
        this.call = call;
        this.priority = priority;
    }
}
