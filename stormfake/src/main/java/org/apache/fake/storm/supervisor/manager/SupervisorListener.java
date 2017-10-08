package org.apache.fake.storm.supervisor.manager;

import org.apache.fake.storm.utils.QueueElement;
import org.apache.fake.storm.utils.QueuedTimerConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.JedisPubSub;

import java.util.concurrent.ConcurrentHashMap;

/**
 * Created by yilong on 2017/7/7.
 */
public class SupervisorListener extends JedisPubSub {
    public static final Logger LOG = LoggerFactory.getLogger(SupervisorListener.class);

    private QueuedTimerConsumer<QueueElement> task = null;
    private ConcurrentHashMap<String, Runnable> channelCallbacks;

    public SupervisorListener(ISupervisor iSupervisor) {
        super();
        this.channelCallbacks = new ConcurrentHashMap();

        this.task = new QueuedTimerConsumer();

        this.task.setName(iSupervisor.getSupervisorId());
        this.task.setActive(true);
        this.task.setDaemon(true);
        this.task.start();
    }

    public SupervisorListener addChannelCallback(String channel, Runnable cb) {
        this.channelCallbacks.put(channel, cb);
        return this;
    }

    public void onMessage(String channel, String message) {

    }

    public void onPMessage(String pattern, String channel, String message) {

    }
}
