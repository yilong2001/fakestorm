package org.apache.fake.storm.supervisor.manager;

import org.apache.fake.storm.supervisor.tasks.HeartbeatTask;
import org.apache.fake.storm.supervisor.tasks.StatusManager;
import org.apache.fake.storm.supervisor.worker.LocalCache;
import org.apache.fake.storm.utils.*;

import java.util.Map;

/**
 * Created by yilong on 2017/7/7.
 * assign -> download -> worker
 */
public class SupervisorServer {

    private final String supervisorId;
    private final IEventConsumer<Runnable> eventConsumer;
    private QueuedTimer heartbeatTimer;

    private final StatusManager statusManager;
    private final QueuedTimer statusTimer;
    private final ISupervisor supervisor;

    private final LocalCache localCache;
    private final Map<String, Object> conf;

    private final int TimerPeriod = 10*1000;
    private final String supervisorHost;

    public SupervisorServer(Map<String, Object> conf, String supervisorHost, String supervisorId) {
        //TODO: supervisorId should get from localstore
        this.conf = conf;
        this.supervisorHost = supervisorHost;

        this.supervisorId = supervisorId;

        supervisor = SupervisorFactory.makeSupervisor(conf, supervisorId, supervisorHost);

        eventConsumer = new QueuedEventConsumer();

        statusTimer = new QueuedTimer(supervisorId+"_assign");
        heartbeatTimer = new QueuedTimer(supervisorId+"_heartbeat");

        statusManager = new StatusManager(conf, supervisor, supervisorHost);

        localCache = new LocalCache(conf, supervisor);
    }

    public String getSupervisorId() {
        return supervisorId;
    }

    public IEventConsumer<Runnable> getEventConsumer() {
        return this.eventConsumer;
    }

    public void start() {
        heartbeatTimer.schedule(TimerPeriod, new HeartbeatTask(supervisor));

        statusTimer.schedule(TimerPeriod, new EventConsumerRunnableWrapper<Runnable>(eventConsumer, statusManager));

        //localCache.start();

        eventConsumer.start();
    }

    public void stop() {
        //TODO:
        try {
            eventConsumer.setActive(false);
            heartbeatTimer.close();
            statusTimer.close();
            //localCache.stop();
        } catch (Exception e) {

        }
    }
}
