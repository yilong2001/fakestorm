package org.apache.fake.storm.supervisor.worker;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.apache.fake.storm.supervisor.manager.ISupervisor;
import org.apache.fake.storm.supervisor.tasks.LocalCacheUpdateTask;
import org.apache.fake.storm.utils.EventConsumerRunnableWrapper;
import org.apache.fake.storm.utils.QueuedTimer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * Created by yilong on 2017/7/8.
 */
public class LocalCache {
    public static final Logger LOG = LoggerFactory.getLogger(LocalCache.class);

    private ScheduledExecutorService cacheCleanService;
    private QueuedTimer localCacheUpdateTimer;
    private ISupervisor iSupervisor;
    private LocalCacheUpdateTask cacheUpdateTask;

    public LocalCache(Map<String, Object> conf, ISupervisor iSupervisor) {
        this.iSupervisor = iSupervisor;
        localCacheUpdateTimer = new QueuedTimer(iSupervisor.getSupervisorId()+"_localcache");
        cacheUpdateTask = new LocalCacheUpdateTask(iSupervisor);
    }

    public void start() {
        localCacheUpdateTimer.schedule(1000,
                new EventConsumerRunnableWrapper<Runnable>(this.iSupervisor.getEventConsumer(), cacheUpdateTask));

        cacheCleanService = new ScheduledThreadPoolExecutor(1,
                new ThreadFactoryBuilder()
                        .setNameFormat("Localizer Cache Cleanup")
                        .build());

        cacheCleanService.scheduleWithFixedDelay(new Runnable() {
            @Override
            public void run() {
                cleanCache();
            }
        }, 1000, 2000, TimeUnit.MILLISECONDS);
    }

    public void cleanCache() {
        //TODO: TIMER-LOG
        //LOG.info(Thread.currentThread().getName()+" : clean cache!");
    }

    public void stop() {
        cacheCleanService.shutdown();
        try {
            cacheUpdateTask.close();
            localCacheUpdateTimer.close();
        } catch (Exception e) {
            LOG.error(e.getMessage(), e);
        }
    }
}
