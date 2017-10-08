package org.apache.fake.storm.supervisor.tasks;

import org.apache.fake.storm.model.SupervisorInfo;
import org.apache.fake.storm.supervisor.manager.ISupervisor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by yilong on 2017/7/8.
 */
public class LocalCacheUpdateTask implements Runnable, AutoCloseable {
    public static final Logger LOG = LoggerFactory.getLogger(LocalCacheUpdateTask.class);
    private ISupervisor iSupervisor;

    public LocalCacheUpdateTask(ISupervisor iSupervisor) {
        this.iSupervisor = iSupervisor;
    }

    @Override
    public void run() {
        //TODO: download files
        SupervisorInfo str = this.iSupervisor.getSupervisorInfo();
        try {
            //TODO: TIMER-LOG
            //LOG.info(Thread.currentThread().getName()+" : "+str.serialiaze());
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Override
    public void close() throws Exception {

    }
}
