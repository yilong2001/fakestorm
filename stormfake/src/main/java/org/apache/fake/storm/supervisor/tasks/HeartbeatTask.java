package org.apache.fake.storm.supervisor.tasks;

import org.apache.fake.storm.supervisor.manager.ISupervisor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by yilong on 2017/7/8.
 */
public class HeartbeatTask implements Runnable, AutoCloseable {
    public static final Logger LOG = LoggerFactory.getLogger(HeartbeatTask.class);

    private ISupervisor iSupervisor;
    public HeartbeatTask(ISupervisor iSupervisor) {
        this.iSupervisor = iSupervisor;
        try {
            this.iSupervisor.getResourceStore().registerSupervisor(this.iSupervisor.getStormId(),
                    this.iSupervisor.getSupervisorId(),
                    10*1000,
                    this.iSupervisor.getSupervisorInfo());
        } catch (Exception e) {
            LOG.error(e.getMessage(), e);
            throw new RuntimeException(e.getMessage());
        }
    }

    @Override
    public void run() {
        try {
            this.iSupervisor.getResourceStore().updateSupervisor(this.iSupervisor.getStormId(),
                    this.iSupervisor.getSupervisorId(),
                    this.iSupervisor.getSupervisorInfo());
        } catch (Exception e) {
            LOG.error(e.getMessage(), e);
        }
    }

    @Override
    public void close() throws Exception {

    }
}
