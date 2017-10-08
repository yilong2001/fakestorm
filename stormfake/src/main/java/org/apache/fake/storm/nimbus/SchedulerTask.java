package org.apache.fake.storm.nimbus;

import org.apache.fake.storm.model.WorkerAssignmentInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/**
 * Created by yilong on 2017/10/5.
 */
public class SchedulerTask implements Runnable, AutoCloseable {
    public static final Logger LOG = LoggerFactory.getLogger(SchedulerTask.class);
    private IScheduler iScheduler;
    public SchedulerTask(IScheduler iScheduler) {
        this.iScheduler = iScheduler;
    }

    @Override
    public void run() {
        try {
            LOG.info(" ********* nimbus scheduler task ...  *********  ");
            List<WorkerAssignmentInfo> assignmentInfos = iScheduler.schedule();
            if (assignmentInfos == null) {
                return;
            }

            for (WorkerAssignmentInfo info : assignmentInfos) {
                boolean res = iScheduler.getResourceStore().updateAssigment(iScheduler.getStormId(), info.getTopoId(), info);
                LOG.info(" ********* nimbus scheduler task updateAssigment : "+res);
                res = iScheduler.getResourceStore().updateAssignmentVersion(iScheduler.getStormId(), info.getTopoId(), info.getVersion());
                LOG.info(" ********* nimbus scheduler task updateAssignmentVersion : "+res);
            }
        } catch (Exception e) {
            LOG.error(e.getMessage(), e);
        }
    }

    @Override
    public void close() throws Exception {
        //TODO:
    }
}
