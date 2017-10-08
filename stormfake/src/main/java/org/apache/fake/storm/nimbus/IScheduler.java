package org.apache.fake.storm.nimbus;

import org.apache.fake.storm.model.WorkerAssignmentInfo;
import org.apache.fake.storm.store.IResourceStore;

import java.util.List;

/**
 * Created by yilong on 2017/10/5.
 */
public interface IScheduler {
    public List<WorkerAssignmentInfo> schedule() throws Exception;
    public IResourceStore getResourceStore();
    public String getStormId();
}
