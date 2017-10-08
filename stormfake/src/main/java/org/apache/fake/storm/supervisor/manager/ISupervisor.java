package org.apache.fake.storm.supervisor.manager;

import org.apache.fake.storm.model.SupervisorInfo;
import org.apache.fake.storm.store.IResourceStore;
import org.apache.fake.storm.utils.IEventConsumer;

import java.util.Map;

/**
 * Created by yilong on 2017/7/8.
 */
public interface ISupervisor {
    public String getStormId();
    public String getSupervisorHost();

    public String getSupervisorId();
    public SupervisorInfo getSupervisorInfo();
    public IEventConsumer<Runnable> getEventConsumer();
    public IResourceStore getResourceStore();
    public Map<String, Object> getConf();
    public String getWorkerIdByPort(int port);
    public String getHostIp();
    //public
}
