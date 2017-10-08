package org.apache.fake.storm.supervisor.manager;

import org.apache.fake.storm.model.SupervisorInfo;
import org.apache.fake.storm.store.IResourceStore;
import org.apache.fake.storm.store.RedisStore;
import org.apache.fake.storm.utils.ClusterConfigUtil;
import org.apache.fake.storm.utils.IEventConsumer;
import org.apache.fake.storm.utils.QueuedEventConsumer;

import java.util.Map;

/**
 * Created by yilong on 2017/7/8.
 */
public class DefaultSupervisor implements ISupervisor {
    private final String supervisorId;
    private final String stormId;
    private final IResourceStore resourceStore;
    private final IEventConsumer<Runnable> eventConsumer;
    private final SupervisorInfo supervisorInfo;
    private final Map<String, Object> conf;
    private final String hostIp;

    public DefaultSupervisor(Map<String, Object> conf, String supervisorId, String hostIp) {
        this.supervisorId = supervisorId;
        this.stormId = ClusterConfigUtil.getStormId(conf);
        this.resourceStore = new RedisStore(conf);

        this.eventConsumer = new QueuedEventConsumer<Runnable>();
        this.eventConsumer.start();

        //TODO: get memory and cpu
        //TODO: get host ip address
        this.hostIp = hostIp;
        this.supervisorInfo = new SupervisorInfo(supervisorId, hostIp,1000, 50);
        this.conf = conf;
    }

    @Override
    public String getStormId() {
        return stormId;
    }

    @Override
    public String getSupervisorId() {
        return supervisorId;
    }

    @Override
    public String getSupervisorHost() {
        return hostIp;
    }

    @Override
    public SupervisorInfo getSupervisorInfo() {
        return this.supervisorInfo;
    }

    @Override
    public IEventConsumer<Runnable> getEventConsumer() {
        return this.eventConsumer;
    }

    @Override
    public IResourceStore getResourceStore() {
        return this.resourceStore;
    }

    @Override
    public final Map<String,Object> getConf() {
        return this.conf;
    }

    @Override
    public String getWorkerIdByPort(int port) {
        return stormId+"_"+supervisorId+"_"+port;
    }

    @Override
    public String getHostIp() { return hostIp; }
}
