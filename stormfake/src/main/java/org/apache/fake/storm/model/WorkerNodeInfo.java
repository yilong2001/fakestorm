package org.apache.fake.storm.model;

import org.apache.fake.storm.utils.JsonSerializable;
import sun.security.provider.MD5;

import java.io.Serializable;

/**
 * Created by yilong on 2017/8/27.
 */
public class WorkerNodeInfo extends JsonSerializable<WorkerNodeInfo> implements Serializable {
    private String supervisorId;
    private String hostIp;
    private int port;
    public WorkerNodeInfo(String supervisorId, String hostIp, int port) {
        this.supervisorId = supervisorId;
        this.hostIp = hostIp;
        this.port = port;
    }

    public String getSupervisorId() {
        return supervisorId;
    }
    public void setSupervisorId(String supervisorId) {
        this.supervisorId = supervisorId;
    }

    public String getHostIp() { return hostIp; }
    public void setHostIp(String ip) { hostIp = ip; }

    public int getPort() {
        return port;
    }
    public void setPort(int port) {
        this.port = port;
    }

    public String getWorkerId() {
        //TODO:
        return "12345678";
        //return supervisorId+":"+port;
    }
}
