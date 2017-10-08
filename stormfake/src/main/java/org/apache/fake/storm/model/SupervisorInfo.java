package org.apache.fake.storm.model;

import org.apache.fake.storm.utils.JsonSerializable;

import java.io.Serializable;

/**
 * Created by yilong on 2017/7/8.
 */
public class SupervisorInfo extends JsonSerializable<SupervisorInfo> implements Serializable {
    private String supervisorId;
    private String hostIp;
    private int totalMemoryKB;
    private int totalCpuQurt; // 1 core = 100
    private int usedMemoryKB;

    private int usedCpuQurt;  // 1 core = 100
    //TODO: how to monitor worker?
    public SupervisorInfo(String supervisorId, String hostIp, int tmem, int tcpu) {
        this.supervisorId = supervisorId;
        this.hostIp = hostIp;
        totalMemoryKB = tmem;
        totalCpuQurt = tcpu;
        usedMemoryKB = 0;
        usedCpuQurt = 0;
    }

    public String getSupervisorId() { return supervisorId; }
    public void setSupervisorId(String si) { supervisorId = si; }

    public String getHostIp() { return hostIp; }
    public void setHostIp(String ip) { hostIp = ip; }

    public int getTotalMemoryKB() {
        return totalMemoryKB;
    }

    public void setTotalMemoryKB(int totalMemoryKB) {
        this.totalMemoryKB = totalMemoryKB;
    }

    public int getTotalCpuQurt() {
        return totalCpuQurt;
    }

    public void setTotalCpuQurt(int totalCpuQurt) {
        this.totalCpuQurt = totalCpuQurt;
    }

    public int getUsedMemoryKB() {
        return usedMemoryKB;
    }

    public void setUsedMemoryKB(int usedMemoryKB) {
        this.usedMemoryKB = usedMemoryKB;
    }

    public int getUsedCpuQurt() {
        return usedCpuQurt;
    }

    public void setUsedCpuQurt(int usedCpuQurt) {
        this.usedCpuQurt = usedCpuQurt;
    }
}
