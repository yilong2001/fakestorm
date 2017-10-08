package org.apache.fake.storm.model;

import org.apache.fake.storm.utils.JsonSerializable;

import java.io.Serializable;

/**
 * Created by yilong on 2017/8/26.
 */
public class ResourceInfo  extends JsonSerializable<ResourceInfo> implements Serializable {
    private int cpuRatio;
    private int memMegas;

    public ResourceInfo(int cpu, int mem) {
        this.cpuRatio = cpu;
        this.memMegas = mem;
    }

    public int getCpuRatio() {
        return cpuRatio;
    }

    public void setCpuRatio(int cpuRatio) {
        this.cpuRatio = cpuRatio;
    }

    public int getMemMegas() {
        return memMegas;
    }

    public void setMemMegas(int memMegas) {
        this.memMegas = memMegas;
    }
}
