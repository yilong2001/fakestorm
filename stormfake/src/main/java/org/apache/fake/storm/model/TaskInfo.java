package org.apache.fake.storm.model;

import org.apache.fake.storm.utils.JsonSerializable;

import java.io.Serializable;

/**
 * Created by yilong on 2017/8/26.
 */
public class TaskInfo extends JsonSerializable<TaskInfo> implements Serializable {
    //closed range [start, end]
    private int startId;
    private int endId;

    public TaskInfo(int start, int end) {
        this.startId = start;
        this.endId = end;
    }

    public int getStartId() {
        return startId;
    }

    public void setStartId(int startId) {
        this.startId = startId;
    }

    public int getEndId() {
        return endId;
    }

    public void setEndId(int endId) {
        this.endId = endId;
    }

}
