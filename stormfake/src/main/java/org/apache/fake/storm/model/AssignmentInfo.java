package org.apache.fake.storm.model;

import org.apache.fake.storm.utils.JsonSerializable;

import java.io.Serializable;

/**
 * Created by yilong on 2017/7/8.
 */
public class AssignmentInfo extends JsonSerializable<AssignmentInfo> implements Serializable {
    private WorkerNodeInfo nodeInfo;
    private ResourceInfo resourceInfo;
    private TaskInfo taskInfo;
    private String workerId;

    public AssignmentInfo() {
    }

    public String getWorkerId() {
        return workerId;
    }

    public void setWorkerId(String workerId) {
        this.workerId = workerId;
    }

    public WorkerNodeInfo getNodeInfo() {
        return nodeInfo;
    }

    public void setNodeInfo(WorkerNodeInfo nodeInfo) {
        this.nodeInfo = nodeInfo;
    }

    public ResourceInfo getResourceInfo() {
        return resourceInfo;
    }

    public void setResourceInfo(ResourceInfo resourceInfo) {
        this.resourceInfo = resourceInfo;
    }

    public TaskInfo getTaskInfo() {
        return taskInfo;
    }

    public void setTaskInfo(TaskInfo taskInfo) {
        this.taskInfo = taskInfo;
    }
}
