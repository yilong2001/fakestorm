package org.apache.fake.storm.model;

import org.apache.fake.storm.supervisor.worker.stream.StreamGraph;
import org.apache.fake.storm.utils.JsonSerializable;

import java.io.Serializable;
import java.util.*;

/**
 * Created by yilong on 2017/8/27.
 */
public class WorkerAssignmentInfo implements Serializable {
    private String topoId;
    private int version;
    private List<AssignmentInfo> assignmentInfos = new ArrayList<>();

    public WorkerAssignmentInfo() {}

    public String getTopoId() {
        return topoId;
    }

    public void setTopoId(String topoId) {
        this.topoId = topoId;
    }

    public int getVersion() {
        return version;
    }

    public void setVersion(int version) {
        this.version = version;
    }

    public List<AssignmentInfo> getAssignmentInfos() {
        return assignmentInfos;
    }

    public void setAssignmentInfos(List<AssignmentInfo> assignmentInfos) {
        this.assignmentInfos = assignmentInfos;
    }

    public WorkerNodeInfo getWorkerNodeByTaskId(int taskId) {
        for (AssignmentInfo info : assignmentInfos) {
            TaskInfo taskInfo = info.getTaskInfo();
            if (taskInfo.getStartId() <= taskId && taskInfo.getEndId() > taskId) {
                return info.getNodeInfo();
            }
        }

        return null;
    }

    public List<WorkerNodeInfo> getWorkerNodesByTaskIds(List<Integer> taskIds) {
        Map<String, WorkerNodeInfo> allMap = new HashMap<>();
        for (Integer task : taskIds) {
            WorkerNodeInfo nodeInfo = getWorkerNodeByTaskId(task);
            if (nodeInfo != null) {
                allMap.put(nodeInfo.getHostIp()+"_"+nodeInfo.getPort(), nodeInfo);
            }
        }

        List<WorkerNodeInfo> all = new ArrayList<>();
        for (Map.Entry<String, WorkerNodeInfo> entry : allMap.entrySet()) {
            all.add(entry.getValue());
        }

        return all;
    }

    public List<TaskInfo> getTaskInfoBySupervisorAndPort(String supervisor, int port) {
        List<TaskInfo> taskInfos = new ArrayList<>();

        for (AssignmentInfo info : assignmentInfos) {
            if (info.getNodeInfo().getSupervisorId().equals(supervisor) &&
                    info.getNodeInfo().getPort() == port) {
                taskInfos.add(info.getTaskInfo());
            }
        }

        return taskInfos;
    }

    public String show() {
        StringBuilder sb = new StringBuilder();
        sb.append("******************* WorkerAssignmentInfoShow : " + topoId + "\n");
        for (AssignmentInfo info : assignmentInfos) {
            sb.append("\n"+info.getNodeInfo().getHostIp()+":"+info.getNodeInfo().getPort()+":");
            sb.append(info.getTaskInfo().getStartId()+"---"+info.getTaskInfo().getEndId()+"\n");
        }
        return sb.toString();
    }
}
