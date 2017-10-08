package org.apache.fake.storm.supervisor.worker.stream;

import org.apache.storm.shade.org.apache.commons.collections.map.LinkedMap;

import java.util.*;

/**
 * Created by yilong on 2017/10/7.
 */
public class StreamGraph {
    public enum ComponentType {
        SPOUT,
        BOLT
    }

    ComponentType type;
    Object object;
    String componentId;
    int taskId;
    //int taskStartId;
    //int taskEndId;
    //TODO: task should be per stream, and not per component

    Map<String, Set<String>> streamFields;
    Map<String, Set<StreamGraph>> childStreamGraphs;

    public StreamGraph(ComponentType t, Object obj, String cid, int tid) {
        componentId = cid;
        taskId = tid;
        //taskStartId = sid;
        //taskEndId = eid;
        type = t;
        object = obj;
        childStreamGraphs = new LinkedMap();
        streamFields = new LinkedMap();
    }

    public ComponentType getType() {
        return type;
    }

    public String getComponentId() {
        return componentId;
    }

    public int getTaskId() {
        return taskId;
    }

    public Object getObject() { return object; }

    //public int getTaskStartId() {
    //    return taskStartId;
    //}
    //public int getTaskEndId() {
    //    return taskEndId;
    //}

    public void addStreamField(String streamId, String field) {
        Set<String> set = null;
        if (streamFields.containsKey(streamId)) {
            set = streamFields.get(streamId);
        } else {
            set = new HashSet<>();
            streamFields.put(streamId, set);
        }

        set.add(field);
    }

    public List<String> getStreams() {
        List<String> out = new ArrayList();
        for (Map.Entry<String, Set<String>> entry : streamFields.entrySet()) {
            out.add(entry.getKey());
        }

        return out;
    }

    public void addChildStreamGraph(String streamId, StreamGraph n) {
        Set<StreamGraph> set = null;
        if (childStreamGraphs.containsKey(streamId)) {
            set = childStreamGraphs.get(streamId);
        } else {
            set = new HashSet<>();
            childStreamGraphs.put(streamId, set);
        }

        set.add(n);
    }

    public Set<StreamGraph> getChildStreamGraphs(String streamId) {
        return childStreamGraphs.get(streamId);
    }

    public List<Integer> getChildStreamTasks(String streamId) {
        List<Integer> taskIds = new ArrayList<>();

        Set<StreamGraph> sets = getChildStreamGraphs(streamId);
        if (sets == null) {
            return taskIds;
        }

        for (StreamGraph sg : sets) {
            taskIds.add(sg.getTaskId());
        }

        return taskIds;
    }

    public Map<String, Set<StreamGraph>> getChildStreamGraphMap() {
        return childStreamGraphs;
    }

    public StreamGraph findStreamGraph(String componentId) {
        if (this.componentId.equals(componentId)) {
            return this;
        }

        for (Map.Entry<String, Set<StreamGraph>> entry : childStreamGraphs.entrySet()) {
            for (StreamGraph sg : entry.getValue()) {
                if (sg.componentId.equals(componentId)) {
                    return sg;
                }
            }
        }

        return null;
    }
}
