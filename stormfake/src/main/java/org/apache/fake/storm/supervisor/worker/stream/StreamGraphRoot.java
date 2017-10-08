package org.apache.fake.storm.supervisor.worker.stream;

import java.util.*;

/**
 * Created by yilong on 2017/10/7.
 */
public class StreamGraphRoot {
    int spoutTaskCount;
    int taskCount;
    //TODO: task should be per stream, and not per component

    List<StreamGraph> allStreamGraphs;
    Map<String, StreamGraph> childParentMaps;

    public StreamGraphRoot() {
        spoutTaskCount = 0;
        taskCount = 0;
        allStreamGraphs = new ArrayList<>();
        childParentMaps = new HashMap<>();
    }

    public StreamGraphRoot addStreamGraph(StreamGraph sg) {
        allStreamGraphs.add(sg);
        return this;
    }

    public StreamGraphRoot incTaskCount() {
        taskCount++;
        return this;
    }

    public int getTaskCount() {
        return taskCount;
    }

    public void setSpoutTaskCount(int count) {
        spoutTaskCount = count;
    }

    public int getSpoutTaskCount() {
        return spoutTaskCount;
    }

    public StreamGraph findStreamGraphByComponent(String componentId) {
        for (StreamGraph sg : this.allStreamGraphs) {
            if (sg.getComponentId().equals(componentId)) {
                return sg;
            }
        }

        return null;
    }

    public StreamGraph findStreamGraphByTask(int taskid) {
        for (StreamGraph sg : this.allStreamGraphs) {
            if (sg.getTaskId() == taskid) {
                return sg;
            }
        }

        return null;
    }

    public List<StreamGraph> findStreamGraphsByTaskList(List<Integer> taskids) {
        List<StreamGraph> streamGraphs = new ArrayList<>();

        for (int taskid : taskids) {
            for (StreamGraph sg : this.allStreamGraphs) {
                if (sg.getTaskId() == taskid) {
                    streamGraphs.add(sg);
                }
            }
        }

        return streamGraphs;
    }

    public void addChildParentGraph(String childComponentId, String childStreamId, StreamGraph parentStreamGraph) {
        childParentMaps.put(childComponentId+"_"+childStreamId, parentStreamGraph);
    }

    public StreamGraph getParentGraph(String childComponentId, String childStreamId) {
        if (childParentMaps.containsKey(childComponentId+"_"+childStreamId)) {
            return childParentMaps.get(childComponentId+"_"+childStreamId);
        }

        return null;
    }

    public Set<StreamGraph> findChildsByParentComponentAndStream(String parentComponentId, String streamId) {
        StreamGraph sg = findStreamGraphByComponent(parentComponentId);
        if (sg == null) {
            return null;
        }

        Map<String, Set<StreamGraph>> childs = sg.getChildStreamGraphMap();
        for (Map.Entry<String, Set<StreamGraph>> ch : childs.entrySet()) {
            if (streamId.equals(ch.getKey())) {
                return ch.getValue();
            }
        }

        return null;
    }

    public String showGraph() {
        StringBuilder stringBuilder = new StringBuilder();

        for (StreamGraph sg : allStreamGraphs) {
            for (String stream : sg.getStreams()) {
                StreamGraph parent = getParentGraph(sg.getComponentId(), stream);
                String parentComId = (parent == null) ? "none" : parent.getComponentId();

                stringBuilder.append(" child-stream : " + sg.getComponentId() + " - " + stream
                        + "; task(" + sg.getTaskId() + ")"
                        + "; parent : "
                        + parentComId + "\n");
            }
        }

        return stringBuilder.toString();
    }
}
