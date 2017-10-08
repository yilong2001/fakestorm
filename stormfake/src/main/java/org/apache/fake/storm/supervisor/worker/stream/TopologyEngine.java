package org.apache.fake.storm.supervisor.worker.stream;

import org.apache.fake.storm.generated.FileOperationException;
import org.apache.fake.storm.model.WorkerAssignmentInfo;
import org.apache.fake.storm.model.WorkerNodeInfo;
import org.apache.storm.generated.*;
import org.apache.storm.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.*;

import static org.mockito.Mockito.mock;

/**
 * Created by yilong on 2017/10/6.
 */
public class TopologyEngine {
    public static final Logger LOG = LoggerFactory.getLogger(TopologyEngine.class);

    private WorkerAssignmentInfo assignmentInfo;
    private StreamGraphRoot streamGraphRoot;

    public TopologyEngine(WorkerAssignmentInfo info, StreamGraphRoot root) {
        assignmentInfo = info;
        streamGraphRoot = root;
    }

    public WorkerAssignmentInfo getAssignmentInfo() {
        return assignmentInfo;
    }

    public StreamGraphRoot getStreamGraphRoot() {
        return streamGraphRoot;
    }

    public static StreamGraphRoot processTopology(StormTopology topology) throws FileOperationException {
        try {
            StreamGraphRoot root = buildStreamGraph(topology);
            return root;
        } catch (Exception e) {
            LOG.error(e.getMessage(), e);
            throw new FileOperationException("StreamGraphRoot_processTopology failed : " + e.getMessage());
        }
    }

    public List<WorkerNodeInfo> findDestWorkerNodeByComponentAndStream(String componentId, String streamId) {
        Map<String, WorkerNodeInfo> allMap = new HashMap<>();
        Set<StreamGraph> sgs = streamGraphRoot.findChildsByParentComponentAndStream(componentId, streamId);
        for (StreamGraph sg : sgs) {
            WorkerNodeInfo nodeInfo = assignmentInfo.getWorkerNodeByTaskId(sg.getTaskId());
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

    public static void buildSpoutStreamGraph(StreamGraphRoot root, StormTopology topology) {
        Map<String, SpoutSpec> spsinfo = topology.get_spouts();
        for (Map.Entry<String, SpoutSpec> tmp1 : spsinfo.entrySet()) {
            StreamGraph.ComponentType type = StreamGraph.ComponentType.SPOUT;
            String componentid = tmp1.getKey().toLowerCase();
            Object object = Utils.javaDeserialize( tmp1.getValue().get_spout_object().get_serialized_java(), Serializable.class);
            int taskId = root.getTaskCount();

            //TODO: for demo, skip parallim
            root.incTaskCount();

            root.setSpoutTaskCount(root.getTaskCount());

            StreamGraph streamGraph = new StreamGraph(type, object, componentid, taskId);
            root.addStreamGraph(streamGraph);

            Map<String, StreamInfo> ssinfo = tmp1.getValue().get_common().get_streams();
            for (Map.Entry<String, StreamInfo> tmp2 : ssinfo.entrySet()) {
                for (String s : tmp2.getValue().get_output_fields()) {
                    streamGraph.addStreamField(tmp2.getKey(), s);
                }
            }
        }
    }

    public static void buildBoltStreamGraph(StreamGraphRoot root, StormTopology topology) {
        Map<String, Bolt> binfo = topology.get_bolts();

        for (Map.Entry<String, Bolt> tmp1 : binfo.entrySet()) {
            String componentid = tmp1.getKey().toLowerCase();
            StreamGraph.ComponentType type =  StreamGraph.ComponentType.BOLT;
            Object object = Utils.javaDeserialize( tmp1.getValue().get_bolt_object().get_serialized_java(), Serializable.class);
            int taskId = root.getTaskCount();

            //TODO: for demo, skip parallim
            root.incTaskCount();

            StreamGraph streamGraph = new StreamGraph(type, object, componentid, taskId);
            root.addStreamGraph(streamGraph);

            Map<String, StreamInfo> ssinfo = tmp1.getValue().get_common().get_streams();
            for (Map.Entry<String, StreamInfo> tmp2 : ssinfo.entrySet()) {
                for (String s : tmp2.getValue().get_output_fields()) {
                    streamGraph.addStreamField(tmp2.getKey(), s);
                }
            }
        }

        for (Map.Entry<String, Bolt> tmp1 : binfo.entrySet()) {
            String childComponentid = tmp1.getKey();
            Map<GlobalStreamId, Grouping> gginfo = tmp1.getValue().get_common().get_inputs();

            StreamGraph child = root.findStreamGraphByComponent(childComponentid);
            if (child == null) {
                throw new RuntimeException(childComponentid + " can not find StreamGraph! ");
            }

            for (Map.Entry<GlobalStreamId, Grouping> tmp2 : gginfo.entrySet()) {
                String parentComponentId = tmp2.getKey().get_componentId();
                String streamId = tmp2.getKey().get_streamId();
                StreamGraph parent = root.findStreamGraphByComponent(parentComponentId);

                if (parent == null) {
                    throw new RuntimeException(parentComponentId + " can not find StreamGraph! ");
                }

                root.addChildParentGraph(childComponentid, streamId, parent);
                parent.addChildStreamGraph(streamId, child);

                String out = tmp2.getValue().getSetField().getFieldName();
                LOG.info(parentComponentId + ":" + streamId + ":"+out);
            }
        }
    }

    public static StreamGraphRoot buildStreamGraph(StormTopology topology) {
        StreamGraphRoot root = new StreamGraphRoot();

        buildSpoutStreamGraph(root, topology);
        buildBoltStreamGraph(root, topology);

        return root;
    }
}
