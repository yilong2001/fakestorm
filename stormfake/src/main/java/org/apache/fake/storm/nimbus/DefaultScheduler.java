package org.apache.fake.storm.nimbus;

import org.apache.fake.storm.model.*;
import org.apache.fake.storm.store.IResourceStore;
import org.apache.fake.storm.store.RedisStore;
import org.apache.fake.storm.utils.ClusterConfigUtil;
import org.apache.fake.storm.utils.SupervisorConfigUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by yilong on 2017/10/5.
 */
public class DefaultScheduler implements IScheduler {
    public static final Logger LOG = LoggerFactory.getLogger(DefaultScheduler.class);
    private final Map<String, Object> conf;
    private final String stormid;
    private final IResourceStore resourceStore;

    public DefaultScheduler(final Map<String, Object> conf) {
        this.conf = conf;
        stormid = ClusterConfigUtil.getStormId(conf);
        resourceStore = new RedisStore(conf);
    }

    @Override
    public List<WorkerAssignmentInfo> schedule() throws Exception {
        List<String> orgtopos  = resourceStore.getTopoIds(stormid);
        List<String> topos = new ArrayList<>();
        for (String topo : orgtopos) {
            int v = resourceStore.getAssignmentVersion(stormid, topo);
            if (v == 0) {
                topos.add(topo);
            } else {
                //
            }
        }

        if (topos.size() == 0) {
            LOG.warn(" there is no more topos with version 0 ... ");
            return null;
        }

        List<SupervisorInfo> supervisorInfos = resourceStore.getSupervisors(stormid);

        Map<String, WorkerAssignmentInfo> topoAssignments = new HashMap<>();
        for (String topo : topos) {
            WorkerAssignmentInfo info = resourceStore.getAssignment(stormid, topo);
            if (info != null) {
                topoAssignments.put(topo, info);
            }
        }

        List<String> unAssignTopos  = new ArrayList<>();
        List<WorkerNodeInfo> unAssignWorkerNode = new ArrayList<>();

        for (String topo : topos) {
            if (!topoAssignments.containsKey(topo)) {
                unAssignTopos.add(topo);
            }
        }

        if (unAssignTopos.size() == 0) {
            LOG.warn(" there is no more topos ... ");
            return null;
        }

        List<Integer> ports = SupervisorConfigUtil.getPorts(conf);
        for (SupervisorInfo si : supervisorInfos) {
            for (Integer port : ports) {
                boolean find = false;
                for (Map.Entry<String, WorkerAssignmentInfo> entry : topoAssignments.entrySet()) {
                    for (AssignmentInfo assignmentInfo : entry.getValue().getAssignmentInfos()) {
                        if (assignmentInfo.getNodeInfo().getSupervisorId().equals(si.getSupervisorId()) &&
                                (assignmentInfo.getNodeInfo().getPort() == port)) {
                            find = true;
                            break;
                        }
                    }
                }

                if (!find) {
                    unAssignWorkerNode.add(new WorkerNodeInfo(si.getSupervisorId(), si.getHostIp(), port));
                }
            }
        }

        if (unAssignWorkerNode.size() == 0) {
            LOG.warn(" there is no enough worker node ... ");
            return null;
        }

        //TODO: alloc worker
        //TODO: alloc topo
        //TODO: simple for demo
        List<WorkerAssignmentInfo> newAssignInfos = new ArrayList<>();

        int allUnAssignNodeNum = unAssignWorkerNode.size();
        int curNodeIndex = 0;
        for (String topo : unAssignTopos) {
            if (curNodeIndex >= allUnAssignNodeNum - 1) {
                //at least two nodes for one topo
                break;
            }

            WorkerNodeInfo node1 = unAssignWorkerNode.get(curNodeIndex++);
            WorkerNodeInfo node2 = unAssignWorkerNode.get(curNodeIndex++);

            WorkerAssignmentInfo info = new WorkerAssignmentInfo();
            List<AssignmentInfo> asss = new ArrayList<>();

            AssignmentInfo ass1 = new AssignmentInfo();
            ass1.setNodeInfo(node1);
            ass1.setResourceInfo(new ResourceInfo(100, 1000));
            ass1.setTaskInfo(new TaskInfo(0, 1));

            asss.add(ass1);

            AssignmentInfo ass2 = new AssignmentInfo();
            ass2.setNodeInfo(node2);
            ass2.setResourceInfo(new ResourceInfo(100, 1000));
            ass2.setTaskInfo(new TaskInfo(1, 5));

            asss.add(ass2);

            info.setAssignmentInfos(asss);
            info.setTopoId(topo);
            info.setVersion(2);

            newAssignInfos.add(info);

            LOG.info(info.toString());
        }

        return newAssignInfos;
    }

    public IResourceStore getResourceStore() {
        return resourceStore;
    }

    public String getStormId() {
        return stormid;
    }
}
