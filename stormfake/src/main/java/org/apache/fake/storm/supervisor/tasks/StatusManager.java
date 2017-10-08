package org.apache.fake.storm.supervisor.tasks;

import org.apache.fake.storm.model.AssignmentInfo;
import org.apache.fake.storm.model.WorkerAssignmentInfo;
import org.apache.fake.storm.store.IResourceStore;
import org.apache.fake.storm.supervisor.manager.ISupervisor;
import org.apache.fake.storm.supervisor.manager.WorkerNodeController;
import org.apache.fake.storm.utils.IEventConsumer;
import org.apache.fake.storm.utils.SupervisorConfigUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicReference;

/**
 * 0, initialize workercontroller according port config
 * 1, periodly fetch assignment for supervisor node
 * 2, according latest assignment to start worker or stop worker
 * 3, save assginment to local, when supervisor restart,
 * Created by yilong on 2017/7/18.
 */
public class StatusManager implements Runnable, AutoCloseable {
    public static final Logger LOG = LoggerFactory.getLogger(StatusManager.class);

    private final ISupervisor iSupervisor;
    private final IResourceStore resourceStore;
    private final IEventConsumer<Runnable> eventConsumer;
    //private final Map<Integer, WorkerNodeController> workerControllerMap = new HashMap<>();
    private final ConcurrentHashMap<Integer, WorkerNodeController> workerControllerMap = new ConcurrentHashMap<>();
    private final Map<String, Object> conf;
    private AtomicReference<Boolean> isActive = new AtomicReference<>();
    private AtomicReference<Map<String, Integer>> curTopoAssignmentVers = new AtomicReference<>(new HashMap<>());

    public static final int SLEEP_PERIOD = 10 * 1000;

    private final String supervisorHost;

    public StatusManager(final Map<String, Object> conf, ISupervisor iSupervisor, String supervisorHost) {
        this.conf = conf;
        this.iSupervisor = iSupervisor;
        this.resourceStore = iSupervisor.getResourceStore();
        this.eventConsumer = iSupervisor.getEventConsumer();

        this.supervisorHost = supervisorHost;

        List<Integer> ports = SupervisorConfigUtil.getPorts(conf);
        for (Integer port : ports) {
            WorkerNodeController wc = new WorkerNodeController(iSupervisor, port);
            workerControllerMap.put(port, wc);
        }

        isActive.set(true);

        //TODO: cleanup workers and start new workers again

        //TODO: start
        for (WorkerNodeController wc : workerControllerMap.values()) {
            wc.start();
        }
    }

    /**
     * two method support assignment update:
     * 1) poll up periodly;
     * 2) notify callback when changed immediately;
     *
     *                   topoId
     *                     |
     *                    / \
     *                   /   \
     *             version   assignment[multi]
     *                          \
     *                  /       |       \
     *           host:port  resources  tasks
     *                          |
     *                        /   \
     *                      CPU   mem
     *
     */
    @Override
    public void run() {
        while (isActive.get()) {
            try {
                Thread.sleep(SLEEP_PERIOD);

                LOG.info("--------- StatusManager run start ... --------- ");
                boolean updated = false;
                String stormid = iSupervisor.getStormId();

                HashMap<String, Integer> topoAssignmentVer = new HashMap<>();
                List<String> topos = resourceStore.getTopoIds(stormid);
                for (String topo : topos) {
                    int v = resourceStore.getAssignmentVersion(stormid, topo);
                    topoAssignmentVer.put(topo, v);
                }

                Map<String, Integer> cur = curTopoAssignmentVers.get();
                if (cur != null) {
                    for (String topo : topos) {
                        if (!cur.containsKey(topo)) {
                            updated = true;
                            continue;
                        }

                        if ((cur.get(topo) == 0) || (cur.get(topo) != topoAssignmentVer.get(topo))) {
                            updated = true;
                            continue;
                        }
                    }
                } else {
                    updated = true;
                }

                //topo assignments is no update
                if (!updated) {
                    LOG.warn(" ---------  StatusManager , there is no updated topo assignment ---------- ");
                    continue;
                }

                curTopoAssignmentVers.set(topoAssignmentVer);

                //TODO: get updated detail, which host:port is updated(add/update/delete)
                Map<String, WorkerAssignmentInfo> topoAssignments = new HashMap<>();
                for (String topo : topos) {
                    WorkerAssignmentInfo info = resourceStore.getAssignment(stormid, topo);
                    if (info != null) {
                        topoAssignments.put(topo, info);
                    }
                }

                if (topoAssignments.size() == 0) {
                    LOG.warn(" ---------  StatusManager : topoAssignmentsSize is 0 --------- ");
                }

                for (Map.Entry<String, WorkerAssignmentInfo> entry : topoAssignments.entrySet()) {
                    for (AssignmentInfo assignmentInfo : entry.getValue().getAssignmentInfos()) {
                        if (!assignmentInfo.getNodeInfo().getSupervisorId().equals(iSupervisor.getSupervisorId())) {
                            LOG.warn(assignmentInfo.getNodeInfo().getSupervisorId() + " is skipped, cur supervisor id is : " + iSupervisor.getSupervisorId());
                            continue;
                        }

                        //the node is current supervisor, and should update
                        WorkerAssignmentInfo info = entry.getValue();

                        WorkerNodeController wc = workerControllerMap.get(assignmentInfo.getNodeInfo().getPort());
                        if (wc == null) {
                            wc = new WorkerNodeController(iSupervisor, assignmentInfo.getNodeInfo().getPort(), info);
                            workerControllerMap.put(assignmentInfo.getNodeInfo().getPort(), wc);
                            wc.start();
                        } else {
                            wc.setWorkerAssignmentInfo(info);
                        }
                    }
                }

                //LOG.info(topoAssignments.toString());
                LOG.info("--------- StatusManager run end ... --------- ");
            } catch (Exception e) {
                //
                LOG.error(e.getMessage(), e);
                throw new RuntimeException(e.getMessage());
            }
        }
    }

    @Override
    public void close() throws Exception {
        for (WorkerNodeController wc : workerControllerMap.values()) {
            wc.close();
        }

        isActive.set(false);
    }

}
