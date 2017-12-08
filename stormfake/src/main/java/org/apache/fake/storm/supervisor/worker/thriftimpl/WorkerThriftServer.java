package org.apache.fake.storm.supervisor.worker.thriftimpl;

import org.apache.fake.storm.generated.AuthorizationException;
import org.apache.fake.storm.generated.FileOperationException;
import org.apache.fake.storm.generated.Worker;
import org.apache.fake.storm.model.WorkerAssignmentInfo;
import org.apache.fake.storm.store.IResourceStore;
import org.apache.fake.storm.store.RedisStore;
import org.apache.fake.storm.supervisor.worker.IWorkerServer;
import org.apache.fake.storm.supervisor.worker.stream.*;
import org.apache.fake.storm.utils.Serde;
import org.apache.fake.storm.utils.TimeoutCleanMap;
import org.apache.storm.generated.*;
import org.apache.storm.shade.org.eclipse.jetty.util.ConcurrentHashSet;

import org.apache.thrift.TException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Created by yilong on 2017/6/29.
 */
public class WorkerThriftServer implements Worker.Iface {
    public static final Logger LOG = LoggerFactory.getLogger(WorkerThriftServer.class);

    private TimeoutCleanMap<String, BufferedInputStream> downloadMaps;
    private TimeoutCleanMap<String, BufferedOutputStream> uploadMaps;

    private final static String defaultTopoPackagesPath = "/Users/yilong/work/bigdata/code/stream/stormfake/topo/";
    private final IResourceStore resourceStore;

    private final String supervisorId;
    private final String workerId;
    private final String stormId;
    private final String host;
    private final int port;
    private ConcurrentHashSet<String> workerClientSessions;
    private AtomicReference<List<String>> currentSessions;
    private AtomicReference<WorkerAssignmentInfo> assignmentInfoAtomic;
    private AtomicReference<StreamGraphRoot> streamGraphRootAtomic;

    private final IWorkerServer iworkerServer;

    public WorkerThriftServer(String host, int port,
                              String stormId, String supervisorId, String workerId,
                              Map<String, Object> conf,
                              IWorkerServer iworkerServer) {
        this.stormId = stormId;
        this.supervisorId = supervisorId;
        this.workerId = workerId;

        this.iworkerServer = iworkerServer;

        this.host = host;
        this.port = port;
        this.resourceStore = new RedisStore(conf);
        this.workerClientSessions = new ConcurrentHashSet<>();
        this.currentSessions = new AtomicReference<>(new ArrayList<>());
        this.assignmentInfoAtomic = new AtomicReference<>(null);
        this.streamGraphRootAtomic = new AtomicReference<>(null);
    }

    @Override
    public String getConnectionId() throws AuthorizationException, FileOperationException, TException {
        String id = UUID.randomUUID().toString();

        List<String> ids = currentSessions.get();
        ids.add(id);
        currentSessions.compareAndSet(currentSessions.get(), ids);

        return id;
    }

    @Override
    public boolean setAssignment(String id, String assignment) throws AuthorizationException, FileOperationException, TException {
        //TODO: update assignment
        return true;
    }

    @Override
    public boolean setAssignmentPath(String id, String stormId, String topoId) throws AuthorizationException, FileOperationException, TException {
        List<String> ids = currentSessions.get();
        if (!ids.contains(id)) {
            throw new FileOperationException("id is not in currentSessions_list, actual:"+id);
        }

        WorkerAssignmentInfo info = null;
        try {
            info = resourceStore.getAssignment(stormId, topoId);
        } catch (Exception e) {
            LOG.error(e.getMessage(), e);
            throw new FileOperationException("setAssignmentPath on getAssignment failed : " + e.getMessage());
        }

        WorkerAssignmentInfo curInfo = assignmentInfoAtomic.get();
        if (curInfo == null) {
            assignmentInfoAtomic.compareAndSet(curInfo, info);
        } else if (curInfo.getVersion() < info.getVersion()) {
            assignmentInfoAtomic.compareAndSet(curInfo, info);
        } else {
            LOG.warn(" old version assignment ... ");
            return true;
        }

        TopologyGetter topologyGetter = new TopologyGetter(stormId, info.getTopoId(), resourceStore);
        StormTopology topology = topologyGetter.get();

        StreamGraphRoot root = TopologyEngine.processTopology(topology);
        streamGraphRootAtomic.compareAndSet(streamGraphRootAtomic.get(), root);

        TopologyEngine topologyEngine = new TopologyEngine(info, root);

        LOG.info("======================================");
        LOG.info(info.show());
        LOG.info(root.showGraph());
        LOG.info("======================================");

        iworkerServer.setTopologyEngine(topologyEngine);

        LOG.info(" workerServer setAssignmentPath successful : v = " + info.getVersion());

        //TODO: update assignment
        return true;
    }

    @Override
    public boolean ping(String id) throws AuthorizationException, FileOperationException, TException {
        LOG.info(" worker ping heartbeat ");
        return true;
    }

    @Override
    public boolean sendMessage(String sessionid, String message, int taskid, String messageId) throws AuthorizationException, FileOperationException, TException {
        List<String> ids = currentSessions.get();
        if (!ids.contains(sessionid)) {
            throw new FileOperationException("id is not in currentSessions_list, actual:"+sessionid);
        }

        List<Object> tuple = null;
        try {
            tuple = Serde.collectionDeSerialize(message);
        } catch (Exception e) {
            LOG.error(e.getMessage(), e);
            throw new FileOperationException(" sendMessage tuple collection serialize failed ......");
        }

        iworkerServer.receiveTuple(taskid, tuple, messageId);
        LOG.info( " ThriftServerSendMessageOk ");
        return true;
    }
}
