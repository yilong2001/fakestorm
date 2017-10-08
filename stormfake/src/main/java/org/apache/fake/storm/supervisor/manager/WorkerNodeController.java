package org.apache.fake.storm.supervisor.manager;

import org.apache.fake.storm.model.TopoFileLocationInfo;
import org.apache.fake.storm.model.WorkerAssignmentInfo;
import org.apache.fake.storm.nimbus.thriftimpl.NimbusThriftClient;
import org.apache.fake.storm.store.IResourceStore;
import org.apache.fake.storm.store.RedisStore;
import org.apache.fake.storm.supervisor.worker.thriftimpl.WorkerThriftClient;
import org.apache.fake.storm.utils.ClusterConfigUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicReference;

import static org.apache.fake.storm.utils.SupervisorConfigUtil.getWorkerJarFilename;

/**
 * 1, manage worker status
 * 2, launch worker or stop worker
 * 3, maintain state machine of worker
 * Created by yilong on 2017/7/18.
 */
public class WorkerNodeController extends Thread implements AutoCloseable {
    public static final Logger LOG = LoggerFactory.getLogger(WorkerNodeController.class);
    private static int sleepIntervalMs = 1000;    //1s
    private static int workerTimeoutMs = 1000*10; //10s

    private final int port;
    private AtomicReference<Boolean> active = new AtomicReference<>(false);
    private AtomicReference<WorkerAssignmentInfo> workerAssignmentInfo = new AtomicReference<>();
    private volatile WorkerStateMachine workerStateMachine;
    private WorkerThriftClient workerThriftClient;
    private WorkerHeartbeat workerHeartbeat;
    private NimbusThriftClient nimbusThriftClient;
    private final ISupervisor iSupervisor;
    private final ThreadPoolExecutor threadPoolExecutor;
    private final IResourceStore resourceStore;

    static enum WorkerState {
        EMPTY,
        WAITING_TO_WORKER_START,
        WAITING_TO_LOADING_PACKAGE,
        RUNNING,
        WAITING_TO_KILL;
    };

    static class WorkerStateMachine {
        public final long startTime;
        public final WorkerState workerState;
        public final WorkerAssignmentInfo newAssignment;
        public final WorkerAssignmentInfo curAssignment;
        public final Future<?> pendingDownloader;
        public final WorkerNodeLoader workerNodeLoader;

        public WorkerStateMachine(WorkerAssignmentInfo curAssignment,
                                  WorkerAssignmentInfo newAssignment,
                                  WorkerNodeLoader workerNodeLoader) {
            this.curAssignment = curAssignment;
            this.newAssignment = newAssignment;
            this.startTime = System.currentTimeMillis();
            this.workerState = WorkerState.EMPTY;
            this.pendingDownloader = null;
            this.workerNodeLoader = workerNodeLoader;
        }

        public WorkerStateMachine(WorkerAssignmentInfo curAssignment,
                                  WorkerAssignmentInfo newAssignment,
                                  WorkerState state,
                                  Future<?> pendingDownloader,
                                  WorkerNodeLoader workerNodeLoader) {
            this.curAssignment = curAssignment;
            this.newAssignment = newAssignment;
            this.startTime = System.currentTimeMillis();
            this.workerState = state;
            this.pendingDownloader = pendingDownloader;
            this.workerNodeLoader = workerNodeLoader;
        }

        public boolean isTimeout() {
            return ((startTime + workerTimeoutMs) > System.currentTimeMillis());
        }

    }

    public WorkerNodeController(ISupervisor supervisor, int port) {
        this(supervisor, port, null);
    }

    public WorkerNodeController(ISupervisor supervisor, int port, WorkerAssignmentInfo info) {
        this.iSupervisor = supervisor;
        this.port = port;

        workerStateMachine = new WorkerStateMachine(null,
                info,
                new WorkerNodeLoader(supervisor.getConf(),
                        supervisor.getSupervisorId(),
                        supervisor.getWorkerIdByPort(port),
                        supervisor.getSupervisorHost(),
                        port));

        nimbusThriftClient = new NimbusThriftClient(iSupervisor.getConf());
        workerThriftClient = new WorkerThriftClient(iSupervisor.getConf(), port, iSupervisor.getSupervisorHost());
        workerAssignmentInfo.set(info);
        workerHeartbeat = new WorkerHeartbeat(workerThriftClient);
        threadPoolExecutor = new ThreadPoolExecutor(1, 1, 10,
                TimeUnit.SECONDS,
                new ArrayBlockingQueue<Runnable>(100));
        resourceStore = new RedisStore(iSupervisor.getConf());
    }

    public void setWorkerAssignmentInfo(WorkerAssignmentInfo info) {
        LOG.info("--------------- " + iSupervisor.getSupervisorId() + " update assignment info ");
        workerAssignmentInfo.set(info);
    }

    private WorkerStateMachine processEmptyState(WorkerStateMachine stateMachine, WorkerAssignmentInfo newAssignment) {
        if (newAssignment == null) {
            return stateMachine;
        }

        //TODO: start worker process

        //TODO: load package
        TopoFileLocationInfo fileLocationInfo = null;
        try {
             fileLocationInfo = resourceStore.getTopoFileLocation(iSupervisor.getStormId(),
                    newAssignment.getTopoId(),
                    ClusterConfigUtil.TOPO_FILE_TYPE_JAR);
        } catch (Exception e) {
            LOG.error(e.getMessage(), e);
            return stateMachine;
        }

        if (fileLocationInfo == null) {
            LOG.error("processEmptyState : but fileLocationInfo is null!");
            return stateMachine;
        }

        String localFile = getWorkerJarFilename(iSupervisor.getConf(), iSupervisor.getWorkerIdByPort(port));
        WorkerNodeLoader wl = new WorkerNodeLoader(iSupervisor.getConf(),
                iSupervisor.getSupervisorId(),
                iSupervisor.getWorkerIdByPort(port),
                iSupervisor.getSupervisorHost(),
                port);

        CompletionDownloader completionDownloader = new CompletionDownloader(fileLocationInfo,
                localFile, threadPoolExecutor, iSupervisor.getConf(), wl);
        Future<?> downloader = completionDownloader.startDownload();

        workerHeartbeat.restart();

        return new WorkerStateMachine(stateMachine.curAssignment, newAssignment,
                WorkerState.WAITING_TO_WORKER_START, downloader,wl
                );
    }

    private WorkerStateMachine processWaitToStartState(WorkerStateMachine stateMachine, WorkerAssignmentInfo newAssignment) {
        if (stateMachine.pendingDownloader.isDone()) {
            LOG.info("-------------- pending downloader is done! --------------");
        } else if (stateMachine.pendingDownloader.isCancelled()) {
            LOG.info("-------------- pending downloader is cancelled! --------------");
        } else {
            try {
                stateMachine.pendingDownloader.get(1, TimeUnit.SECONDS);
            } catch (TimeoutException e) {
                LOG.warn("downloading jar is timeout!");
                return stateMachine;
            } catch (ExecutionException e) {
                LOG.error(e.getMessage(), e);
                return new WorkerStateMachine(null, stateMachine.curAssignment, null);
            } catch (InterruptedException e) {
                LOG.error(e.getMessage(), e);
                throw new RuntimeException(e.getMessage());
            }

            return stateMachine;
        }

        if (workerHeartbeat.timeout()) {
            //TODO: kill and restart
            throw new RuntimeException("worker state timeout!");
        }

        if (workerThriftClient.isClosed()) {
            return stateMachine;
        }

        return new WorkerStateMachine(stateMachine.curAssignment, newAssignment, WorkerState.RUNNING, null, stateMachine.workerNodeLoader);
    }

    private WorkerStateMachine processRuningState(WorkerStateMachine stateMachine, WorkerAssignmentInfo newAssignment) {
        if (workerHeartbeat.timeout()) {
            //TODO: kill and restart
            LOG.error("runing worker is stopped and timeout!");
            return new WorkerStateMachine(null, stateMachine.curAssignment, null);
        }

        //TODO: when running, heartbeat worker and update assignment
        if (newAssignment != null && (stateMachine.curAssignment == null ||
                (stateMachine.curAssignment.getVersion() != newAssignment.getVersion())) ) {
            try {
                LOG.info("------------ worker controller setAssignment with worker thrift client ---------- ");
                workerThriftClient.setAssignmentPath(iSupervisor.getStormId(), newAssignment.getTopoId());
                return new WorkerStateMachine(newAssignment, null, WorkerState.RUNNING, null, null);
            } catch (Exception e) {
                LOG.error(e.getMessage(), e);
                return new WorkerStateMachine(null, newAssignment, null);
            }
        }

        return stateMachine;
    }

    private WorkerStateMachine processWorkerStatus(WorkerStateMachine stateMachine, WorkerAssignmentInfo newAssignment) {
        switch (stateMachine.workerState) {
            case EMPTY:
                return processEmptyState(stateMachine, newAssignment);
            case WAITING_TO_WORKER_START:
                return processWaitToStartState(stateMachine, newAssignment);
            case WAITING_TO_LOADING_PACKAGE:
                break;
            case RUNNING:
                return processRuningState(stateMachine, newAssignment);
            case WAITING_TO_KILL:
                break;
        }
        return stateMachine;
    }

    static class WorkerHeartbeat {
        private long startTime;
        private final WorkerThriftClient workerThriftClient;
        public WorkerHeartbeat(WorkerThriftClient workerThriftClient) {
            this.startTime = System.currentTimeMillis();
            this.workerThriftClient = workerThriftClient;
        }

        public void restart() {
            startTime = System.currentTimeMillis();
            try {
                this.workerThriftClient.close();
            } catch (Exception e) {
                LOG.error(e.getMessage(), e);
            }

            try {
                workerThriftClient.connect();
            } catch (Exception e) {
                LOG.error(e.getMessage(), e);
            }
        }

        public boolean timeout() {
            try {
                if (workerThriftClient.isClosed()) {
                    workerThriftClient.connect();
                }
                if (workerThriftClient.ping()) {
                    startTime = System.currentTimeMillis();
                    LOG.info("--------------------- ping is ok ----------------");
                    return false;
                }
            } catch (Exception e) {
                LOG.error(e.getMessage(), e);
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException e1) {
                    LOG.error(e.getMessage(), e);
                }

                connectWorker();
            }

            return ((System.currentTimeMillis() - startTime) > workerTimeoutMs);
        }

        private void connectWorker() {
            if (workerThriftClient.isClosed()) {
                for (int i=0; i<3; i++) {
                    try {
                        if (workerThriftClient.connect()) {
                            break;
                        }
                        Thread.sleep(1000);
                    } catch (Exception e) {
                        LOG.error("trying to connect worker : ");
                        LOG.error(e.getMessage());
                    }
                }
            }
        }
    }

    public class CompletionDownloader {
        private ThreadPoolExecutor executor;
        private NimbusThriftClient client;
        private TopoFileLocationInfo remoteInfo;
        private final String localFile;
        private final WorkerNodeLoader workerNodeLoader;

        public CompletionDownloader(TopoFileLocationInfo remoteInfo,
                                    String localFile,
                                    ThreadPoolExecutor executor,
                                    Map<String,Object> conf,
                                    WorkerNodeLoader workerNodeLoader) {
            //executor = new ThreadPoolExecutor(1, 1, 10, TimeUnit.SECONDS,
            //        new ArrayBlockingQueue<Runnable>(100));
            this.remoteInfo = remoteInfo;
            this.localFile = localFile;
            this.executor = executor;
            this.client = new NimbusThriftClient(remoteInfo.getHost(),
                    remoteInfo.getPort(),
                    conf);
            this.workerNodeLoader = workerNodeLoader;
        }

        public Future<?> startDownload() {
            return executor.submit(new Runnable() {
                @Override
                public void run() {
                    client.downloadTopo(remoteInfo.getPath(), localFile);
                    try {
                        workerNodeLoader.init();
                        workerNodeLoader.loadWorkProcess(null);
                    } catch (Exception e) {
                        LOG.error("--------- workerload loadWorkProcess failed -----------", e);
                        throw new RuntimeException(e);
                    }
                }
            });
        }
    }

    @Override
    public void run() {
        active.set(true);
        String out = "";

        while (active.get()) {
            try {
                LOG.info(" WorkController run start : " + iSupervisor.getSupervisorId() + " : " + port);

                //TODO
                try {
                    Thread.sleep(sleepIntervalMs);
                } catch (InterruptedException e) {
                    LOG.error(" WorkController Sleep InterruptedException ");
                }

                WorkerAssignmentInfo info = workerAssignmentInfo.get();
                if (info != null) {
                    workerStateMachine = processWorkerStatus(workerStateMachine, info);
                }
            } catch (Exception e) {
                LOG.error(" WorkController Exception ");
                try {
                    nimbusThriftClient.close();
                } catch (Exception e1) {
                    LOG.error(e1.getMessage(), e1);
                }

                try {
                    nimbusThriftClient.close();
                } catch (Exception e1) {
                    LOG.error(e1.getMessage(), e1);
                }

                LOG.error("WorkController "+e.getMessage(), e);
            }
        }

        LOG.error("worker controller stopped : " + out);
    }

    @Override
    public void close() throws Exception {
        active.set(false);

        try {
            nimbusThriftClient.close();
        } catch (Exception e1) {
            LOG.error(e1.getMessage(), e1);
        }

        try {
            nimbusThriftClient.close();
        } catch (Exception e1) {
            LOG.error(e1.getMessage(), e1);
        }
    }
}
