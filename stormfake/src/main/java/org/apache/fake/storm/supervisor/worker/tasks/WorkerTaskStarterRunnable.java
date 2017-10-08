package org.apache.fake.storm.supervisor.worker.tasks;

import org.apache.fake.storm.model.TaskInfo;
import org.apache.fake.storm.supervisor.worker.IWorkerServer;
import org.apache.fake.storm.supervisor.worker.stream.DefaultBoltOutputCollector;
import org.apache.fake.storm.supervisor.worker.stream.DefaultSpoutOutputCollector;
import org.apache.fake.storm.supervisor.worker.stream.StreamGraph;
import org.apache.fake.storm.supervisor.worker.stream.TopologyEngine;
import org.apache.fake.storm.utils.TopoCtxFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Created by yilong on 2017/10/7.
 */
public class WorkerTaskStarterRunnable implements Runnable {
    public static final Logger LOG = LoggerFactory.getLogger(WorkerTaskStarterRunnable.class);

    private final TopologyEngine topologyEngine;
    private final String host;
    private final int port;
    private final String supervisorId;
    private final Map<String, Object> conf;
    private final IWorkerServer workerServer;

    private List<SpoutRunnable> spoutRunnables;
    private List<BoltRunnable> boltRunnables;

    private List<Thread> threadList;

    public WorkerTaskStarterRunnable(Map<String, Object> conf,
                                     String supervisorId,
                                     String host, int port,
                                     IWorkerServer iWorkerServer,
                                     TopologyEngine topologyEngine) {
        this.supervisorId = supervisorId;
        this.conf = conf;
        this.host = host;
        this.port = port;
        this.workerServer = iWorkerServer;

        this.topologyEngine = topologyEngine;

        this.spoutRunnables = new ArrayList<>();
        this.boltRunnables = new ArrayList<>();

        this.threadList = new ArrayList<>();
    }

    @Override
    public void run() {
        LOG.info( " WorkerTaskStarterRunnable run... ");
        List<TaskInfo> taskInfos = topologyEngine.getAssignmentInfo().getTaskInfoBySupervisorAndPort(supervisorId, port);
        List<StreamGraph> streamGraphs = new ArrayList<>();

        for (TaskInfo taskInfo : taskInfos) {
            for (int taskid = taskInfo.getStartId(); taskid < taskInfo.getEndId(); taskid++) {
                StreamGraph sg = topologyEngine.getStreamGraphRoot().findStreamGraphByTask(taskid);
                if (sg != null) {
                    streamGraphs.add(sg);
                }
            }
        }

        if (streamGraphs.size() == 0) {
            LOG.error(" worker_tasks_starter, but no stream graph found ");
            return;
        }

        for (StreamGraph sg : streamGraphs) {
            switch (sg.getType()) {
                case SPOUT:
                    SpoutRunnable spoutRunnable = new SpoutRunnable(
                            topologyEngine.getStreamGraphRoot(),
                            sg,
                            sg.getTaskId(),
                            port,
                            workerServer.newInputPoller(),
                            TopoCtxFactory.createTopologyContext(workerServer.getStormId(),
                                    sg.getTaskId(), port),
                            new DefaultSpoutOutputCollector(sg, workerServer));

                    spoutRunnables.add(spoutRunnable);
                    break;
                case BOLT:
                    BoltRunnable boltRunnable = new BoltRunnable(
                            topologyEngine.getStreamGraphRoot(),
                            sg, sg.getTaskId(), port,
                            workerServer.newInputPoller(),
                            TopoCtxFactory.createTopologyContext(workerServer.getStormId(),
                                    sg.getTaskId(), port),
                            new DefaultBoltOutputCollector(sg, workerServer)
                    );

                    boltRunnables.add(boltRunnable);
                    break;
            }
        }

        for (BoltRunnable bolt : boltRunnables) {
            bolt.prepare();
            threadList.add(new Thread(bolt));
        }

        for (SpoutRunnable spout : spoutRunnables) {
            spout.prepare();
            threadList.add(new Thread(spout));
        }

        for (Thread thread : threadList) {
            thread.start();
        }
    }

    public void stop() {
        for (Thread thread : threadList) {
            thread.interrupt();
            try {
                thread.join();
            } catch (InterruptedException e) {
                LOG.error(e.getMessage(), e);
            }
        }
    }
}
