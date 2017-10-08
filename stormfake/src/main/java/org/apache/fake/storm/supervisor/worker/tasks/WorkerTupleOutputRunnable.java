package org.apache.fake.storm.supervisor.worker.tasks;

import com.lmax.disruptor.EventPoller;
import org.apache.fake.storm.model.WorkerAssignmentInfo;
import org.apache.fake.storm.model.WorkerNodeInfo;
import org.apache.fake.storm.supervisor.worker.thriftimpl.WorkerThriftClient;
import org.apache.fake.storm.supervisor.worker.IWorkerServer;
import org.apache.fake.storm.supervisor.worker.WorkerClientConnPool;
import org.apache.fake.storm.supervisor.worker.disruptor.DisruptorUtil;
import org.apache.fake.storm.supervisor.worker.disruptor.MessageBox;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Created by yilong on 2017/10/7.
 */
public class WorkerTupleOutputRunnable implements Runnable {
    public static final Logger LOG = LoggerFactory.getLogger(WorkerTupleOutputRunnable.class);

    final EventPoller<MessageBox<List<Object>>> outputPoller;
    final WorkerClientConnPool clientConnPool;

    private AtomicReference<Boolean> isActive = new AtomicReference<>();
    private AtomicReference<WorkerAssignmentInfo> assignmentInfoAtomic;

    private final String host;
    private final int port;
    private final IWorkerServer workerServer;

    public WorkerTupleOutputRunnable(final EventPoller<MessageBox<List<Object>>> p,
                                     WorkerClientConnPool pool,
                                     String host,
                                     int port,
                                     IWorkerServer workerServer) {
        outputPoller = p;
        clientConnPool = pool;
        isActive.set(false);
        assignmentInfoAtomic = new AtomicReference<>();

        this.host = host;
        this.port = port;
        this.workerServer = workerServer;
    }

    @Override
    public void run() {
        while(isActive.get()) {
            try {
                Thread.sleep(100);

                //TODO: to simple process
                WorkerAssignmentInfo assignmentInfo = assignmentInfoAtomic.get();

                MessageBox<List<Object>> value = DisruptorUtil.getNextValue(outputPoller);
                WorkerNodeInfo nodeInfo = assignmentInfo.getWorkerNodeByTaskId(value.taskId);
                if (nodeInfo == null) {
                    LOG.error(value.taskId + " can not get worker node info ...");
                    continue;
                }

                if (nodeInfo.getHostIp().equals(host) && nodeInfo.getPort() == port) {
                    LOG.warn(" send_to_localport : " + host + ":" + port + ":" + value.taskId);
                    workerServer.receiveTuple(value.taskId, value.tuple, value.messageId);
                    continue;
                }

                //TODO: should confirm inner or outter router
                WorkerThriftClient client = null;
                try {
                    client = clientConnPool.getClientConn(nodeInfo);
                    if (client == null) {
                        LOG.error("from:"+host+":"+port+"; to:"+nodeInfo.getHostIp() + ":" + nodeInfo.getPort() +":"+value.taskId+ ", getClientConnFailed ...");
                        continue;
                    }
                } catch (Exception e) {
                    LOG.error("from:"+host+":"+port+"; to:"+nodeInfo.getHostIp() + ":" + nodeInfo.getPort() +":"+value.taskId+ ": getClientConnError", e);
                    continue;
                }

                LOG.info(" willSendMessage to " + nodeInfo.getHostIp() + ":" + nodeInfo.getPort() + " ");
                client.sendMessage(value.taskId, value.tuple, value.messageId);
            } catch (Exception e) {
                LOG.error(e.getMessage(), e);
            }
        }
    }

    public void setAssignmentInfo(WorkerAssignmentInfo info) {
        assignmentInfoAtomic.set(info);
    }

    public void active(boolean act) {
        isActive.set(act);
    }
}
