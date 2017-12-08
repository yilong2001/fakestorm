package org.apache.fake.storm.supervisor.worker.thriftimpl;

import org.apache.fake.storm.auth.DefaultTransportInterceptor;
import org.apache.fake.storm.auth.ITransportInterceptor;
import org.apache.fake.storm.generated.AuthorizationException;
import org.apache.fake.storm.generated.FileOperationException;
import org.apache.fake.storm.generated.Worker;
import org.apache.fake.storm.thriftutil.ThriftClient;
import org.apache.fake.storm.utils.Serde;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Created by yilong on 2017/8/29.
 */
public class WorkerThriftClient implements AutoCloseable {
    public static final Logger LOG = LoggerFactory.getLogger(WorkerThriftClient.class);
    private Worker.Client workerClient = null;
    private ThriftClient thriftClient = null;
    
    private final int DefaultBufferSize = 102400;
    private final Map<String, Object> conf;
    private final int port;
    private final String workerHost;

    private AtomicReference<String> workerThriftServerSessionId = new AtomicReference();

    public WorkerThriftClient(Map<String, Object> conf, int port, String hostIp) {
        this.conf = conf;
        this.port = port;
        this.workerHost = hostIp;
    }

    @Override
    public void close() throws Exception {
        try {
            if (thriftClient != null) {
                thriftClient.close();
                thriftClient = null;
            }
        } catch (Exception e) {
            //
        }

        if (workerClient != null) {
            workerClient = null;
        }
    }

    public boolean connect() {
        thriftClient = null;
        workerClient = null;

        ITransportInterceptor transportInterceptor = new DefaultTransportInterceptor(conf);
        //AuthUtil.getTransportInterceptor(conf);

        try {
            thriftClient = new ThriftClient(workerHost, port, transportInterceptor);
        } catch (Exception e) {
            LOG.error(e.getMessage(), e);
            throw new RuntimeException(e.getMessage());
        }

        try {
            thriftClient = new ThriftClient(workerHost, port, transportInterceptor);
            workerClient = new Worker.Client(thriftClient.getProtocol());
        } catch (Exception e) {
            thriftClient.close();
            thriftClient = null;
            workerClient = null;
            LOG.error(e.getMessage(), e);
            throw new RuntimeException(e.getMessage());
        }

        return true;
    }

    public boolean isClosed() {
        return (thriftClient == null);
    }

    public void setAssignmentPath(String stormId, String topoId) throws
            AuthorizationException, FileOperationException, TException, Exception {
        String sessionId = workerThriftServerSessionId.get();
        if (sessionId == null) {
            workerThriftServerSessionId.compareAndSet(sessionId, this.workerClient.getConnectionId());
        }

        this.workerClient.setAssignmentPath(sessionId, stormId, topoId);
    }

    public boolean ping() throws
            AuthorizationException, FileOperationException, TException, IOException {
        String sessionId = workerThriftServerSessionId.get();
        if (sessionId == null) {
            workerThriftServerSessionId.compareAndSet(sessionId, this.workerClient.getConnectionId());
        }

        return this.workerClient.ping(workerThriftServerSessionId.get());
    }

    public boolean sendMessage(int taskId, List<Object> tuple, String messageId) throws
            AuthorizationException, FileOperationException, TException, IOException {
        String sessionId = workerThriftServerSessionId.get();
        if (sessionId == null) {
            workerThriftServerSessionId.compareAndSet(sessionId, this.workerClient.getConnectionId());
        }

        if (tuple == null || tuple.size() == 0) {
            LOG.warn(" WorkerThriftClient_sendMessage, but tuple is null or empty , taskId : " + taskId);
            return false;
        }

        String str = null;
        try {
            str = Serde.collectionSerialize(tuple);
            if (str == null) {
                LOG.warn(" WorkerThriftClient_sendMessage, but collectionSerialize_null , taskId : " + taskId);
                return false;
            }
        } catch (Exception e) {
            LOG.error(e.getMessage(), tuple);
            throw new FileOperationException(" WorkerThriftClient_sendMessage, but collection_serialize_failed ......");
        }

        return this.workerClient.sendMessage(workerThriftServerSessionId.get(), str, taskId, messageId);
    }
}
