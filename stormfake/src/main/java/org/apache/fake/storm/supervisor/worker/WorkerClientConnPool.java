package org.apache.fake.storm.supervisor.worker;

import org.apache.fake.storm.model.WorkerNodeInfo;
import org.apache.fake.storm.supervisor.worker.thriftimpl.WorkerThriftClient;
import org.apache.thrift.TException;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Created by yilong on 2017/10/7.
 */
public class WorkerClientConnPool {
    public static final org.slf4j.Logger LOG = LoggerFactory.getLogger(WorkerClientConnPool.class);
    private final String host;
    private final int port;
    private final Map<String, Object> conf;

    private ConcurrentHashMap<String, WorkerThriftClient> nodeClientConnMap;

    public WorkerClientConnPool(Map<String, Object> conf, String host, int port) {
        this.host = host;
        this.port = port;
        this.conf = conf;
        this.nodeClientConnMap = new ConcurrentHashMap();
    }

    public WorkerThriftClient getClientConn(WorkerNodeInfo nodeInfo) {
        String nodeHost = nodeInfo.getHostIp() + "_" + nodeInfo.getPort();
        String curHost = host + "_" + port;
        if (nodeHost.equals(curHost)) {
            return null;
        }

        WorkerThriftClient client = nodeClientConnMap.get(nodeHost);
        if (client == null) {
            client = new WorkerThriftClient(conf,
                    nodeInfo.getPort(),
                    nodeInfo.getHostIp());
        }

        client = reTryConn(client);
        if (client != null) {
            nodeClientConnMap.put(nodeHost, client);
        } else {
            nodeClientConnMap.remove(nodeHost);
        }

        return client;
    }

    public void close() {
        for (Map.Entry<String, WorkerThriftClient> entry : nodeClientConnMap.entrySet()) {
            try {
                entry.getValue().close();
            } catch (Exception e) {
                LOG.error(e.getMessage(), e);
            }
        }

        nodeClientConnMap.clear();
    }

    private WorkerThriftClient reTryConn(WorkerThriftClient client) {
        try {
            if (!client.isClosed()) {
                client.ping();
                return client;
            }
        } catch (TException e) {
            //LOG.error(e.getMessage(), e);
        } catch (IOException e) {
            //LOG.error(e.getMessage(), e);
        } catch (Exception e) {
            //
        }

        try {
            client.close();
        } catch (Exception e) {
            //LOG.error(e.getMessage(), e);
        }

        boolean conn = client.connect();
        int tryNum = 0;

        while (!conn && (tryNum++ < 3)) {
            try {
                Thread.sleep(1 * 1000);
            } catch (InterruptedException e) {
                //LOG.error(e.getMessage(), e);
            }
            conn = client.connect();
        }

        if (conn) {
            return client;
        }

        return null;
    }
}
