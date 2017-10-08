package org.apache.fake.storm.supervisor.worker;

import org.apache.fake.storm.auth.AuthUtil;
import org.apache.fake.storm.auth.ITransportInterceptor;
import org.apache.fake.storm.generated.Worker;
import org.apache.fake.storm.supervisor.worker.thriftimpl.WorkerThriftServer;
import org.apache.fake.storm.utils.ClusterConfigUtil;
import org.apache.thrift.TProcessor;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.server.TServer;
import org.apache.thrift.server.TThreadPoolServer;
import org.apache.thrift.transport.TServerSocket;
import org.apache.thrift.transport.TTransportException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * Created by yilong on 2017/9/11.
 */
public class WorkerServer {
    public static final Logger LOG = LoggerFactory.getLogger(WorkerServer.class);

    private final Map<String, Object> conf;
    private TServer server;
    private final int port;
    private final String host;
    private final int numWorkerThreads = 5;
    private final int blockQueueSize = 10;
    private final String stormId = "";
    private final String supervisorId = "";
    private final String workerId = "";

    private ITransportInterceptor transportInterceptor;
    public WorkerServer(Map<String, Object> conf, String host, int port) {
        this.conf = conf;
        this.transportInterceptor = AuthUtil.getTransportInterceptor(conf);
        this.port = port;
        this.host = host;
    }

    public void start() {
        startThriftServer();
    }

    private void startThriftServer() {
        try {
            LOG.info("WorkerServer startThriftServer begin ... ");
            TServerSocket serverTransport = new TServerSocket(port);
            TBinaryProtocol.Factory proFactory = new TBinaryProtocol.Factory();
            TProcessor processor = new Worker.Processor(
                    new WorkerThriftServer(host, port,
                            ClusterConfigUtil.getStormId(conf),
                            supervisorId,
                            workerId,
                            conf, null));

            BlockingQueue workQueue = new ArrayBlockingQueue(blockQueueSize);
            ThreadPoolExecutor executorService = new ThreadPoolExecutor(numWorkerThreads, numWorkerThreads,
                    60, TimeUnit.SECONDS, workQueue);

            TThreadPoolServer.Args args = new TThreadPoolServer.Args(serverTransport);
            args.processor(processor);
            args.protocolFactory(proFactory);
            args.executorService(executorService);
            args.transportFactory(transportInterceptor.getServerTransportFactory());

            server = new TThreadPoolServer(args);
            LOG.info("start to worker server : " + port);
            server.serve();
            LOG.info("WorkerServer startThriftServer end ... ");
        } catch (TTransportException e) {
            LOG.error(e.getMessage(), e);
        } catch (IOException e) {
            LOG.error(e.getMessage(), e);
        }
    }

    public void stop() {

    }
}
