package org.apache.fake.storm.nimbus;

import org.apache.fake.storm.auth.ITransportInterceptor;
import org.apache.fake.storm.generated.Nimbus;
import org.apache.fake.storm.nimbus.thriftimpl.NimbusThriftServer;
import org.apache.fake.storm.auth.AuthUtil;
import org.apache.fake.storm.store.IResourceStore;
import org.apache.fake.storm.store.RedisStore;
import org.apache.fake.storm.utils.ClusterConfigUtil;
import org.apache.fake.storm.utils.NimbusConfigUtil;
import org.apache.fake.storm.utils.QueuedTimer;
import org.apache.thrift.TProcessor;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.server.TServer;
import org.apache.thrift.server.TThreadPoolServer;
import org.apache.thrift.transport.TServerSocket;
import org.apache.thrift.transport.TTransportException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetAddress;
import java.util.Map;
import java.util.concurrent.*;

/**
 * Created by yilong on 2017/6/29.
 */
public class NimbusServer {
    public static final Logger LOG = LoggerFactory.getLogger(NimbusServer.class);

    private final Map<String, Object> conf;
    private TServer server;

    private ITransportInterceptor transportInterceptor;
    private IResourceStore resourceStore;
    private QueuedTimer schedulerTimer;

    private final int TimerPeriod = 10*1000;

    public NimbusServer(Map<String, Object> conf) {
        this.conf = conf;
        this.transportInterceptor = AuthUtil.getTransportInterceptor(conf);
        this.resourceStore = new RedisStore(conf);
        schedulerTimer = new QueuedTimer("nimbus_master_scheduler_heartbeat");
    }

    private void startThriftServer() throws TTransportException, IOException {
        TServerSocket serverTransport = new TServerSocket(NimbusConfigUtil.getNimbusThriftPort(conf));
        TBinaryProtocol.Factory proFactory = new TBinaryProtocol.Factory();
        TProcessor processor = new Nimbus.Processor(
                new NimbusThriftServer(conf, ClusterConfigUtil.getStormId(conf),
                        InetAddress.getLocalHost().getHostAddress(),
                        NimbusConfigUtil.getNimbusThriftPort(conf),
                        resourceStore));

        BlockingQueue workQueue = new ArrayBlockingQueue(NimbusConfigUtil.getNimbusBlockQueueSize(conf));
        int numWorkerThreads = NimbusConfigUtil.getNimbusWorkerThreadNum(conf);

        ThreadPoolExecutor executorService = new ThreadPoolExecutor(numWorkerThreads, numWorkerThreads,
                60, TimeUnit.SECONDS, workQueue);

        TThreadPoolServer.Args args = new TThreadPoolServer.Args(serverTransport);
        args.processor(processor);
        args.protocolFactory(proFactory);
        args.executorService(executorService);
        args.transportFactory(transportInterceptor.getServerTransportFactory());

        server = new TThreadPoolServer(args);
        LOG.info("start to nimbus server : " + NimbusConfigUtil.getNimbusThriftPort(conf));

        server.serve();
    }

    public void start() {
        try {
            schedulerTimer.schedule(TimerPeriod, new SchedulerTask(new DefaultScheduler(conf)));

            LOG.info(" Nimbus Server start begin ... ");

            startThriftServer();

            LOG.info(" Nimbus Server start end ... ");
        } catch (TTransportException e) {
            LOG.error(e.getMessage(), e);
        } catch (IOException e) {
            LOG.error(e.getMessage(), e);
        }
    }

    public void stop() {
        //TODO:
        try {
            schedulerTimer.close();
        } catch (Exception e) {
            LOG.error(e.getMessage(), e);
        }
    }
}
