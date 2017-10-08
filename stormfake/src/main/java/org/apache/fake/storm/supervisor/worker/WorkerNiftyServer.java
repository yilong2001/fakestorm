package org.apache.fake.storm.supervisor.worker;

import com.facebook.nifty.core.*;
import com.facebook.nifty.guice.NiftyModule;
import com.google.inject.Guice;
import com.google.inject.Stage;
import com.lmax.disruptor.EventPoller;
import com.lmax.disruptor.RingBuffer;
import org.apache.fake.storm.auth.AuthUtil;
import org.apache.fake.storm.auth.ITransportInterceptor;
import org.apache.fake.storm.generated.Worker;
import org.apache.fake.storm.supervisor.worker.disruptor.MessageBox;
import org.apache.fake.storm.supervisor.worker.disruptor.MessageDisruptor;
import org.apache.fake.storm.supervisor.worker.disruptor.MessagePublisher;
import org.apache.fake.storm.supervisor.worker.stream.TopologyEngine;
import org.apache.fake.storm.supervisor.worker.tasks.WorkerTupleOutputRunnable;
import org.apache.fake.storm.supervisor.worker.tasks.WorkerTaskStarterRunnable;
import org.apache.fake.storm.supervisor.worker.thriftimpl.WorkerThriftServer;
import org.apache.fake.storm.utils.ClusterConfigUtil;
import org.slf4j.LoggerFactory;

import javax.inject.Provider;

import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Created by yilong on 2017/10/4.
 */
public class WorkerNiftyServer implements IWorkerServer {
    public static final org.slf4j.Logger LOG = LoggerFactory.getLogger(WorkerNiftyServer.class);

    private final Map<String, Object> conf;
    private final String supervisorId;
    private final String workerId;
    private final int port;
    private final String host;

    private final ITransportInterceptor transportInterceptor;

    private final MessageDisruptor<List<Object>> inputDisruptor;
    private final MessageDisruptor<List<Object>> outputDisruptor;

    private final RingBuffer<MessageBox<List<Object>>> inputRingBuffer;
    private final MessagePublisher<List<Object>> inputPublisher;

    private final RingBuffer<MessageBox<List<Object>>> outputRingBuffer;
    private final MessagePublisher<List<Object>> outputPublisher;

    private final AtomicReference<TopologyEngine> topologyEngineAtomic;

    private final Thread tupleOutputThread;

    private final WorkerTupleOutputRunnable workerTupleOutputRunnable;

    private NiftyBootstrap bootstrap = null;

    private WorkerTaskStarterRunnable workerTaskStarterRunnable;

    private final IWorkerServer iWorkerServer;

    public WorkerNiftyServer(Map<String, Object> conf,
                             String host, int port, String supervisorId, String workerId) {
        this.conf = conf;
        this.transportInterceptor = AuthUtil.getTransportInterceptor(conf);
        this.port = port;
        this.host = host;
        this.supervisorId = supervisorId;
        this.workerId = workerId;

        this.inputDisruptor = new MessageDisruptor();
        this.outputDisruptor = new MessageDisruptor();

        this.inputRingBuffer = inputDisruptor.getRingBuffer();
        this.inputPublisher = inputDisruptor.newPublisher();

        this.outputRingBuffer = outputDisruptor.getRingBuffer();
        this.outputPublisher = outputDisruptor.newPublisher();

        this.topologyEngineAtomic = new AtomicReference();


        this.workerTaskStarterRunnable = null;

        iWorkerServer = this;

        this.workerTupleOutputRunnable = new WorkerTupleOutputRunnable(outputDisruptor.newPoller(),
                new WorkerClientConnPool(conf, host, port), host, port, iWorkerServer);

        this.tupleOutputThread = new Thread(workerTupleOutputRunnable);

    }

    @Override
    public void start() {
        startNiftyServer();
    }

    @Override
    public void stop() {
        if (bootstrap != null) {
            bootstrap.stop();
            bootstrap = null;
        }

        workerTupleOutputRunnable.active(false);

        tupleOutputThread.interrupt();
        try {
            tupleOutputThread.join();
        } catch (InterruptedException e) {
            LOG.error(e.getMessage(), e);
        }

        if (workerTaskStarterRunnable == null) {
            //
        } else {
            workerTaskStarterRunnable.stop();
            workerTaskStarterRunnable = null;
        }
    }

    private void startNiftyServer() {
        final WorkerThriftServer thriftServer = new WorkerThriftServer(host, port,
                ClusterConfigUtil.getStormId(conf),
                supervisorId,
                workerId,
                conf,
                this);

        bootstrap = Guice.createInjector(Stage.PRODUCTION,
                new NiftyModule() {
                    @Override
                    protected void configureNifty() {
                        bind().toInstance(new ThriftServerDefBuilder()
                                .listen(port)
                                .withProcessor(new Worker.Processor(thriftServer))
                                .build()
                        );
                        withNettyServerConfig(NettyConfigProvider.class);
                    }
                }
        ).getInstance(NiftyBootstrap.class);

        bootstrap.start();

        // Arrange to stop the server at shutdown
        Runtime.getRuntime().addShutdownHook(new Thread() {
            @Override
            public void run() {
                try {
                    bootstrap.stop();
                    bootstrap = null;
                } catch (Exception e) {
                    Thread.currentThread().interrupt();
                }
            }
        });
    }

    @Override
    public String getStormId() {
        return ClusterConfigUtil.getStormId(conf);
    }

    @Override
    public String getHostIp() {
        return host;
    }

    @Override
    public int getPort() {
        return port;
    }

    @Override
    public String getSupervisorId() {
        return supervisorId;
    }

    @Override
    public String getWorkerId() {
        return workerId;
    }

    @Override
    public Map<String, Object> getConf() {
        return conf;
    }

    public static class NettyConfigProvider implements Provider<NettyServerConfig> {
        @Override
        public NettyServerConfig get() {
            NettyServerConfigBuilder nettyConfigBuilder = new NettyServerConfigBuilder();
            nettyConfigBuilder.getSocketChannelConfig().setTcpNoDelay(true);
            nettyConfigBuilder.getSocketChannelConfig().setConnectTimeoutMillis(5000);
            nettyConfigBuilder.getSocketChannelConfig().setTcpNoDelay(true);
            return nettyConfigBuilder.build();
        }
    }

    @Override
    public void sendTuple(int taskId, List<Object> tuple, String messageId) {
        LOG.info(" WorkerNiftyServer_sendTuple:"+host+":"+port+":"+taskId+":"+tuple);
        outputRingBuffer.publishEvent(outputPublisher, tuple, taskId, messageId);
    }

    @Override
    public void receiveTuple(int taskId, List<Object> tuple, String messageId) {
        LOG.info(" WorkerNiftyServer_receiveTuple:"+host+":"+port+":"+taskId+":"+tuple);
        inputRingBuffer.publishEvent(inputPublisher, tuple, taskId, messageId);
    }

    @Override
    public void setTopologyEngine(TopologyEngine topologyEngine) {
        topologyEngineAtomic.set(topologyEngine);

        workerTupleOutputRunnable.setAssignmentInfo(topologyEngine.getAssignmentInfo());
        workerTupleOutputRunnable.active(true);

        if (!tupleOutputThread.isAlive()) {
            tupleOutputThread.start();
        }

        if (workerTaskStarterRunnable == null) {
            //
        } else {
            workerTaskStarterRunnable.stop();
            workerTaskStarterRunnable = null;
        }

        workerTaskStarterRunnable = new WorkerTaskStarterRunnable(conf, supervisorId,
                host, port, iWorkerServer, topologyEngine);

        new Thread(workerTaskStarterRunnable).start();
    }

    @Override
    public TopologyEngine getTopologyEngine(){
        return topologyEngineAtomic.get();
    }

    public final RingBuffer<MessageBox<List<Object>>> getOutputRingBuffer() {
        return outputDisruptor.getRingBuffer();
    }

    public final MessagePublisher<List<Object>> newOutputPublisher() {
        return new MessagePublisher<List<Object>>();
    }

    @Override
    public final EventPoller<MessageBox<List<Object>>> newInputPoller() {
        return inputDisruptor.newPoller();
    }
}
