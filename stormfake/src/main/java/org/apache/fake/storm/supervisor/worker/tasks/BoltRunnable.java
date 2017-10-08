package org.apache.fake.storm.supervisor.worker.tasks;

import com.lmax.disruptor.EventPoller;
import org.apache.fake.storm.supervisor.worker.disruptor.DisruptorUtil;
import org.apache.fake.storm.supervisor.worker.disruptor.MessageBox;
import org.apache.fake.storm.supervisor.worker.stream.SimpleTuple;
import org.apache.fake.storm.supervisor.worker.stream.StreamGraph;
import org.apache.fake.storm.supervisor.worker.stream.StreamGraphRoot;
import org.apache.storm.task.IOutputCollector;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.BasicBoltExecutor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/**
 * Created by yilong on 2017/10/5.
 */
public class BoltRunnable extends BaseRunnable {
    public static final Logger LOG = LoggerFactory.getLogger(BoltRunnable.class);

    private final BasicBoltExecutor exec;
    private final EventPoller<MessageBox<List<Object>>> poller;

    private final TopologyContext topologyContext;
    private final IOutputCollector outputCollector;
    private final int taskId;
    private final int port;

    private volatile boolean isActive = true;

    static final int TASK_SLEEP_INTERVAL = 100;

    public BoltRunnable(StreamGraphRoot streamGraphRoot,
                        StreamGraph streamGraph,
                        int taskId,
                        int port,
                        EventPoller<MessageBox<List<Object>>> poller,
                        TopologyContext topologyContext,
                        IOutputCollector outputCollector) {
        super(streamGraphRoot, streamGraph);
        this.taskId = taskId;
        this.exec = (BasicBoltExecutor)streamGraph.getObject();
        this.poller = poller;
        this.topologyContext = topologyContext;
        this.outputCollector = outputCollector;
        this.port = port;
    }

    @Override
    public void prepare() {
        this.exec.prepare(null, topologyContext, new OutputCollector(outputCollector));
    }

    @Override
    public void run() {
        while(isActive) {
            try {
                LOG.info("  BoltRunnable_at_port_task : "+ port +":" + taskId);
                Thread.sleep(TASK_SLEEP_INTERVAL);

                MessageBox<List<Object>> value = DisruptorUtil.getNextValue(poller);
                if (value == null) {
                    LOG.warn("  DisruptorUtil.getNextValue, but value=null ");
                    continue;
                } else {
                }

                if (value.tuple == null) {
                    LOG.warn(" DisruptorUtil.getNextValue, but value.tuple=null ");
                    continue;
                }

                if (value.taskId != taskId) {
                    continue;
                }

                //TODO: streamId ???
                SimpleTuple tuple = new SimpleTuple(this.streamGraph.getComponentId(),
                        "TBD",
                        value.taskId,
                        value.tuple,
                        null);

                this.exec.execute(tuple);

            } catch (InterruptedException e) {
                LOG.error(e.getMessage(), e);
            } catch (Exception e) {
                LOG.error(e.getMessage(), e);
            }
        }
    }

    public void stop() {
        isActive = false;
    }
}
