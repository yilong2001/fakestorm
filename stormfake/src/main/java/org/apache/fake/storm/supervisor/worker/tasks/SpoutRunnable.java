package org.apache.fake.storm.supervisor.worker.tasks;

import com.lmax.disruptor.EventPoller;
import org.apache.fake.storm.supervisor.worker.disruptor.MessageBox;
import org.apache.fake.storm.supervisor.worker.stream.StreamGraph;
import org.apache.fake.storm.supervisor.worker.stream.StreamGraphRoot;
import org.apache.storm.spout.ISpoutOutputCollector;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.base.BaseRichSpout;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/**
 * Created by yilong on 2017/10/5.
 */
public class SpoutRunnable extends BaseRunnable {
    private final BaseRichSpout exec;
    public static final Logger LOG = LoggerFactory.getLogger(BoltRunnable.class);

    private final EventPoller<MessageBox<List<Object>>> poller;

    private final TopologyContext topologyContext;
    private final ISpoutOutputCollector outputCollector;

    private volatile boolean isActive = true;
    final int taskId;
    final int port;

    static final int TASK_SLEEP_INTERVAL = 100;

    public SpoutRunnable(StreamGraphRoot streamGraphRoot,
                         StreamGraph streamGraph,
                         int taskId,
                         int port,
                         EventPoller<MessageBox<List<Object>>> poller,
                         TopologyContext topologyContext,
                         ISpoutOutputCollector outputCollector) {
        super(streamGraphRoot, streamGraph);
        this.exec = (BaseRichSpout)streamGraph.getObject();
        this.poller = poller;
        this.taskId = taskId;
        this.port = port;
        this.topologyContext = topologyContext;
        this.outputCollector = outputCollector;
    }

    @Override
    public void prepare() {
        this.exec.open(null, topologyContext, new SpoutOutputCollector(outputCollector));
    }

    @Override
    public void run() {
        while( isActive) {
            try {
                LOG.info(" SpoutRunnable_at_port_task : "+ port +":" + taskId);
                Thread.sleep(TASK_SLEEP_INTERVAL);

                this.exec.nextTuple();
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
