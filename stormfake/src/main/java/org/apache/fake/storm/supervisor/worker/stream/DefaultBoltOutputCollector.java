package org.apache.fake.storm.supervisor.worker.stream;

import com.lmax.disruptor.RingBuffer;
import org.apache.fake.storm.supervisor.worker.IWorkerServer;
import org.apache.fake.storm.supervisor.worker.disruptor.MessageBox;
import org.apache.fake.storm.supervisor.worker.disruptor.MessagePublisher;
import org.apache.storm.task.IOutputCollector;
import org.apache.storm.tuple.Tuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

/**
 * Created by yilong on 2017/10/7.
 */
public class DefaultBoltOutputCollector implements IOutputCollector {
    public static final Logger LOG = LoggerFactory.getLogger(DefaultSpoutOutputCollector.class);

    final private StreamGraph streamGraph;

    final private IWorkerServer iWorkerServer;

    public DefaultBoltOutputCollector(StreamGraph streamGraph,
                                      IWorkerServer iWorkerServer) {
        this.streamGraph = streamGraph;
        this.iWorkerServer = iWorkerServer;
    }

    @Override
    public List<Integer> emit(String streamId, Collection<Tuple> anchors, List<Object> tuple) {
        List<Integer> res = new ArrayList<>();

        List<Integer> taskIds = streamGraph.getChildStreamTasks(streamId);
        if (taskIds == null || taskIds.size() == 0) {
            LOG.warn(" streamId = " + streamId + ", no_stream_child!");
            res.add(-1);
            return res;
        }

        //List<WorkerNodeInfo> nodeInfos = assignmentInfo.getWorkerNodesByTaskIds(taskIds);
        //TODO: message should be Object type
        //TODO: skip anchors
        for (Integer task : taskIds) {
            iWorkerServer.sendTuple(task, tuple, "messageNoId");
        }

        return res;
    }

    @Override
    public void emitDirect(int i, String s, Collection<Tuple> collection, List<Object> list) {
        LOG.error("emitDirect do not support ... ");
    }

    @Override
    public void ack(Tuple tuple) {

    }

    @Override
    public void fail(Tuple tuple) {

    }

    @Override
    public void resetTimeout(Tuple tuple) {

    }

    @Override
    public void reportError(Throwable throwable) {

    }
}
