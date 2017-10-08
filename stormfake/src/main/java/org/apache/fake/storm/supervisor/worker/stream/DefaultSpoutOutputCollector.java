package org.apache.fake.storm.supervisor.worker.stream;

import com.lmax.disruptor.RingBuffer;
import org.apache.fake.storm.supervisor.worker.IWorkerServer;
import org.apache.fake.storm.supervisor.worker.disruptor.MessageBox;
import org.apache.fake.storm.supervisor.worker.disruptor.MessagePublisher;
import org.apache.storm.spout.ISpoutOutputCollector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by yilong on 2017/10/7.
 */
public class DefaultSpoutOutputCollector implements ISpoutOutputCollector {
    public static final Logger LOG = LoggerFactory.getLogger(DefaultSpoutOutputCollector.class);

    final private StreamGraph streamGraph;

    private final IWorkerServer iWorkerServer;

    public DefaultSpoutOutputCollector(StreamGraph streamGraph,
                                       IWorkerServer iWorkerServer) {
        this.streamGraph = streamGraph;
        this.iWorkerServer = iWorkerServer;
    }

    @Override
    public List<Integer> emit(String streamId, List<Object> tuple, Object messageId) {
        List<Integer> res = new ArrayList<>();

        List<Integer> taskIds = streamGraph.getChildStreamTasks(streamId);
        if (taskIds == null || taskIds.size() == 0) {
            LOG.warn(" streamId = " + streamId + ", no_stream_child!");
            res.add(-1);
            return res;
        }

        //List<WorkerNodeInfo> nodeInfos = assignmentInfo.getWorkerNodesByTaskIds(taskIds);
        //TODO: message should be Object type
        for (Integer task : taskIds) {
            if (iWorkerServer == null) {
                throw new RuntimeException(" iWorkerServer_is_null ");
            }
            iWorkerServer.sendTuple(task, tuple, "unknownid");
        }

        return res;
    }

    @Override
    public void emitDirect(int taskId, String streamId, List<Object> tuple, Object messageId) {
        LOG.error("emitDirect do not support ... ");
    }

    @Override
    public long getPendingCount() {
        return 0;
    }

    @Override
    public void reportError(Throwable throwable) {

    }
}
