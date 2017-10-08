package org.apache.fake.storm.supervisor.worker;

import com.lmax.disruptor.EventPoller;
import com.lmax.disruptor.RingBuffer;
import org.apache.fake.storm.supervisor.worker.disruptor.MessageBox;
import org.apache.fake.storm.supervisor.worker.disruptor.MessagePublisher;
import org.apache.fake.storm.supervisor.worker.stream.TopologyEngine;

import java.util.List;
import java.util.Map;

/**
 * Created by yilong on 2017/9/11.
 */
public interface IWorkerServer {
    public void start();
    public void stop();

    public String getStormId();
    public String getHostIp();
    public int getPort();
    public String getSupervisorId();
    public String getWorkerId();
    public Map<String, Object> getConf();

    public void setTopologyEngine(TopologyEngine topologyEngine);
    public TopologyEngine getTopologyEngine();

    public void sendTuple(int taskId, List<Object> tuple, String messageId);
    public void receiveTuple(int taskId, List<Object> tuple, String messageId);

    public EventPoller<MessageBox<List<Object>>> newInputPoller();
}
