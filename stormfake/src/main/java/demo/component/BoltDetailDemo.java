package demo.component;

import org.apache.storm.Config;
import org.apache.storm.generated.Bolt;
import org.apache.storm.generated.GlobalStreamId;
import org.apache.storm.generated.Grouping;
import org.apache.storm.generated.StreamInfo;
import org.apache.storm.task.GeneralTopologyContext;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.BasicBoltExecutor;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.TupleImpl;
import org.apache.storm.tuple.Values;
import org.apache.storm.utils.Utils;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.mockito.Mockito.*;

/**
 * Created by yilong on 2017/10/6.
 */
public class BoltDetailDemo {
    public static final Logger LOG = LoggerFactory.getLogger(BoltDetailDemo.class);

    public static Tuple generateTestTuple() {
        TopologyBuilder builder = new TopologyBuilder();
        GeneralTopologyContext topologyContext = new GeneralTopologyContext(builder.createTopology(),
                new Config(), new HashMap(), new HashMap(), new HashMap(), "") {
            @Override
            public Fields getComponentOutputFields(String componentId, String streamId) {
                return new Fields("record");
            }
        };
        return new TupleImpl(topologyContext, new Values("hello"), 1, "");
    }

    public static void showBolt(String key, Bolt bolt) {
        LOG.warn("^^^^^^^^^^^^^   " + key + "  ^^^^^^^^^^^^^ ");
        Map<String, StreamInfo> ssinfo = bolt.get_common().get_streams();

        try {
            Object obj = Utils.javaDeserialize(bolt.get_bolt_object().get_serialized_java(), Serializable.class);
            LOG.warn(obj.toString());
            BasicBoltExecutor exec = (BasicBoltExecutor) obj;

            Map conf = mock(Map.class);
            TopologyContext context = mock(TopologyContext.class);
            OutputCollector collector = mock(OutputCollector.class);
            exec.prepare(conf, context, collector);

            exec.execute(generateTestTuple());
        } catch (Exception e) {
            LOG.error(e.getMessage(), e);
        }

        for (Map.Entry<String, StreamInfo> tmp : ssinfo.entrySet()) {
            String out = "";
            for (String s : tmp.getValue().get_output_fields()) {
                out += s+", ";
            }
            LOG.warn(tmp.getKey()+":"+out);
        }
        LOG.warn(" ^^^^^^^^^^^^^ stream id and group ^^^^^^^^^^^^^ ");
        Map<GlobalStreamId, Grouping> gginfo = bolt.get_common().get_inputs();
        for (Map.Entry<GlobalStreamId, Grouping> tmp : gginfo.entrySet()) {
            String out = tmp.getValue().getSetField().getFieldName();
            //for (String s : tmp.getValue().getSetField()) {
            //    out += s+", ";
            //}
            LOG.warn(tmp.getKey().get_componentId()+":"+tmp.getKey().get_streamId()+":"+out);
        }
        LOG.warn(" ^^^^^^^^^^^^^ ");
    }
}
