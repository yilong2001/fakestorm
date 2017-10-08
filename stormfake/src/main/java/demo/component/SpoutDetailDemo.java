package demo.component;

import org.apache.storm.generated.*;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.Map;

import static org.mockito.Mockito.mock;

/**
 * Created by yilong on 2017/10/6.
 */
public class SpoutDetailDemo {
    public static final Logger LOG = LoggerFactory.getLogger(SpoutDetailDemo.class);

    public static void showSpout(String key, SpoutSpec spout) {
        LOG.warn("^^^^^^^^^^^^^   "+key+"  ^^^^^^^^^^^^^");
        Map<String, StreamInfo> ssinfo = spout.get_common().get_streams();

        try {
            Object obj = Utils.javaDeserialize(spout.get_spout_object().get_serialized_java(), Serializable.class);
            LOG.warn(obj.toString());
            BaseRichSpout exec = (BaseRichSpout) obj;

            Map conf = mock(Map.class);
            TopologyContext context = mock(TopologyContext.class);
            SpoutOutputCollector collector = mock(SpoutOutputCollector.class);
            exec.open(conf, context, collector);

            exec.nextTuple();
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

        LOG.warn(" ^^^^^^^^^^^^^ stream and group ^^^^^^^^^^^^^ ");
        Map<GlobalStreamId, Grouping> gginfo = spout.get_common().get_inputs();
        for (Map.Entry<GlobalStreamId, Grouping> tmp : gginfo.entrySet()) {
            String out = tmp.getValue().getSetField().getFieldName();
            LOG.warn(tmp.getKey().get_componentId()+":"+tmp.getKey().get_streamId()+":"+out);
        }
        LOG.warn(" ^^^^^^^^^^^^^ ");
    }
}
