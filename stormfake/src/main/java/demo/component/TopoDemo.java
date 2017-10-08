package demo.component;

import demo.server.ConfigMain;
import org.apache.fake.storm.store.RedisStore;
import org.apache.fake.storm.supervisor.worker.stream.StreamGraphRoot;
import org.apache.fake.storm.supervisor.worker.stream.TopologyEngine;
import org.apache.fake.storm.supervisor.worker.stream.TopologyGetter;
import org.apache.fake.storm.utils.ClusterConfigUtil;
import org.apache.storm.generated.StormTopology;

import java.util.Map;

/**
 * Created by yilong on 2017/10/8.
 */
public class TopoDemo {
    private static Map<String, Object> conf = ConfigMain.getConf();

    public static void main(String[] args) throws Exception {
        TopologyGetter topologyGetter = new TopologyGetter(ClusterConfigUtil.getStormId(conf),
                new RedisStore(conf));

        StormTopology topology = topologyGetter.get();

        StreamGraphRoot root = TopologyEngine.processTopology(topology);
        String sb = root.showGraph();

        System.out.println(sb.toString());
    }
}
