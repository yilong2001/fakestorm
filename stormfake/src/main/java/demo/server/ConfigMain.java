package demo.server;

import java.util.HashMap;
import java.util.Map;

import static org.apache.fake.storm.utils.ClusterConfigUtil.*;
import static org.apache.fake.storm.utils.NimbusConfigUtil.NIMBUS_THRIFT_HOSTS;
import static org.apache.fake.storm.utils.NimbusConfigUtil.NIMBUS_THRIFT_PORT;

/**
 * Created by yilong on 2017/9/17.
 */
public class ConfigMain {
    public static final Map<String, Object> getConf() {
        System.setProperty("user", "Guset");
        System.setProperty(STORM_ROOT_DIR, "/Users/yilong/setup/apache-storm-1.1.0");

        Map<String,Object> conf = new HashMap<>();
        //conf.put("java.security.auth.login.config", "/Users/yilong/setup/keytab/jaas.conf");
        conf.put(STORM_LOCAL_DIR, "/Users/yilong/setup/apache-storm-1.1.0/fakelocal");
        conf.put(SUPERVISOR_PORTS, "9061,9062");
        conf.put(NIMBUS_THRIFT_PORT, "8191");
        conf.put(NIMBUS_THRIFT_HOSTS, "localhost");

        conf.put(CLUSTER_STORM_ID, "fakeid1");

        conf.put(FAKE_STORM_JAR_PATH, "/Users/yilong/work/bigdata/code/stream/stormfake/target/stormfake-1.0-SNAPSHOT-jar-with-dependencies.jar");

        return conf;
    }
}
