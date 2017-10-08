package org.apache.fake.storm.utils;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.apache.fake.storm.utils.ClusterConfigUtil.FILE_PATH_SEPARATOR;
import static org.apache.fake.storm.utils.ClusterConfigUtil.STORM_LOCAL_DIR;
import static org.apache.fake.storm.utils.ClusterConfigUtil.composePath;

/**
 * Created by yilong on 2017/6/30.
 */
public class NimbusConfigUtil {
    public static final String NIMBUS_THRIFT_HOSTS = "nimbus.thrifthosts";
    public static final String NIMBUS_THRIFT_PORT = "nimbus.thriftport";
    public static final String NIMBUS_BLOCK_QUEUE_SIZE = "nimbus.blockqueuesize";
    public static final String NIMBUS_WORKE_RTHREAD_NUM = "nimbus.workerthreadnum";

    private NimbusConfigUtil() {}

    public static int getNimbusThriftPort(Map<String, Object> conf) {
        String p = (String)conf.get(NIMBUS_THRIFT_PORT);
        try {
            if (p != null) {
                return Integer.parseInt(p.trim());
            }
        } catch (NumberFormatException e) {
            ;
        }

        return 8191;
    }

    public static int getNimbusBlockQueueSize(Map<String, Object> conf) {
        String p = (String)conf.get(NIMBUS_BLOCK_QUEUE_SIZE);
        try {
            if (p != null) {
                return Integer.parseInt(p.trim());
            }
        } catch (NumberFormatException e) {
            ;
        }

        return 100;
    }

    public static int getNimbusWorkerThreadNum(Map<String, Object> conf) {
        String p = (String)conf.get(NIMBUS_WORKE_RTHREAD_NUM);
        try {
            if (p != null) {
                return Integer.parseInt(p.trim());
            }
        } catch (NumberFormatException e) {
            ;
        }

        return 15;
    }

    public static List<String> getNimbusHost(Map<String, Object> conf) {
        String ps = (String)conf.get(NIMBUS_THRIFT_HOSTS);

        List<String> hosts = new ArrayList<>();

        if (ps == null || ps.length() < 1) {
            hosts.add("localhost");
            return hosts;
        }

        String[] psarr = ps.split(",");
        for (String p : psarr) {
            String x = p.trim();
            if (x.length() > 0) {
                hosts.add(x);
            }
        }

        return hosts;
    }

    public static String getNimbusFiledir(Map<String, Object> conf, String stormId) {
        String localdDir = (String)conf.get(STORM_LOCAL_DIR);
        if (localdDir == null) {
            throw new RuntimeException(STORM_LOCAL_DIR + " is not be set!");
        }

        String arr[] = {localdDir, "storm", stormId};
        return composePath(arr, FILE_PATH_SEPARATOR);
    }

    public static String getNimbusFilename(Map<String, Object> conf, String stormId) {
        String localdDir = (String)conf.get(STORM_LOCAL_DIR);
        if (localdDir == null) {
            throw new RuntimeException(STORM_LOCAL_DIR + " is not be set!");
        }
        String name = UUID.randomUUID().toString()+".dat";

        String arr[] = {localdDir, "storm", stormId, name};
        return composePath(arr, FILE_PATH_SEPARATOR);
    }

}
