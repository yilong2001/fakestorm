package org.apache.fake.storm.client;

import demo.server.ConfigMain;
import org.apache.fake.storm.nimbus.thriftimpl.NimbusThriftClient;
import org.apache.fake.storm.utils.IFileProgress;
import org.apache.storm.Config;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.generated.SubmitOptions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

/**
 * Created by yilong on 2017/9/17.
 */
public class Submitter {
    public static final Logger LOG = LoggerFactory.getLogger(Submitter.class);

    private static Map<String, Object> stormConf = ConfigMain.getConf();

    public static void submitTopologyWithProgressBar(String jar, String name, Config topoConf, StormTopology topology)
            throws Exception {
        submitTopologyWithProgressBar(jar, name, topoConf, topology, (SubmitOptions)null);
    }

    public static void submitTopologyWithProgressBar(String jar, String name, Config topoConf, StormTopology topology, SubmitOptions opts)
            throws Exception {
        class MyProgress implements IFileProgress {
            @Override
            public void onStart(String src, String dst, long total) {
                System.out.println(dst + ":start:");
            }

            @Override
            public void onRunning(String src, String dst, long hasSuccssed, long total) {
                System.out.print(".");
            }

            @Override
            public void onComplete(String src, String dst, long total) {
                System.out.println(Thread.currentThread().getName()+"-end");
            }
        }

        LOG.info("Submitter start nimbus client ... ");
        NimbusThriftClient nimbusClient = new NimbusThriftClient(stormConf);
        nimbusClient.submitTopology(jar, name, topoConf, topology, new MyProgress());
        nimbusClient.close();
        LOG.info("Submitter end nimbus client ... ");
    }
}
