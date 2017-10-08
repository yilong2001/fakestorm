package demo.server;

import org.apache.fake.storm.nimbus.NimbusServer;

import java.util.Map;

/**
 * Created by yilong on 2017/6/30.
 */
public class NimbusMain {
    private static NimbusServer nimbusServer;
    private static Map<String,Object> conf = ConfigMain.getConf();

    public static void main(String[] args) {
        nimbusServer = new NimbusServer(conf);
        nimbusServer.start();

        Runtime.getRuntime().addShutdownHook(new Thread() {
            @Override
            public void run() {
                try {
                    nimbusServer.stop();
                } catch (Exception e) {
                    Thread.currentThread().interrupt();
                }
            }
        });

        try {
            Thread.sleep(Integer.MAX_VALUE);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
}
