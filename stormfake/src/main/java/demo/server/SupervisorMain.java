package demo.server;

import org.apache.fake.storm.supervisor.manager.SupervisorServer;

import java.util.Map;

/**
 * Created by yilong on 2017/7/11.
 */
public class SupervisorMain {
    private static Map<String,Object> conf = ConfigMain.getConf();

    public static void main(String[] args) {
        String supervisorHost = "localhost";
        String supervisorId = "6e40210b-a685-4f17-a524-1ebbb8809fae";
        if (args.length > 0) {
            supervisorHost = args[0];
        }

        if (args.length > 1) {
            supervisorId = args[1];
        }

        SupervisorServer supervisorServer = new SupervisorServer(conf, supervisorHost, supervisorId);

        supervisorServer.start();

        Runtime.getRuntime().addShutdownHook(new Thread() {
            @Override
            public void run() {
                try {
                    supervisorServer.stop();
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
