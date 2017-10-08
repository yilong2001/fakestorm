package demo.server;

import org.apache.fake.storm.supervisor.worker.thriftimpl.WorkerThriftClient;
import org.apache.fake.storm.utils.SupervisorConfigUtil;

import java.util.Map;

/**
 * Created by yilong on 2017/10/3.
 */
public class WorkerClientMain {
    private static Map<String, Object> conf = ConfigMain.getConf();
    private static WorkerThriftClient workerThriftClient;

    public static void main(String[] args) throws Exception {
        workerThriftClient = new WorkerThriftClient(conf,
                SupervisorConfigUtil.getPorts(conf).get(0),
                "localhost");

        try {
            if (workerThriftClient.isClosed()) {
                workerThriftClient.connect();
            }
            if (workerThriftClient.ping()) {
                System.out.println(" ping is ok! ");
            } else {
                System.out.println(" ping is wrong! ");
            }
        } catch (Exception e) {
            e.printStackTrace();
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e1) {
                e1.printStackTrace();
            }
        }

        try {
            workerThriftClient.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
