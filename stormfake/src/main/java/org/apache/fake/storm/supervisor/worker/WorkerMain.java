package org.apache.fake.storm.supervisor.worker;

import demo.server.ConfigMain;

import java.util.Map;

/**
 * Created by yilong on 2017/9/12.
 */
public class WorkerMain {
    private static Map<String,Object> conf = ConfigMain.getConf();
    public static void main(String[] args) {
        String supervisorId = "";
        int port = 9061;
        String host = "localhost";
        String workerId = "";

        if (args.length >= 4) {
            supervisorId = args[0];
            host = args[1];
            port = Integer.parseInt(args[2]);
            workerId = args[3];
            System.out.println("supervisor id : " + supervisorId);
            System.out.println("worker id : " + workerId);
        }

        WorkerNiftyServer workerServer = new WorkerNiftyServer(conf,
                host,
                port,
                supervisorId,
                workerId);
        workerServer.start();
    }
}
