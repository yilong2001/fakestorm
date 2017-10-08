package org.apache.fake.storm.supervisor.manager;

import java.util.Map;

/**
 * Created by yilong on 2017/7/11.
 */
public class SupervisorFactory {
    public static ISupervisor makeSupervisor(Map<String, Object> conf, String supervisorId, String hostIp) {
        return new DefaultSupervisor(conf, supervisorId, hostIp);
    }
}
