package org.apache.fake.storm.utils;

import org.apache.storm.task.TopologyContext;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by yilong on 2017/10/7.
 */
public class TopoCtxFactory {
    public static TopologyContext createTopologyContext(String stormId, int taskId, int port){
        Map<Integer, String> taskToComponent = new HashMap<Integer, String>();
        taskToComponent.put(taskId, "demoX");
        return new TopologyContext(null,
                null, taskToComponent,
                null,
                null,
                stormId,
                null, null,
                taskId,
                port,
                null, null, null, null, null, null);
    }
}
