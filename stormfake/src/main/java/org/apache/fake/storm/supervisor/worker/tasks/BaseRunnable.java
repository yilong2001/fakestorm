package org.apache.fake.storm.supervisor.worker.tasks;

import org.apache.fake.storm.supervisor.worker.stream.StreamGraph;
import org.apache.fake.storm.supervisor.worker.stream.StreamGraphRoot;

import java.util.List;

/**
 * Created by yilong on 2017/10/5.
 */
public abstract class BaseRunnable implements Runnable {
    final StreamGraphRoot streamGraphRoot;
    final StreamGraph streamGraph;

    public BaseRunnable(StreamGraphRoot streamGraphRoot, StreamGraph streamGraph) {
        this.streamGraphRoot = streamGraphRoot;
        this.streamGraph = streamGraph;
    }

    public abstract void prepare();

    @Override
    public void run() {

    }
}
