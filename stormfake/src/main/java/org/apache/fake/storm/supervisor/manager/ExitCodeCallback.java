package org.apache.fake.storm.supervisor.manager;

/**
 * Created by yilong on 2017/9/14.
 */
public interface ExitCodeCallback {

    /**
     * The process finished
     * @param exitCode the exit code of the finished process.
     */
    public void call(int exitCode);
}
