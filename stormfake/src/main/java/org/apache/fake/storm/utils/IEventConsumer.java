package org.apache.fake.storm.utils;

/**
 * Created by yilong on 2017/7/8.
 */
public interface IEventConsumer<Run extends Runnable> {
    public void add(Run cb);
    public void start();
    public void close() throws Exception;
    public boolean waiting();
    public void setActive(boolean flag);
    public boolean isActive();

}
