package org.apache.fake.storm.utils;

/**
 * Created by yilong on 2017/7/8.
 */
public class EventConsumerRunnableWrapper<Run extends Runnable> implements Runnable {
    private IEventConsumer<Run> eventConsumer;
    private Run run;
    public EventConsumerRunnableWrapper(IEventConsumer<Run> eventConsumer, Run run) {
        this.eventConsumer = eventConsumer;
        this.run = run;
    }

    @Override
    public void run() {
        eventConsumer.add(run);
    }
}
