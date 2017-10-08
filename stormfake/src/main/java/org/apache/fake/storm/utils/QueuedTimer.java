package org.apache.fake.storm.utils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.UUID;

/**
 * Created by yilong on 2017/7/7.
 */
public class QueuedTimer {
    public static final Logger LOG = LoggerFactory.getLogger(QueuedTimer.class);
    private QueuedTimerConsumer<QueueElement> task = new QueuedTimerConsumer<QueueElement>();
    public static final int DEFAULT_TIMER_TASK_PRIORITY = 1;

    public QueuedTimer(String name) {
        if (name != null && name.length() > 0) {
            this.task.setName(name);
        }

        this.task.setActive(true);
        this.task.setDaemon(true);
        this.task.start();
    }

    public void schedule(long delayMs, Runnable call, boolean checkActive, int priority) {
        if (delayMs < 100) {
            delayMs = 100;
        }

        if (checkActive && !this.task.isActive()) {
            throw new IllegalStateException("thred is not active");
        }

        QueueElement queueElement = new QueueElement(UUID.randomUUID().toString(),
                System.currentTimeMillis() + delayMs, call, priority);

        //TODO: TIMER_LOG
        //LOG.info("timer-schedule : "+queueElement.id);
        this.task.addElement(queueElement);
    }

    public void schedule(final long periodMs, final Runnable call) {
        schedule(periodMs, new Runnable() {
            @Override
            public void run() {
                call.run();
                schedule(periodMs, call);
            }
        }, false, DEFAULT_TIMER_TASK_PRIORITY);
    }

    public void schedule(long delayMs, final long periodMs, final Runnable call) {
        schedule(delayMs, new Runnable(){
            @Override
            public void run() {
                call.run();
                schedule(periodMs, call);
            }
        });
    }

    public void close() throws Exception {
        if (!this.task.isActive()) {
            throw new IllegalStateException("not active thread.");
        }
        this.task.setActive(false);
        this.task.interrupt();
        this.task.join();
    }
}
