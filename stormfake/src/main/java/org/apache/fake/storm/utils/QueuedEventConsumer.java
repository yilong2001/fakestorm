package org.apache.fake.storm.utils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.InterruptedIOException;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Created by yilong on 2017/7/7.
 */
public class QueuedEventConsumer<Run extends Runnable> implements IEventConsumer<Run> {
    public static final Logger LOG = LoggerFactory.getLogger(QueuedEventConsumer.class);
    private ArrayBlockingQueue<Run> taskQueue = new ArrayBlockingQueue<Run>(20);

    private AtomicInteger innum;
    private AtomicInteger outnum;

    private AtomicBoolean active;
    private Runnable callback;
    private Thread thread;

    public QueuedEventConsumer() {
        innum = new AtomicInteger();
        outnum = new AtomicInteger();
        active = new AtomicBoolean(true);

        callback = () -> {
            while(this.active.get()) {
                Run qu = null;
                try {
                    qu = this.taskQueue.take();
                    if (qu == null) {
                        LOG.error("task queue take, bug get null!");
                        return;
                    }
                    //TODO: TIMER-LOG
                    //LOG.info(Thread.currentThread().getName()+":event-performer : "+qu.toString());
                    qu.run();
                    outnum.incrementAndGet();
                } catch (InterruptedException e) {
                    LOG.error(Thread.currentThread().getName()+"--"+e.getMessage(), e);
                } catch (Exception e) {
                    LOG.error(e.getMessage(), e);
                    if (e instanceof InterruptedIOException) {
                        ;
                    } else {
                        exitProcess(31, e.getMessage());
                    }
                }
            }
        };

        thread = new Thread(callback);
    }

    public void start() {
        thread.start();
    }

    public void close() throws Exception {
        this.active.set(false);
        this.thread.interrupt();
        this.thread.join();
    }

    public void setActive(boolean flag) {
        this.active.set(flag);
    }

    public boolean isActive() {
        return this.active.get();
    }

    public boolean waiting() {
        return (!thread.isAlive() && (innum.get() == outnum.get()));
    }

    public void add(Run event) {
        innum.incrementAndGet();
        this.taskQueue.add(event);
    }

    public static void exitProcess (int val, String msg) {
        String combinedErrorMessage = "Halting process: " + msg;
        LOG.error(combinedErrorMessage, new RuntimeException(combinedErrorMessage));
        Runtime.getRuntime().exit(val);
    }
}
