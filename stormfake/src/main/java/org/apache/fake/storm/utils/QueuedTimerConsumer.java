package org.apache.fake.storm.utils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Comparator;
import java.util.concurrent.PriorityBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Created by yilong on 2017/7/7.
 */
public class QueuedTimerConsumer<Run extends QueueElement> extends Thread {
    public static final Logger LOG = LoggerFactory.getLogger(QueuedTimerConsumer.class);
    private PriorityBlockingQueue<Run> taskQueue = new PriorityBlockingQueue<Run>(20, new Comparator<Run>() {
        @Override
        public int compare(QueueElement o1, QueueElement o2) {
            return ((o1.expiredMs.intValue() + o1.priority.intValue() * 10) - (o2.expiredMs.intValue() + o2.priority.intValue() * 10));
        }
    });

    private AtomicBoolean active = new AtomicBoolean(false);

    public QueuedTimerConsumer() {
    }

    @Override
    public void run() {
        while(this.active.get()) {
            Run qu = null;
            try {
                qu = this.taskQueue.peek();
                if (qu != null) {
                    long deta = System.currentTimeMillis() - qu.expiredMs.longValue();
                    //TODO: TIMER-LOG
                    //LOG.info(Thread.currentThread().getName()+":timer-performer : "+qu.id);
                    if (deta >= 0) {
                        this.taskQueue.remove(qu);

                        //if longtime running, will be risk
                        qu.call.run();
                    } else {
                        Thread.sleep((1000 > (-deta))?(-deta):1000);
                    }
                } else {
                    Thread.sleep(1000);
                }
            } catch (InterruptedException e) {
                LOG.error(Thread.currentThread().getName()+"--"+e.getMessage(), e);
                this.active.set(false);
            }
        }
    }

    public void setActive(boolean flag) {
        this.active.set(flag);
    }

    public boolean isActive() {
        return this.active.get();
    }

    public boolean addElement(Run qe) {
        return this.taskQueue.add(qe);
    }
}
