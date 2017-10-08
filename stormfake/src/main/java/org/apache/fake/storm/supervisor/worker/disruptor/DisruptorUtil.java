package org.apache.fake.storm.supervisor.worker.disruptor;

import com.lmax.disruptor.EventPoller;
import org.apache.fake.storm.supervisor.worker.tasks.BoltRunnable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by yilong on 2017/10/7.
 */
public class DisruptorUtil {
    public static final Logger LOG = LoggerFactory.getLogger(DisruptorUtil.class);

    public static MessageBox<List<Object>> getNextValue(EventPoller<MessageBox<List<Object>>> poller) throws Exception {
        final MessageBox<List<Object>> out = new MessageBox<List<Object>>();

        poller.poll(new EventPoller.Handler<MessageBox<List<Object>>>() {
            @Override
            public boolean onEvent(MessageBox<List<Object>> event, long sequence, boolean endOfBatch) throws Exception {
                if (event.tuple == null) {
                    LOG.warn(" getNextValue onEvent is null ");
                } else {
                    LOG.warn(" getNextValue onEvent is task:"+event.taskId+", num:" + event.tuple.size());
                }

                //out.addAll(event.copyOfData());
                out.setMessageId(event.messageId);
                out.setTaskId(event.taskId);

                out.setTuple(event.tuple);
                // Return false so that only one event is processed at a time.
                return false;
            }
        });

        return out;
    }
}
