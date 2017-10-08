package org.apache.fake.storm.supervisor.worker.disruptor;

import com.lmax.disruptor.*;
import com.lmax.disruptor.dsl.Disruptor;
import com.lmax.disruptor.dsl.ProducerType;
import com.lmax.disruptor.util.DaemonThreadFactory;

/**
 * Created by yilong on 2017/10/5.
 */
public class MessageDisruptor<TUPLE> {
    static final int RING_SIZE = 1024 * 1024;

    final private Disruptor<MessageBox<TUPLE>> disruptor;
    final RingBuffer<MessageBox<TUPLE>> ringBuffer;

    public final EventFactory<MessageBox<TUPLE>> FACTORY = new EventFactory<MessageBox<TUPLE>>() {
        @Override
        public MessageBox newInstance() {
            return new MessageBox<TUPLE>();
        }
    };

    public MessageDisruptor() {
        disruptor = new Disruptor<MessageBox<TUPLE>>(
                FACTORY, RING_SIZE,
                DaemonThreadFactory.INSTANCE,
                ProducerType.MULTI,
                new BlockingWaitStrategy());
        ringBuffer = disruptor.getRingBuffer();
    }

    public void addConsumer(EventHandler<MessageBox> ...consumers) {
        disruptor.handleEventsWith(consumers);
    }

    public final RingBuffer<MessageBox<TUPLE>> getRingBuffer() {
        return ringBuffer;
    }

    public final EventPoller<MessageBox<TUPLE>> newPoller() {
        return ringBuffer.newPoller();
    }

    public MessagePublisher<TUPLE> newPublisher() {
        return new MessagePublisher<TUPLE>();
    }

    public void start() {
        disruptor.start();
    }
}
