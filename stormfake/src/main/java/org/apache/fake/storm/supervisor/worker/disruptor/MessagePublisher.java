package org.apache.fake.storm.supervisor.worker.disruptor;

import com.lmax.disruptor.EventTranslatorThreeArg;

/**
 * Created by yilong on 2017/10/5.
 */
public class MessagePublisher<TUPLE> implements EventTranslatorThreeArg<MessageBox<TUPLE>, TUPLE, Integer, String> {
    @Override
    public void translateTo(MessageBox<TUPLE> event, long sequence, TUPLE arg0, Integer arg1, String arg2) {
        event.setTuple(arg0);
        event.setTaskId(arg1);
        event.setMessageId(arg2);
        //System.out.println(Thread.currentThread().getName() + ", procuder : " + arg1);
    }
}
