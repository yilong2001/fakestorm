package org.apache.fake.storm.supervisor.worker.disruptor;

import com.lmax.disruptor.EventFactory;
import org.apache.storm.tuple.ITuple;

/**
 * Created by yilong on 2017/10/5.
 */
public class MessageBox<TUPLE> {
    public TUPLE tuple;
    //ITransportable transportable;
    public String messageId;
    public  int taskId;

    public void setTuple(TUPLE arg0) { tuple = arg0; }

    //public void setTransportable(ITransportable arg1) { transportable = arg1; }

    public void setMessageId(String arg1) { messageId = arg1; }

    public void setTaskId(int arg2) { taskId = arg2; }

    public TUPLE copyOfData() {
        // Copy the data out here.  In this case we have a single reference object, so the pass by
        // reference is sufficient.  But if we were reusing a byte array, then we would need to copy
        // the actual contents.
        return tuple;
    }
}
