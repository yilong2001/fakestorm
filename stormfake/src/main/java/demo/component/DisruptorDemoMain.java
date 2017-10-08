package demo.component;

import com.lmax.disruptor.EventHandler;
import com.lmax.disruptor.EventPoller;
import com.lmax.disruptor.RingBuffer;
import org.apache.fake.storm.supervisor.worker.disruptor.MessageBox;
import org.apache.fake.storm.supervisor.worker.disruptor.MessageDisruptor;
import org.apache.fake.storm.supervisor.worker.disruptor.MessagePublisher;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

/**
 * Created by yilong on 2017/10/5.
 */
public class DisruptorDemoMain {
    public static class Message {
        String id;
        public Message(String i) { id = i; }
        public void setId(String i) { id = i; }
    }

    public static class Consumer implements EventHandler<MessageBox<Message>> {
        @Override
        public void onEvent(MessageBox<Message> event, long sequence, boolean endOfBatch) throws Exception {

        }
    }

    private static class MyProduceThread implements Runnable {
        final RingBuffer<MessageBox<List<Message>>> ringBuffer;
        final MessagePublisher<List<Message>> publisher;
        MyProduceThread(final RingBuffer<MessageBox<List<Message>>>  rb, MessagePublisher<List<Message>> pub) {
            ringBuffer = rb;
            publisher = pub;
        }

        @Override
        public void run() {
            while (true) {
                int id = (new Random().nextInt() % 10000);
                Message msg = new Message("task : " + id);
                List<Message> msglist = new ArrayList<>();
                msglist.add(msg);

                ringBuffer.publishEvent(publisher, msglist, id, "default");

                try {
                    Thread.sleep(100);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    private static class MyConsumerThread implements Runnable {
        final EventPoller<MessageBox<List<Message>>> poller;

        MyConsumerThread(final EventPoller<MessageBox<List<Message>>> p) {
            poller = p;
        }

        @Override
        public void run() {
            while( true) {
                try {
                    List<Message> value = getNextValue(poller);
                    if (value == null) {
                    } else {
                        System.out.println(Thread.currentThread().getName() + ", consumer : " + value.size());
                    }

                    Thread.sleep(100);
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        }
    }

    private static List<Message> getNextValue(EventPoller<MessageBox<List<Message>>> poller) throws Exception {
        final List<Message> out = new ArrayList<Message>();

        poller.poll(new EventPoller.Handler<MessageBox<List<Message>>>() {
                    @Override
                    public boolean onEvent(MessageBox<List<Message>> event, long sequence, boolean endOfBatch) throws Exception {
                        out.addAll(event.copyOfData());
                        // Return false so that only one event is processed at a time.
                        return false;
                    }
                });

        return out;
    }

    public static void main(String[] args) throws InterruptedException {
        MessageDisruptor<List<Message>> mydisruptor = new MessageDisruptor();

        Runnable[] produces = {new MyProduceThread(mydisruptor.getRingBuffer(), mydisruptor.newPublisher()),
                new MyProduceThread(mydisruptor.getRingBuffer(), mydisruptor.newPublisher()),
                new MyProduceThread(mydisruptor.getRingBuffer(), mydisruptor.newPublisher())};

        Runnable[] consumers = {new MyConsumerThread(mydisruptor.newPoller()), new MyConsumerThread(mydisruptor.newPoller()),new MyConsumerThread(mydisruptor.newPoller())};

        mydisruptor.start();

        for (Runnable produce : produces) {
            Thread thread = new Thread(produce);
            thread.start();
        }

        for (Runnable consumer : consumers) {
            Thread thread = new Thread(consumer);
            thread.start();
        }

        Thread.sleep(Integer.MAX_VALUE);
    }
}
