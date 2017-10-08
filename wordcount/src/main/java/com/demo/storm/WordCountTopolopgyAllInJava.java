package com.demo.storm;

/**
 * Created by yilong on 2017/9/13.
 */

import org.apache.storm.Config;
import org.apache.storm.generated.*;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.GeneralTopologyContext;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.thrift.TBase;
import org.apache.storm.thrift.protocol.TProtocol;
import org.apache.storm.thrift.transport.TMemoryBuffer;
import org.apache.storm.topology.BasicBoltExecutor;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.TupleImpl;
import org.apache.storm.tuple.Values;
import org.apache.storm.utils.Utils;

import org.apache.fake.storm.client.Submitter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.*;
//import java.util.HashMap;
//import java.util.Map;
//import java.util.Random;
//import java.util.StringTokenizer;

import static org.mockito.Mockito.*;


/*
** WordCountTopolopgyAllInJava类（单词计数）
*/
public class  WordCountTopolopgyAllInJava{
    public static final Logger LOG = LoggerFactory.getLogger(WordCountTopolopgyAllInJava.class);

    // 定义一个喷头，用于产生数据。该类继承自BaseRichSpout
    public static class RandomSentenceSpout extends BaseRichSpout {
        SpoutOutputCollector _collector;
        Random _rand;

        @Override
        public void open(Map conf, TopologyContext context, SpoutOutputCollector collector){
            _collector = collector;
            _rand = new Random();
        }

        @Override
        public void nextTuple(){
            LOG.info("RandomSentenceSpout *** : nextTuple");

            // 睡眠一段时间后再产生一个数据
            Utils.sleep(100);

            // 句子数组
            String[] sentences = new String[]{ "the cow jumped over the moon", "an apple a day keeps the doctor away",
                    "four score and seven years ago", "snow white and the seven dwarfs", "i am at two with nature" };

            // 随机选择一个句子
            String sentence = sentences[_rand.nextInt(sentences.length)];

            // 发射该句子给Bolt
            _collector.emit(new Values(sentence));
        }

        // 确认函数
        @Override
        public void ack(Object id){
        }

        // 处理失败的时候调用
        @Override
        public void fail(Object id){
        }

        @Override
        public void declareOutputFields(OutputFieldsDeclarer declarer){
            // 定义一个字段word
            declarer.declare(new Fields("word"));
        }
    }

    // 定义个Bolt，用于将句子切分为单词
    public static class SplitSentence extends BaseBasicBolt {
        @Override
        public void execute(Tuple tuple, BasicOutputCollector collector){
            LOG.info("SplitSentence *** : execute");
            // 接收到一个句子
            String sentence = tuple.getString(0);
            // 把句子切割为单词
            StringTokenizer iter = new StringTokenizer(sentence);
            // 发送每一个单词
            while(iter.hasMoreElements()){
                collector.emit(new Values(iter.nextToken()));
            }
        }

        @Override
        public void declareOutputFields(OutputFieldsDeclarer declarer){
            // 定义一个字段
            declarer.declare(new Fields("word"));
        }
    }

    // 定义一个Bolt，用于单词计数
    public static class WordCount extends BaseBasicBolt {
        Map<String, Integer> counts = new HashMap<String, Integer>();

        @Override
        public void execute(Tuple tuple, BasicOutputCollector collector){
            LOG.info("WordCount *** : execute");
            // 接收一个单词
            String word = tuple.getString(0);
            // 获取该单词对应的计数
            Integer count = counts.get(word);
            if(count == null)
                count = 0;
            // 计数增加
            count++;
            // 将单词和对应的计数加入map中
            counts.put(word,count);
            LOG.info("hello word!");
            LOG.info(word +"  "+count);
            // 发送单词和计数（分别对应字段word和count）
            collector.emit(new Values(word, count));
        }

        @Override
        public void declareOutputFields(OutputFieldsDeclarer declarer){
            // 定义两个字段word和count
            declarer.declare(new Fields("word","count"));
        }
    }

    private static Tuple generateTestTuple() {
        TopologyBuilder builder = new TopologyBuilder();
        GeneralTopologyContext topologyContext = new GeneralTopologyContext(builder.createTopology(),
                new Config(), new HashMap(), new HashMap(), new HashMap(), "") {
            @Override
            public Fields getComponentOutputFields(String componentId, String streamId) {
                return new Fields("record");
            }
        };
        return new TupleImpl(topologyContext, new Values("hello"), 1, "");
    }

    private TopologyContext createTopologyContext(){
        Map<Integer, String> taskToComponent = new HashMap<Integer, String>();
        taskToComponent.put(7, "Xcom");
        return new TopologyContext(null, null, taskToComponent, null, null, null, null, null, 7, 6703, null, null, null, null, null, null);
    }

    private static void showSpout(String key, SpoutSpec spout) {
        LOG.info("*****************   "+key+"  ***************** ");
        //LOG.info("Bolt get_full_class_name ("+key+") : "+bolt.get_bolt_object().get_java_object().get_full_class_name());
        Map<String, StreamInfo> ssinfo = spout.get_common().get_streams();

        Object obj = Utils.javaDeserialize( spout.get_spout_object().get_serialized_java(), Serializable.class);
        LOG.info(obj.toString());
        BaseRichSpout exec = (BaseRichSpout)obj;

        Map conf = mock(Map.class);
        TopologyContext context = mock(TopologyContext.class);
        SpoutOutputCollector collector = mock(SpoutOutputCollector.class);
        exec.open(conf, context, collector);

        exec.nextTuple();

        for (Map.Entry<String, StreamInfo> tmp : ssinfo.entrySet()) {
            String out = "";
            for (String s : tmp.getValue().get_output_fields()) {
                out += s+", ";
            }
            LOG.info(tmp.getKey()+":"+out);
        }
        LOG.info(" ******** stream id and group ********* ");
        Map<GlobalStreamId, Grouping> gginfo = spout.get_common().get_inputs();
        for (Map.Entry<GlobalStreamId, Grouping> tmp : gginfo.entrySet()) {
            String out = tmp.getValue().getSetField().getFieldName();
            //for (String s : tmp.getValue().getSetField()) {
            //    out += s+", ";
            //}
            LOG.info(tmp.getKey().get_componentId()+":"+tmp.getKey().get_streamId()+":"+out);
        }
        LOG.info(" ***************** ");
    }

    private static void showBolt(String key, Bolt bolt) {
        LOG.info("*****************   "+key+"  ***************** ");
        //LOG.info("Bolt get_full_class_name ("+key+") : "+bolt.get_bolt_object().get_java_object().get_full_class_name());
        Map<String, StreamInfo> ssinfo = bolt.get_common().get_streams();

        Object obj = Utils.javaDeserialize( bolt.get_bolt_object().get_serialized_java(), Serializable.class);
        LOG.info(obj.toString());
        BasicBoltExecutor exec = (BasicBoltExecutor)obj;

        Map conf = mock(Map.class);
        TopologyContext context = mock(TopologyContext.class);
        OutputCollector collector = mock(OutputCollector.class);
        exec.prepare(conf, context, collector);

        exec.execute(generateTestTuple());

        for (Map.Entry<String, StreamInfo> tmp : ssinfo.entrySet()) {
            String out = "";
            for (String s : tmp.getValue().get_output_fields()) {
                out += s+", ";
            }
            LOG.info(tmp.getKey()+":"+out);
        }
        LOG.info(" ******** stream id and group ********* ");
        Map<GlobalStreamId, Grouping> gginfo = bolt.get_common().get_inputs();
        for (Map.Entry<GlobalStreamId, Grouping> tmp : gginfo.entrySet()) {
            String out = tmp.getValue().getSetField().getFieldName();
            //for (String s : tmp.getValue().getSetField()) {
            //    out += s+", ";
            //}
            LOG.info(tmp.getKey().get_componentId()+":"+tmp.getKey().get_streamId()+":"+out);
        }
        LOG.info(" ***************** ");
    }

    public static <T extends TBase> String thriftObjSerialize(T obj) throws Exception {
        TMemoryBuffer mb = new TMemoryBuffer(128);
        TProtocol prot = new org.apache.storm.thrift.protocol.TCompactProtocol(mb);
        obj.write(prot);

        byte[] out = mb.getArray();

        return org.apache.commons.codec.binary.Base64.encodeBase64String(out);
    }

    public static <T extends TBase> T thriftObjDeSerialize(String str, T orgObj) throws Exception {
        byte[] out = org.apache.commons.codec.binary.Base64.decodeBase64(str);
        TMemoryBuffer mb = new TMemoryBuffer(128);
        TProtocol prot = new org.apache.storm.thrift.protocol.TCompactProtocol(mb);
        mb.write(out);
        orgObj.read(prot);

        return orgObj;
    }

    public static void main(String[] args) throws Exception {
        // 创建一个拓扑
        TopologyBuilder builder = new TopologyBuilder();
        // 设置Spout，这个Spout的名字叫做"Spout"，设置并行度为5
        builder.setSpout("Spout", new RandomSentenceSpout(), 1);
        // 设置slot——“split”，并行度为8，它的数据来源是spout的
        builder.setBolt("split", new SplitSentence(), 1).shuffleGrouping("spout");
        // 设置slot——“count”,你并行度为12，它的数据来源是split的word字段
        builder.setBolt("count", new WordCount(), 1).fieldsGrouping("split", new Fields("word"));

        Config conf = new Config();
        conf.setDebug(false);

        conf.setMaxTaskParallelism(3);

        //StormSubmitter.submitTopologyWithProgressBar("word-count", conf, builder.createTopology());
        String jar = "./target/wordcount-1.0-SNAPSHOT.jar";
        String topo = "wordcount";

        StormTopology topology = builder.createTopology();

        String out = thriftObjSerialize(topology);

        StormTopology topology2 = new TopologyBuilder().createTopology();

        topology2 = thriftObjDeSerialize(out, topology2);

        Map<String, Bolt> ssinfo = topology2.get_bolts();
        for (Map.Entry<String, Bolt> tmp : ssinfo.entrySet()) {
            showBolt(tmp.getKey(), tmp.getValue());
        }

        Map<String, SpoutSpec> spsinfo = topology2.get_spouts();
        for (Map.Entry<String, SpoutSpec> tmp : spsinfo.entrySet()) {
            showSpout(tmp.getKey(), tmp.getValue());
        }

        //command: java -jar target/wordcount-1.0-SNAPSHOT-jar-with-dependencies.jar com.demo.storm.WordCountTopolopgyAllInJava
        Submitter.submitTopologyWithProgressBar(jar, topo, conf, topology, null);

        Thread.sleep(1000);
    }
}