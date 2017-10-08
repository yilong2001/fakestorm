package org.apache.fake.storm.supervisor.worker.stream;

import org.apache.storm.generated.GlobalStreamId;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.MessageId;
import org.apache.storm.tuple.Tuple;

import java.util.List;

/**
 * Created by yilong on 2017/10/7.
 */
public class SimpleTuple implements Tuple {
    final String componentId;
    final String streamId;
    final int taskId;
    final List<Object> values;
    final MessageId messageId;

    public SimpleTuple(String componentId, String streamId, int taskId, List<Object> values, MessageId messageId) {
        this.componentId = componentId;
        this.streamId = streamId;
        this.taskId = taskId;
        this.values = values;
        this.messageId = messageId;
    }

    @Override
    public GlobalStreamId getSourceGlobalStreamid() {
        return new GlobalStreamId(componentId, streamId);
    }

    @Override
    public GlobalStreamId getSourceGlobalStreamId() {
        return new GlobalStreamId(componentId, streamId);
    }

    @Override
    public String getSourceComponent() {
        return componentId;
    }

    @Override
    public int getSourceTask() {
        return 0;
    }

    @Override
    public String getSourceStreamId() {
        return streamId;
    }

    @Override
    public MessageId getMessageId() {
        return messageId;
    }

    @Override
    public int size() {
        return 0;
    }

    @Override
    public boolean contains(String s) {
        return false;
    }

    @Override
    public Fields getFields() {
        return null;
    }

    @Override
    public int fieldIndex(String s) {
        return 0;
    }

    @Override
    public List<Object> select(Fields fields) {
        return null;
    }

    @Override
    public Object getValue(int i) {
        return null;
    }

    @Override
    public String getString(int i) {
        if (i > values.size()) {
            return "unknown";
        }

        return values.get(i).toString();
    }

    @Override
    public Integer getInteger(int i) {
        return null;
    }

    @Override
    public Long getLong(int i) {
        return null;
    }

    @Override
    public Boolean getBoolean(int i) {
        return null;
    }

    @Override
    public Short getShort(int i) {
        return null;
    }

    @Override
    public Byte getByte(int i) {
        return null;
    }

    @Override
    public Double getDouble(int i) {
        return null;
    }

    @Override
    public Float getFloat(int i) {
        return null;
    }

    @Override
    public byte[] getBinary(int i) {
        return new byte[0];
    }

    @Override
    public Object getValueByField(String s) {
        return null;
    }

    @Override
    public String getStringByField(String s) {
        return null;
    }

    @Override
    public Integer getIntegerByField(String s) {
        return null;
    }

    @Override
    public Long getLongByField(String s) {
        return null;
    }

    @Override
    public Boolean getBooleanByField(String s) {
        return null;
    }

    @Override
    public Short getShortByField(String s) {
        return null;
    }

    @Override
    public Byte getByteByField(String s) {
        return null;
    }

    @Override
    public Double getDoubleByField(String s) {
        return null;
    }

    @Override
    public Float getFloatByField(String s) {
        return null;
    }

    @Override
    public byte[] getBinaryByField(String s) {
        return new byte[0];
    }

    @Override
    public List<Object> getValues() {
        return null;
    }
}
