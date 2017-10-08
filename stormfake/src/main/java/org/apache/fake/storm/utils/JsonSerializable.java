package org.apache.fake.storm.utils;

import org.codehaus.jackson.JsonGenerator;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.map.SerializationConfig;

import java.io.IOException;
import java.io.Serializable;
import java.io.StringWriter;

/**
 * Created by yilong on 2017/7/7.
 */
public class JsonSerializable<Obj extends Serializable> implements Serializable {

    public static Object deserialize(String jsonString, Class cls) throws Exception {
        return Serde.deSerialize(jsonString);
//        ObjectMapper objectMapper = new ObjectMapper();
//        Object obj = (Object)objectMapper.readValue(jsonString, cls);
//        return obj;
    }

    public String serialiaze() throws Exception {
        Obj obj = (Obj)this;
        String json = Serde.serialize(obj);
        return json;
//        StringWriter sw = new StringWriter();
//        ObjectMapper objectMapper = new ObjectMapper();
//        objectMapper.disable(SerializationConfig.Feature.FAIL_ON_EMPTY_BEANS);
//
//        JsonGenerator jg = objectMapper.getJsonFactory().createJsonGenerator(sw);
//        jg.writeObject(obj);
//
//        return sw.toString();
    }

    public String toString()  {
        try {
            return serialiaze();
        } catch (Exception e) {
            return null;
        }
    }
}
