package org.apache.fake.storm.model;

import org.apache.fake.storm.utils.JsonSerializable;

import java.io.Serializable;

/**
 * Created by yilong on 2017/7/7.
 */
public class DemoBean extends JsonSerializable<DemoBean> implements Serializable {
    private String name;
    private long high;
    public DemoBean(String n, long h) {
        name = n;
        high = h;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public long getHigh() {
        return high;
    }

    public void setHigh(long high) {
        this.high = high;
    }
}
