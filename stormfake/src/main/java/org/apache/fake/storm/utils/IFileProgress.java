package org.apache.fake.storm.utils;

/**
 * Created by yilong on 2017/6/30.
 */
public interface IFileProgress {
    public void onStart(String src, String dst, long total);
    public void onRunning(String src, String dst, long hasSuccssed, long total);
    public void onComplete(String src, String dst, long total);
}
