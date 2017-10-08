package org.apache.fake.storm.utils;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.apache.fake.storm.utils.ClusterConfigUtil.*;

/**
 * Created by yilong on 2017/7/27.
 */
public class SupervisorConfigUtil {

    private SupervisorConfigUtil(){}

    public static List<Integer> getPorts(Map<String, Object> conf) {
        List<Integer> ports = new ArrayList<Integer>();
        String strports = (String)conf.get(SUPERVISOR_PORTS);
        if (strports == null) {
            ports.add(9061);
            return ports;
        }

        String[] strportlist = strports.split(",");
        for (String p : strportlist) {
            try {
                int x = Integer.parseInt(p.trim());
                ports.add(x);
            } catch (NumberFormatException e) {
                ;
            }
        }

        return ports;
    }

    public static String getWorkerPidPath(Map<String, Object> conf, String workerId) {
        String localdDir = (String)conf.get(STORM_LOCAL_DIR);
        if (localdDir == null) {
            throw new RuntimeException(STORM_LOCAL_DIR + " is not be set!");
        }

        String arr[] = {localdDir, workerId, "pid"};
        return composePath(arr, FILE_PATH_SEPARATOR);
    }

    public static String getWorkerTmpPath(Map<String, Object> conf, String workerId) {
        String localdDir = (String)conf.get(STORM_LOCAL_DIR);
        if (localdDir == null) {
            throw new RuntimeException(STORM_LOCAL_DIR + " is not be set!");
        }

        String arr[] = {localdDir, workerId, "tmp"};
        return composePath(arr, FILE_PATH_SEPARATOR);
    }

    public static String getWorkerJarPath(Map<String, Object> conf, String workerId) {
        String localdDir = (String)conf.get(STORM_LOCAL_DIR);
        if (localdDir == null) {
            throw new RuntimeException(STORM_LOCAL_DIR + " is not be set!");
        }

        String arr[] = {localdDir, workerId, "jar"};
        return composePath(arr, FILE_PATH_SEPARATOR);
    }

    public static String getWorkerJarFilename(Map<String, Object> conf, String workerId) {
        String localdDir = (String)conf.get(STORM_LOCAL_DIR);
        if (localdDir == null) {
            throw new RuntimeException(STORM_LOCAL_DIR + " is not be set!");
        }

        String arr[] = {localdDir, workerId, "jar", "storm.jar"};
        return composePath(arr, FILE_PATH_SEPARATOR);
    }

    public static String getJavaLibraryPath(Map<String, Object> conf, String stormRoot) {
        String resDir = stormRoot + FILE_PATH_SEPARATOR + "resource";
        return resDir;
    }

}
