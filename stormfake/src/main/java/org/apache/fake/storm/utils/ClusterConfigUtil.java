package org.apache.fake.storm.utils;

import java.util.Map;

/**
 * Created by yilong on 2017/7/7.
 */
public class ClusterConfigUtil {
    public static final String TOPO_FILE_TYPE_JAR = "jar";
    public static final String TOPO_FILE_TYPE_CONF = "conf";

    public static final String RS_PATH_ASSIGNMENT_ROOT = "assignmant";
    public static final String RS_PATH_SUPERVISOR_ROOT = "supervisor";

    public static final String RS_PATH_SEPRATOR = "_";

    public static final String RS_PATH_TOPO_ROOT = "topo";
    public static final String RS_PATH_IDS =  "ids";
    public static final String RS_PATH_VERSION =  "version";

    public static final String RS_PATH_ASSIGNMENT =  "assignment";

    public static final String CLUSTER_STORM_ID = "storm.id";

    public static final String FILE_PATH_SEPARATOR = System.getProperty("file.separator");

    public static final String STORM_LOCAL_DIR = "storm.local.dir";
    public static final String STORM_ROOT_DIR = "storm.root.dir";

    public static final String SUPERVISOR_PORTS = "supervisor.ports";

    public static final String FAKE_STORM_JAR_PATH = "storm.jar.path";

    public static String getStormId(Map<String, Object> conf) {
        return (String)conf.get(CLUSTER_STORM_ID);
    }

    public static String getFakeStormJarPath(Map<String, Object> conf) {
        return (String)conf.get(FAKE_STORM_JAR_PATH);
    }

    public static String composePath(final String[] arr) {
        String out = arr[0];
        for (int i=1; i<arr.length; i++) {
            out += (RS_PATH_SEPRATOR + arr[i]);
        }
        return out;
    }

    public static String composePath(final String[] arr, String seperator) {
        String out = arr[0];
        for (int i=1; i<arr.length; i++) {
            out += (seperator + arr[i]);
        }
        return out;
    }

    public static String getAssignmentPath(String stormId, String topoId) {
        return composePath(new String[]{stormId, topoId, RS_PATH_ASSIGNMENT_ROOT});
    }

    public static String getSupervisorPathPattern(String stormId) {
        return composePath(new String[]{stormId, RS_PATH_SUPERVISOR_ROOT, "*"});
    }

    public static String getSupervisorPath(String stormId, String supervisorId) {
        return composePath(new String[]{stormId, RS_PATH_SUPERVISOR_ROOT, supervisorId});
    }

    public static String getTopoIdPath(String stormId) {
        //stormid_topo_ids: set[topid1, topoid2, ...]
        return composePath(new String[]{stormId, RS_PATH_TOPO_ROOT, RS_PATH_IDS});
    }

    public static String getTopoFilePath(String stormId, String topoId, String filetype) {
        return composePath(new String[]{stormId, topoId, filetype});
    }

    public static String getTopoConfPath(String stormId, String topoId) {
        return composePath(new String[]{stormId, topoId, "topoconf"});
    }

    public static String getTopoSerialPath(String stormId, String topoId) {
        return composePath(new String[]{stormId, topoId, "toposerd"});
    }

    public static String getAssignmentVersionPath(String stormId, String topoId) {
        return composePath(new String[]{stormId, RS_PATH_TOPO_ROOT, RS_PATH_ASSIGNMENT, RS_PATH_VERSION, topoId});
    }

    public static String getAssignmentVersionPathPattern(String stormId) {
        return composePath(new String[]{stormId, RS_PATH_TOPO_ROOT, RS_PATH_ASSIGNMENT, RS_PATH_VERSION, "*"});
    }

}
