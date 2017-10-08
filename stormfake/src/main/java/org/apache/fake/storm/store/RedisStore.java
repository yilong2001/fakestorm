package org.apache.fake.storm.store;

import org.apache.fake.storm.model.SupervisorInfo;
import org.apache.fake.storm.model.TopoFileLocationInfo;
import org.apache.fake.storm.model.WorkerAssignmentInfo;
import org.apache.fake.storm.utils.ClusterConfigUtil;
import org.apache.fake.storm.utils.JsonSerializable;
import org.apache.fake.storm.utils.Serde;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Created by yilong on 2017/7/8.
 */
public class RedisStore implements IResourceStore {
    public static final Logger LOG = LoggerFactory.getLogger(RedisStore.class);

    private RedisResourceDb redisResourceDb;
    public RedisStore(Map<String, Object> conf) {
        redisResourceDb = new RedisResourceDb(conf);
    }


    @Override
    public boolean registerTopoId(String stormId, String topoId) throws Exception {
        String path = ClusterConfigUtil.getTopoIdPath(stormId);
        return redisResourceDb.setSetValue(path, topoId);
    }

    @Override
    public boolean unRegisterTopoId(String stormId, String topoId) throws Exception {
        String path = ClusterConfigUtil.getTopoIdPath(stormId);
        if (!redisResourceDb.removeSetValue(path, topoId)) {
            return false;
        }

        path = ClusterConfigUtil.getAssignmentVersionPath(stormId, topoId);
        if (!redisResourceDb.removeKey(path)) {
            return false;
        }

        return true;
    }

    @Override
    public final List<String> getTopoIds(String stormId) throws Exception {
        String path = ClusterConfigUtil.getTopoIdPath(stormId);
        Set<String> ids = redisResourceDb.getSetAllValue(path);
        List<String> outs = new ArrayList<String>(ids);
        return outs;
    }

    @Override
    public TopoFileLocationInfo getTopoFileLocation(String stormId, String topoId, String filetype) throws Exception {
        String path = ClusterConfigUtil.getTopoFilePath(stormId, topoId, filetype);
        LOG.info("getTopoFileLocation : " + path);

        String json = redisResourceDb.getValue(path);
        if (json == null || json.length() < 2) {
            return null;
        }

        TopoFileLocationInfo info = (TopoFileLocationInfo)JsonSerializable.deserialize(json, TopoFileLocationInfo.class);
        return info;
    }


    @Override
    public boolean updateTopoFileLocation(String stormId, String topoId, String filetype,
                                          TopoFileLocationInfo locationInfo) throws Exception {
        String path = ClusterConfigUtil.getTopoFilePath(stormId, topoId, filetype);
        return redisResourceDb.setValue(path, locationInfo.serialiaze());
    }

    @Override
    public boolean updateTopoConf(String stormId, String topoId, String topoConf) throws Exception {
        String path = ClusterConfigUtil.getTopoConfPath(stormId, topoId);
        return redisResourceDb.setValue(path, topoConf);
    }

    @Override
    public String getTopoConf(String stormId, String topoId) throws Exception {
        String path = ClusterConfigUtil.getTopoConfPath(stormId, topoId);
        return redisResourceDb.getValue(path);
    }

    @Override
    public boolean updateTopoSerial(String stormId, String topoId, String topoSerial) throws Exception {
        String path = ClusterConfigUtil.getTopoSerialPath(stormId, topoId);
        return redisResourceDb.setValue(path, topoSerial);
    }

    @Override
    public String getTopoSerial(String stormId, String topoId) throws Exception {
        String path = ClusterConfigUtil.getTopoSerialPath(stormId, topoId);
        return redisResourceDb.getValue(path);
    }

    @Override
    public boolean updateAssignmentVersion(String stormId, String topoId, int version) throws Exception {
        String path = ClusterConfigUtil.getAssignmentVersionPath(stormId, topoId);
        return redisResourceDb.setValue(path, String.valueOf(version));
    }

    @Override
    public Integer getAssignmentVersion(String stormId, String topoId) throws Exception {
        String path = ClusterConfigUtil.getAssignmentVersionPath(stormId, topoId);
        String v = redisResourceDb.getValue(path);
        if (v == null) {
            return 0;
        }

        try {
            return Integer.parseInt(v);
        } catch (NumberFormatException e) {
            //TODO:
            return 0;
        }
    }

    @Override
    public boolean updateAssigment(String stormId, String topoId, WorkerAssignmentInfo assignmentInfo) throws Exception {
        String path = ClusterConfigUtil.getAssignmentPath(stormId, topoId);
        return redisResourceDb.setValue(path, Serde.serialize(assignmentInfo));
    }

    @Override
    public final WorkerAssignmentInfo getAssignment(String stormId, String topoId) throws Exception {
        String path = ClusterConfigUtil.getAssignmentPath(stormId, topoId);
        String val = redisResourceDb.getValue(path);
        if (val == null) {
            return  null;
        }
        WorkerAssignmentInfo info = Serde.deSerialize(val);
        return info;
        //return (WorkerAssignmentInfo)JsonSerializable.deserialize(val, WorkerAssignmentInfo.class);
    }

    @Override
    public boolean registerSupervisor(String stormId, String supervisorId, long timeoutMs,  final SupervisorInfo si) throws Exception {
        String path = ClusterConfigUtil.getSupervisorPath(stormId, supervisorId);
        return redisResourceDb.acquireOptimisticLockWithExpire(path,
                si.serialiaze(),
                10);
    }

    @Override
    public boolean registerWorker(String stormId, String supervisorId, String workerId, long timeoutMs) {
        //TODO:
        return false;
    }

    @Override
    public final List<SupervisorInfo> getSupervisors(String stormId) throws Exception {
        String pattern = ClusterConfigUtil.getSupervisorPathPattern(stormId);
        List<String> jsons = redisResourceDb.getListValueByPattern(pattern);

        List<SupervisorInfo> sis = new ArrayList<>();
        for (String json : jsons) {
            SupervisorInfo si = (SupervisorInfo)JsonSerializable.deserialize(json, SupervisorInfo.class);
            sis.add(si);
        }
        return sis;
    }

    @Override
    public boolean updateSupervisor(String stormId, String supervisorId, SupervisorInfo si) throws Exception {
        String path = ClusterConfigUtil.getSupervisorPath(stormId, supervisorId);
        return redisResourceDb.setValue(path, si.serialiaze());
    }

}
