package org.apache.fake.storm.store;

import org.apache.fake.storm.model.SupervisorInfo;
import org.apache.fake.storm.model.TopoFileLocationInfo;
import org.apache.fake.storm.model.WorkerAssignmentInfo;

import java.util.List;

/**
 * Created by yilong on 2017/7/5.
 */
public interface IResourceStore {
    //TODO: topo interface
    public boolean registerTopoId(String stormId, String topoId) throws Exception;
    public boolean unRegisterTopoId(String stormId, String topoId) throws Exception;
    public List<String> getTopoIds(String stormId) throws Exception;

    public TopoFileLocationInfo getTopoFileLocation(String stormId, String topoId, String filetype) throws Exception;
    public boolean updateTopoFileLocation(String stormId, String topoId, String filetype, final TopoFileLocationInfo locationInfo) throws Exception;
    public boolean updateTopoConf(String stormId, String topoId, String topoConf) throws Exception;
    public String getTopoConf(String stormId, String topoId) throws Exception;
    public boolean updateTopoSerial(String stormId, String topoId, String topoSerial) throws Exception;
    public String getTopoSerial(String stormId, String topoId) throws Exception;

    public boolean updateAssignmentVersion(String stormId, String topoId, int version) throws Exception;
    public Integer getAssignmentVersion(String stormId, String topoId) throws Exception;

    public boolean updateAssigment(String stormId, String topoId, WorkerAssignmentInfo assignmentInfo) throws Exception;
    public WorkerAssignmentInfo getAssignment(String stormId, String topoId) throws Exception;

    //TODO: storm cluster interface
    public boolean registerSupervisor(String stormId, String supervisorId, long timeoutMs, final SupervisorInfo si) throws Exception;
    public List<SupervisorInfo> getSupervisors(String stormId) throws Exception;
    public boolean updateSupervisor(String stormId, String supervisorId, final SupervisorInfo si) throws Exception;

    public boolean registerWorker(String stormId, String supervisorId, String workerId, long timeoutMs) throws Exception;

    //public boolean updateWorker(String stormId, String supervisorId, final SupervisorInfo si) throws Exception;
}
