/**
 * Autogenerated by Thrift Compiler (0.9.3)
 *
 * DO NOT EDIT UNLESS YOU ARE SURE THAT YOU KNOW WHAT YOU ARE DOING
 *  @generated
 */
package org.apache.fake.storm.generated;

import org.apache.thrift.scheme.IScheme;
import org.apache.thrift.scheme.SchemeFactory;
import org.apache.thrift.scheme.StandardScheme;

import org.apache.thrift.scheme.TupleScheme;
import org.apache.thrift.protocol.TTupleProtocol;
import org.apache.thrift.protocol.TProtocolException;
import org.apache.thrift.EncodingUtils;
import org.apache.thrift.TException;
import org.apache.thrift.async.AsyncMethodCallback;
import org.apache.thrift.server.AbstractNonblockingServer.*;
import java.util.List;
import java.util.ArrayList;
import java.util.Map;
import java.util.HashMap;
import java.util.EnumMap;
import java.util.Set;
import java.util.HashSet;
import java.util.EnumSet;
import java.util.Collections;
import java.util.BitSet;
import java.nio.ByteBuffer;
import java.util.Arrays;
import javax.annotation.Generated;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@SuppressWarnings({"cast", "rawtypes", "serial", "unchecked"})
@Generated(value = "Autogenerated by Thrift Compiler (0.9.3)", date = "2017-10-04")
public class TopoAssignment implements org.apache.thrift.TBase<TopoAssignment, TopoAssignment._Fields>, java.io.Serializable, Cloneable, Comparable<TopoAssignment> {
  private static final org.apache.thrift.protocol.TStruct STRUCT_DESC = new org.apache.thrift.protocol.TStruct("TopoAssignment");

  private static final org.apache.thrift.protocol.TField MASTER_CODE_DIR_FIELD_DESC = new org.apache.thrift.protocol.TField("master_code_dir", org.apache.thrift.protocol.TType.STRING, (short)1);
  private static final org.apache.thrift.protocol.TField NODE_HOST_FIELD_DESC = new org.apache.thrift.protocol.TField("node_host", org.apache.thrift.protocol.TType.MAP, (short)2);
  private static final org.apache.thrift.protocol.TField WORKER_RESOURCES_FIELD_DESC = new org.apache.thrift.protocol.TField("worker_resources", org.apache.thrift.protocol.TType.MAP, (short)5);
  private static final org.apache.thrift.protocol.TField NODE_PORT_EXECUTORS_FIELD_DESC = new org.apache.thrift.protocol.TField("node_port_executors", org.apache.thrift.protocol.TType.MAP, (short)6);

  private static final Map<Class<? extends IScheme>, SchemeFactory> schemes = new HashMap<Class<? extends IScheme>, SchemeFactory>();
  static {
    schemes.put(StandardScheme.class, new TopoAssignmentStandardSchemeFactory());
    schemes.put(TupleScheme.class, new TopoAssignmentTupleSchemeFactory());
  }

  public String master_code_dir; // required
  public Map<String,String> node_host; // optional
  public Map<NodeInfo,WorkerResources> worker_resources; // optional
  public Map<NodeInfo,List<ExecutorInfo>> node_port_executors; // optional

  /** The set of fields this struct contains, along with convenience methods for finding and manipulating them. */
  public enum _Fields implements org.apache.thrift.TFieldIdEnum {
    MASTER_CODE_DIR((short)1, "master_code_dir"),
    NODE_HOST((short)2, "node_host"),
    WORKER_RESOURCES((short)5, "worker_resources"),
    NODE_PORT_EXECUTORS((short)6, "node_port_executors");

    private static final Map<String, _Fields> byName = new HashMap<String, _Fields>();

    static {
      for (_Fields field : EnumSet.allOf(_Fields.class)) {
        byName.put(field.getFieldName(), field);
      }
    }

    /**
     * Find the _Fields constant that matches fieldId, or null if its not found.
     */
    public static _Fields findByThriftId(int fieldId) {
      switch(fieldId) {
        case 1: // MASTER_CODE_DIR
          return MASTER_CODE_DIR;
        case 2: // NODE_HOST
          return NODE_HOST;
        case 5: // WORKER_RESOURCES
          return WORKER_RESOURCES;
        case 6: // NODE_PORT_EXECUTORS
          return NODE_PORT_EXECUTORS;
        default:
          return null;
      }
    }

    /**
     * Find the _Fields constant that matches fieldId, throwing an exception
     * if it is not found.
     */
    public static _Fields findByThriftIdOrThrow(int fieldId) {
      _Fields fields = findByThriftId(fieldId);
      if (fields == null) throw new IllegalArgumentException("Field " + fieldId + " doesn't exist!");
      return fields;
    }

    /**
     * Find the _Fields constant that matches name, or null if its not found.
     */
    public static _Fields findByName(String name) {
      return byName.get(name);
    }

    private final short _thriftId;
    private final String _fieldName;

    _Fields(short thriftId, String fieldName) {
      _thriftId = thriftId;
      _fieldName = fieldName;
    }

    public short getThriftFieldId() {
      return _thriftId;
    }

    public String getFieldName() {
      return _fieldName;
    }
  }

  // isset id assignments
  private static final _Fields optionals[] = {_Fields.NODE_HOST,_Fields.WORKER_RESOURCES,_Fields.NODE_PORT_EXECUTORS};
  public static final Map<_Fields, org.apache.thrift.meta_data.FieldMetaData> metaDataMap;
  static {
    Map<_Fields, org.apache.thrift.meta_data.FieldMetaData> tmpMap = new EnumMap<_Fields, org.apache.thrift.meta_data.FieldMetaData>(_Fields.class);
    tmpMap.put(_Fields.MASTER_CODE_DIR, new org.apache.thrift.meta_data.FieldMetaData("master_code_dir", org.apache.thrift.TFieldRequirementType.REQUIRED, 
        new org.apache.thrift.meta_data.FieldValueMetaData(org.apache.thrift.protocol.TType.STRING)));
    tmpMap.put(_Fields.NODE_HOST, new org.apache.thrift.meta_data.FieldMetaData("node_host", org.apache.thrift.TFieldRequirementType.OPTIONAL, 
        new org.apache.thrift.meta_data.MapMetaData(org.apache.thrift.protocol.TType.MAP, 
            new org.apache.thrift.meta_data.FieldValueMetaData(org.apache.thrift.protocol.TType.STRING), 
            new org.apache.thrift.meta_data.FieldValueMetaData(org.apache.thrift.protocol.TType.STRING))));
    tmpMap.put(_Fields.WORKER_RESOURCES, new org.apache.thrift.meta_data.FieldMetaData("worker_resources", org.apache.thrift.TFieldRequirementType.OPTIONAL, 
        new org.apache.thrift.meta_data.MapMetaData(org.apache.thrift.protocol.TType.MAP, 
            new org.apache.thrift.meta_data.StructMetaData(org.apache.thrift.protocol.TType.STRUCT, NodeInfo.class), 
            new org.apache.thrift.meta_data.StructMetaData(org.apache.thrift.protocol.TType.STRUCT, WorkerResources.class))));
    tmpMap.put(_Fields.NODE_PORT_EXECUTORS, new org.apache.thrift.meta_data.FieldMetaData("node_port_executors", org.apache.thrift.TFieldRequirementType.OPTIONAL, 
        new org.apache.thrift.meta_data.MapMetaData(org.apache.thrift.protocol.TType.MAP, 
            new org.apache.thrift.meta_data.StructMetaData(org.apache.thrift.protocol.TType.STRUCT, NodeInfo.class), 
            new org.apache.thrift.meta_data.ListMetaData(org.apache.thrift.protocol.TType.LIST, 
                new org.apache.thrift.meta_data.StructMetaData(org.apache.thrift.protocol.TType.STRUCT, ExecutorInfo.class)))));
    metaDataMap = Collections.unmodifiableMap(tmpMap);
    org.apache.thrift.meta_data.FieldMetaData.addStructMetaDataMap(TopoAssignment.class, metaDataMap);
  }

  public TopoAssignment() {
    this.node_host = new HashMap<String,String>();

    this.worker_resources = new HashMap<NodeInfo,WorkerResources>();

    this.node_port_executors = new HashMap<NodeInfo,List<ExecutorInfo>>();

  }

  public TopoAssignment(
    String master_code_dir)
  {
    this();
    this.master_code_dir = master_code_dir;
  }

  /**
   * Performs a deep copy on <i>other</i>.
   */
  public TopoAssignment(TopoAssignment other) {
    if (other.isSetMaster_code_dir()) {
      this.master_code_dir = other.master_code_dir;
    }
    if (other.isSetNode_host()) {
      Map<String,String> __this__node_host = new HashMap<String,String>(other.node_host);
      this.node_host = __this__node_host;
    }
    if (other.isSetWorker_resources()) {
      Map<NodeInfo,WorkerResources> __this__worker_resources = new HashMap<NodeInfo,WorkerResources>(other.worker_resources.size());
      for (Map.Entry<NodeInfo, WorkerResources> other_element : other.worker_resources.entrySet()) {

        NodeInfo other_element_key = other_element.getKey();
        WorkerResources other_element_value = other_element.getValue();

        NodeInfo __this__worker_resources_copy_key = new NodeInfo(other_element_key);

        WorkerResources __this__worker_resources_copy_value = new WorkerResources(other_element_value);

        __this__worker_resources.put(__this__worker_resources_copy_key, __this__worker_resources_copy_value);
      }
      this.worker_resources = __this__worker_resources;
    }
    if (other.isSetNode_port_executors()) {
      Map<NodeInfo,List<ExecutorInfo>> __this__node_port_executors = new HashMap<NodeInfo,List<ExecutorInfo>>(other.node_port_executors.size());
      for (Map.Entry<NodeInfo, List<ExecutorInfo>> other_element : other.node_port_executors.entrySet()) {

        NodeInfo other_element_key = other_element.getKey();
        List<ExecutorInfo> other_element_value = other_element.getValue();

        NodeInfo __this__node_port_executors_copy_key = new NodeInfo(other_element_key);

        List<ExecutorInfo> __this__node_port_executors_copy_value = new ArrayList<ExecutorInfo>(other_element_value.size());
        for (ExecutorInfo other_element_value_element : other_element_value) {
          __this__node_port_executors_copy_value.add(new ExecutorInfo(other_element_value_element));
        }

        __this__node_port_executors.put(__this__node_port_executors_copy_key, __this__node_port_executors_copy_value);
      }
      this.node_port_executors = __this__node_port_executors;
    }
  }

  public TopoAssignment deepCopy() {
    return new TopoAssignment(this);
  }

  @Override
  public void clear() {
    this.master_code_dir = null;
    this.node_host = new HashMap<String,String>();

    this.worker_resources = new HashMap<NodeInfo,WorkerResources>();

    this.node_port_executors = new HashMap<NodeInfo,List<ExecutorInfo>>();

  }

  public String getMaster_code_dir() {
    return this.master_code_dir;
  }

  public TopoAssignment setMaster_code_dir(String master_code_dir) {
    this.master_code_dir = master_code_dir;
    return this;
  }

  public void unsetMaster_code_dir() {
    this.master_code_dir = null;
  }

  /** Returns true if field master_code_dir is set (has been assigned a value) and false otherwise */
  public boolean isSetMaster_code_dir() {
    return this.master_code_dir != null;
  }

  public void setMaster_code_dirIsSet(boolean value) {
    if (!value) {
      this.master_code_dir = null;
    }
  }

  public int getNode_hostSize() {
    return (this.node_host == null) ? 0 : this.node_host.size();
  }

  public void putToNode_host(String key, String val) {
    if (this.node_host == null) {
      this.node_host = new HashMap<String,String>();
    }
    this.node_host.put(key, val);
  }

  public Map<String,String> getNode_host() {
    return this.node_host;
  }

  public TopoAssignment setNode_host(Map<String,String> node_host) {
    this.node_host = node_host;
    return this;
  }

  public void unsetNode_host() {
    this.node_host = null;
  }

  /** Returns true if field node_host is set (has been assigned a value) and false otherwise */
  public boolean isSetNode_host() {
    return this.node_host != null;
  }

  public void setNode_hostIsSet(boolean value) {
    if (!value) {
      this.node_host = null;
    }
  }

  public int getWorker_resourcesSize() {
    return (this.worker_resources == null) ? 0 : this.worker_resources.size();
  }

  public void putToWorker_resources(NodeInfo key, WorkerResources val) {
    if (this.worker_resources == null) {
      this.worker_resources = new HashMap<NodeInfo,WorkerResources>();
    }
    this.worker_resources.put(key, val);
  }

  public Map<NodeInfo,WorkerResources> getWorker_resources() {
    return this.worker_resources;
  }

  public TopoAssignment setWorker_resources(Map<NodeInfo,WorkerResources> worker_resources) {
    this.worker_resources = worker_resources;
    return this;
  }

  public void unsetWorker_resources() {
    this.worker_resources = null;
  }

  /** Returns true if field worker_resources is set (has been assigned a value) and false otherwise */
  public boolean isSetWorker_resources() {
    return this.worker_resources != null;
  }

  public void setWorker_resourcesIsSet(boolean value) {
    if (!value) {
      this.worker_resources = null;
    }
  }

  public int getNode_port_executorsSize() {
    return (this.node_port_executors == null) ? 0 : this.node_port_executors.size();
  }

  public void putToNode_port_executors(NodeInfo key, List<ExecutorInfo> val) {
    if (this.node_port_executors == null) {
      this.node_port_executors = new HashMap<NodeInfo,List<ExecutorInfo>>();
    }
    this.node_port_executors.put(key, val);
  }

  public Map<NodeInfo,List<ExecutorInfo>> getNode_port_executors() {
    return this.node_port_executors;
  }

  public TopoAssignment setNode_port_executors(Map<NodeInfo,List<ExecutorInfo>> node_port_executors) {
    this.node_port_executors = node_port_executors;
    return this;
  }

  public void unsetNode_port_executors() {
    this.node_port_executors = null;
  }

  /** Returns true if field node_port_executors is set (has been assigned a value) and false otherwise */
  public boolean isSetNode_port_executors() {
    return this.node_port_executors != null;
  }

  public void setNode_port_executorsIsSet(boolean value) {
    if (!value) {
      this.node_port_executors = null;
    }
  }

  public void setFieldValue(_Fields field, Object value) {
    switch (field) {
    case MASTER_CODE_DIR:
      if (value == null) {
        unsetMaster_code_dir();
      } else {
        setMaster_code_dir((String)value);
      }
      break;

    case NODE_HOST:
      if (value == null) {
        unsetNode_host();
      } else {
        setNode_host((Map<String,String>)value);
      }
      break;

    case WORKER_RESOURCES:
      if (value == null) {
        unsetWorker_resources();
      } else {
        setWorker_resources((Map<NodeInfo,WorkerResources>)value);
      }
      break;

    case NODE_PORT_EXECUTORS:
      if (value == null) {
        unsetNode_port_executors();
      } else {
        setNode_port_executors((Map<NodeInfo,List<ExecutorInfo>>)value);
      }
      break;

    }
  }

  public Object getFieldValue(_Fields field) {
    switch (field) {
    case MASTER_CODE_DIR:
      return getMaster_code_dir();

    case NODE_HOST:
      return getNode_host();

    case WORKER_RESOURCES:
      return getWorker_resources();

    case NODE_PORT_EXECUTORS:
      return getNode_port_executors();

    }
    throw new IllegalStateException();
  }

  /** Returns true if field corresponding to fieldID is set (has been assigned a value) and false otherwise */
  public boolean isSet(_Fields field) {
    if (field == null) {
      throw new IllegalArgumentException();
    }

    switch (field) {
    case MASTER_CODE_DIR:
      return isSetMaster_code_dir();
    case NODE_HOST:
      return isSetNode_host();
    case WORKER_RESOURCES:
      return isSetWorker_resources();
    case NODE_PORT_EXECUTORS:
      return isSetNode_port_executors();
    }
    throw new IllegalStateException();
  }

  @Override
  public boolean equals(Object that) {
    if (that == null)
      return false;
    if (that instanceof TopoAssignment)
      return this.equals((TopoAssignment)that);
    return false;
  }

  public boolean equals(TopoAssignment that) {
    if (that == null)
      return false;

    boolean this_present_master_code_dir = true && this.isSetMaster_code_dir();
    boolean that_present_master_code_dir = true && that.isSetMaster_code_dir();
    if (this_present_master_code_dir || that_present_master_code_dir) {
      if (!(this_present_master_code_dir && that_present_master_code_dir))
        return false;
      if (!this.master_code_dir.equals(that.master_code_dir))
        return false;
    }

    boolean this_present_node_host = true && this.isSetNode_host();
    boolean that_present_node_host = true && that.isSetNode_host();
    if (this_present_node_host || that_present_node_host) {
      if (!(this_present_node_host && that_present_node_host))
        return false;
      if (!this.node_host.equals(that.node_host))
        return false;
    }

    boolean this_present_worker_resources = true && this.isSetWorker_resources();
    boolean that_present_worker_resources = true && that.isSetWorker_resources();
    if (this_present_worker_resources || that_present_worker_resources) {
      if (!(this_present_worker_resources && that_present_worker_resources))
        return false;
      if (!this.worker_resources.equals(that.worker_resources))
        return false;
    }

    boolean this_present_node_port_executors = true && this.isSetNode_port_executors();
    boolean that_present_node_port_executors = true && that.isSetNode_port_executors();
    if (this_present_node_port_executors || that_present_node_port_executors) {
      if (!(this_present_node_port_executors && that_present_node_port_executors))
        return false;
      if (!this.node_port_executors.equals(that.node_port_executors))
        return false;
    }

    return true;
  }

  @Override
  public int hashCode() {
    List<Object> list = new ArrayList<Object>();

    boolean present_master_code_dir = true && (isSetMaster_code_dir());
    list.add(present_master_code_dir);
    if (present_master_code_dir)
      list.add(master_code_dir);

    boolean present_node_host = true && (isSetNode_host());
    list.add(present_node_host);
    if (present_node_host)
      list.add(node_host);

    boolean present_worker_resources = true && (isSetWorker_resources());
    list.add(present_worker_resources);
    if (present_worker_resources)
      list.add(worker_resources);

    boolean present_node_port_executors = true && (isSetNode_port_executors());
    list.add(present_node_port_executors);
    if (present_node_port_executors)
      list.add(node_port_executors);

    return list.hashCode();
  }

  @Override
  public int compareTo(TopoAssignment other) {
    if (!getClass().equals(other.getClass())) {
      return getClass().getName().compareTo(other.getClass().getName());
    }

    int lastComparison = 0;

    lastComparison = Boolean.valueOf(isSetMaster_code_dir()).compareTo(other.isSetMaster_code_dir());
    if (lastComparison != 0) {
      return lastComparison;
    }
    if (isSetMaster_code_dir()) {
      lastComparison = org.apache.thrift.TBaseHelper.compareTo(this.master_code_dir, other.master_code_dir);
      if (lastComparison != 0) {
        return lastComparison;
      }
    }
    lastComparison = Boolean.valueOf(isSetNode_host()).compareTo(other.isSetNode_host());
    if (lastComparison != 0) {
      return lastComparison;
    }
    if (isSetNode_host()) {
      lastComparison = org.apache.thrift.TBaseHelper.compareTo(this.node_host, other.node_host);
      if (lastComparison != 0) {
        return lastComparison;
      }
    }
    lastComparison = Boolean.valueOf(isSetWorker_resources()).compareTo(other.isSetWorker_resources());
    if (lastComparison != 0) {
      return lastComparison;
    }
    if (isSetWorker_resources()) {
      lastComparison = org.apache.thrift.TBaseHelper.compareTo(this.worker_resources, other.worker_resources);
      if (lastComparison != 0) {
        return lastComparison;
      }
    }
    lastComparison = Boolean.valueOf(isSetNode_port_executors()).compareTo(other.isSetNode_port_executors());
    if (lastComparison != 0) {
      return lastComparison;
    }
    if (isSetNode_port_executors()) {
      lastComparison = org.apache.thrift.TBaseHelper.compareTo(this.node_port_executors, other.node_port_executors);
      if (lastComparison != 0) {
        return lastComparison;
      }
    }
    return 0;
  }

  public _Fields fieldForId(int fieldId) {
    return _Fields.findByThriftId(fieldId);
  }

  public void read(org.apache.thrift.protocol.TProtocol iprot) throws org.apache.thrift.TException {
    schemes.get(iprot.getScheme()).getScheme().read(iprot, this);
  }

  public void write(org.apache.thrift.protocol.TProtocol oprot) throws org.apache.thrift.TException {
    schemes.get(oprot.getScheme()).getScheme().write(oprot, this);
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder("TopoAssignment(");
    boolean first = true;

    sb.append("master_code_dir:");
    if (this.master_code_dir == null) {
      sb.append("null");
    } else {
      sb.append(this.master_code_dir);
    }
    first = false;
    if (isSetNode_host()) {
      if (!first) sb.append(", ");
      sb.append("node_host:");
      if (this.node_host == null) {
        sb.append("null");
      } else {
        sb.append(this.node_host);
      }
      first = false;
    }
    if (isSetWorker_resources()) {
      if (!first) sb.append(", ");
      sb.append("worker_resources:");
      if (this.worker_resources == null) {
        sb.append("null");
      } else {
        sb.append(this.worker_resources);
      }
      first = false;
    }
    if (isSetNode_port_executors()) {
      if (!first) sb.append(", ");
      sb.append("node_port_executors:");
      if (this.node_port_executors == null) {
        sb.append("null");
      } else {
        sb.append(this.node_port_executors);
      }
      first = false;
    }
    sb.append(")");
    return sb.toString();
  }

  public void validate() throws org.apache.thrift.TException {
    // check for required fields
    if (master_code_dir == null) {
      throw new org.apache.thrift.protocol.TProtocolException("Required field 'master_code_dir' was not present! Struct: " + toString());
    }
    // check for sub-struct validity
  }

  private void writeObject(java.io.ObjectOutputStream out) throws java.io.IOException {
    try {
      write(new org.apache.thrift.protocol.TCompactProtocol(new org.apache.thrift.transport.TIOStreamTransport(out)));
    } catch (org.apache.thrift.TException te) {
      throw new java.io.IOException(te);
    }
  }

  private void readObject(java.io.ObjectInputStream in) throws java.io.IOException, ClassNotFoundException {
    try {
      read(new org.apache.thrift.protocol.TCompactProtocol(new org.apache.thrift.transport.TIOStreamTransport(in)));
    } catch (org.apache.thrift.TException te) {
      throw new java.io.IOException(te);
    }
  }

  private static class TopoAssignmentStandardSchemeFactory implements SchemeFactory {
    public TopoAssignmentStandardScheme getScheme() {
      return new TopoAssignmentStandardScheme();
    }
  }

  private static class TopoAssignmentStandardScheme extends StandardScheme<TopoAssignment> {

    public void read(org.apache.thrift.protocol.TProtocol iprot, TopoAssignment struct) throws org.apache.thrift.TException {
      org.apache.thrift.protocol.TField schemeField;
      iprot.readStructBegin();
      while (true)
      {
        schemeField = iprot.readFieldBegin();
        if (schemeField.type == org.apache.thrift.protocol.TType.STOP) { 
          break;
        }
        switch (schemeField.id) {
          case 1: // MASTER_CODE_DIR
            if (schemeField.type == org.apache.thrift.protocol.TType.STRING) {
              struct.master_code_dir = iprot.readString();
              struct.setMaster_code_dirIsSet(true);
            } else { 
              org.apache.thrift.protocol.TProtocolUtil.skip(iprot, schemeField.type);
            }
            break;
          case 2: // NODE_HOST
            if (schemeField.type == org.apache.thrift.protocol.TType.MAP) {
              {
                org.apache.thrift.protocol.TMap _map8 = iprot.readMapBegin();
                struct.node_host = new HashMap<String,String>(2*_map8.size);
                String _key9;
                String _val10;
                for (int _i11 = 0; _i11 < _map8.size; ++_i11)
                {
                  _key9 = iprot.readString();
                  _val10 = iprot.readString();
                  struct.node_host.put(_key9, _val10);
                }
                iprot.readMapEnd();
              }
              struct.setNode_hostIsSet(true);
            } else { 
              org.apache.thrift.protocol.TProtocolUtil.skip(iprot, schemeField.type);
            }
            break;
          case 5: // WORKER_RESOURCES
            if (schemeField.type == org.apache.thrift.protocol.TType.MAP) {
              {
                org.apache.thrift.protocol.TMap _map12 = iprot.readMapBegin();
                struct.worker_resources = new HashMap<NodeInfo,WorkerResources>(2*_map12.size);
                NodeInfo _key13;
                WorkerResources _val14;
                for (int _i15 = 0; _i15 < _map12.size; ++_i15)
                {
                  _key13 = new NodeInfo();
                  _key13.read(iprot);
                  _val14 = new WorkerResources();
                  _val14.read(iprot);
                  struct.worker_resources.put(_key13, _val14);
                }
                iprot.readMapEnd();
              }
              struct.setWorker_resourcesIsSet(true);
            } else { 
              org.apache.thrift.protocol.TProtocolUtil.skip(iprot, schemeField.type);
            }
            break;
          case 6: // NODE_PORT_EXECUTORS
            if (schemeField.type == org.apache.thrift.protocol.TType.MAP) {
              {
                org.apache.thrift.protocol.TMap _map16 = iprot.readMapBegin();
                struct.node_port_executors = new HashMap<NodeInfo,List<ExecutorInfo>>(2*_map16.size);
                NodeInfo _key17;
                List<ExecutorInfo> _val18;
                for (int _i19 = 0; _i19 < _map16.size; ++_i19)
                {
                  _key17 = new NodeInfo();
                  _key17.read(iprot);
                  {
                    org.apache.thrift.protocol.TList _list20 = iprot.readListBegin();
                    _val18 = new ArrayList<ExecutorInfo>(_list20.size);
                    ExecutorInfo _elem21;
                    for (int _i22 = 0; _i22 < _list20.size; ++_i22)
                    {
                      _elem21 = new ExecutorInfo();
                      _elem21.read(iprot);
                      _val18.add(_elem21);
                    }
                    iprot.readListEnd();
                  }
                  struct.node_port_executors.put(_key17, _val18);
                }
                iprot.readMapEnd();
              }
              struct.setNode_port_executorsIsSet(true);
            } else { 
              org.apache.thrift.protocol.TProtocolUtil.skip(iprot, schemeField.type);
            }
            break;
          default:
            org.apache.thrift.protocol.TProtocolUtil.skip(iprot, schemeField.type);
        }
        iprot.readFieldEnd();
      }
      iprot.readStructEnd();

      // check for required fields of primitive type, which can't be checked in the validate method
      struct.validate();
    }

    public void write(org.apache.thrift.protocol.TProtocol oprot, TopoAssignment struct) throws org.apache.thrift.TException {
      struct.validate();

      oprot.writeStructBegin(STRUCT_DESC);
      if (struct.master_code_dir != null) {
        oprot.writeFieldBegin(MASTER_CODE_DIR_FIELD_DESC);
        oprot.writeString(struct.master_code_dir);
        oprot.writeFieldEnd();
      }
      if (struct.node_host != null) {
        if (struct.isSetNode_host()) {
          oprot.writeFieldBegin(NODE_HOST_FIELD_DESC);
          {
            oprot.writeMapBegin(new org.apache.thrift.protocol.TMap(org.apache.thrift.protocol.TType.STRING, org.apache.thrift.protocol.TType.STRING, struct.node_host.size()));
            for (Map.Entry<String, String> _iter23 : struct.node_host.entrySet())
            {
              oprot.writeString(_iter23.getKey());
              oprot.writeString(_iter23.getValue());
            }
            oprot.writeMapEnd();
          }
          oprot.writeFieldEnd();
        }
      }
      if (struct.worker_resources != null) {
        if (struct.isSetWorker_resources()) {
          oprot.writeFieldBegin(WORKER_RESOURCES_FIELD_DESC);
          {
            oprot.writeMapBegin(new org.apache.thrift.protocol.TMap(org.apache.thrift.protocol.TType.STRUCT, org.apache.thrift.protocol.TType.STRUCT, struct.worker_resources.size()));
            for (Map.Entry<NodeInfo, WorkerResources> _iter24 : struct.worker_resources.entrySet())
            {
              _iter24.getKey().write(oprot);
              _iter24.getValue().write(oprot);
            }
            oprot.writeMapEnd();
          }
          oprot.writeFieldEnd();
        }
      }
      if (struct.node_port_executors != null) {
        if (struct.isSetNode_port_executors()) {
          oprot.writeFieldBegin(NODE_PORT_EXECUTORS_FIELD_DESC);
          {
            oprot.writeMapBegin(new org.apache.thrift.protocol.TMap(org.apache.thrift.protocol.TType.STRUCT, org.apache.thrift.protocol.TType.LIST, struct.node_port_executors.size()));
            for (Map.Entry<NodeInfo, List<ExecutorInfo>> _iter25 : struct.node_port_executors.entrySet())
            {
              _iter25.getKey().write(oprot);
              {
                oprot.writeListBegin(new org.apache.thrift.protocol.TList(org.apache.thrift.protocol.TType.STRUCT, _iter25.getValue().size()));
                for (ExecutorInfo _iter26 : _iter25.getValue())
                {
                  _iter26.write(oprot);
                }
                oprot.writeListEnd();
              }
            }
            oprot.writeMapEnd();
          }
          oprot.writeFieldEnd();
        }
      }
      oprot.writeFieldStop();
      oprot.writeStructEnd();
    }

  }

  private static class TopoAssignmentTupleSchemeFactory implements SchemeFactory {
    public TopoAssignmentTupleScheme getScheme() {
      return new TopoAssignmentTupleScheme();
    }
  }

  private static class TopoAssignmentTupleScheme extends TupleScheme<TopoAssignment> {

    @Override
    public void write(org.apache.thrift.protocol.TProtocol prot, TopoAssignment struct) throws org.apache.thrift.TException {
      TTupleProtocol oprot = (TTupleProtocol) prot;
      oprot.writeString(struct.master_code_dir);
      BitSet optionals = new BitSet();
      if (struct.isSetNode_host()) {
        optionals.set(0);
      }
      if (struct.isSetWorker_resources()) {
        optionals.set(1);
      }
      if (struct.isSetNode_port_executors()) {
        optionals.set(2);
      }
      oprot.writeBitSet(optionals, 3);
      if (struct.isSetNode_host()) {
        {
          oprot.writeI32(struct.node_host.size());
          for (Map.Entry<String, String> _iter27 : struct.node_host.entrySet())
          {
            oprot.writeString(_iter27.getKey());
            oprot.writeString(_iter27.getValue());
          }
        }
      }
      if (struct.isSetWorker_resources()) {
        {
          oprot.writeI32(struct.worker_resources.size());
          for (Map.Entry<NodeInfo, WorkerResources> _iter28 : struct.worker_resources.entrySet())
          {
            _iter28.getKey().write(oprot);
            _iter28.getValue().write(oprot);
          }
        }
      }
      if (struct.isSetNode_port_executors()) {
        {
          oprot.writeI32(struct.node_port_executors.size());
          for (Map.Entry<NodeInfo, List<ExecutorInfo>> _iter29 : struct.node_port_executors.entrySet())
          {
            _iter29.getKey().write(oprot);
            {
              oprot.writeI32(_iter29.getValue().size());
              for (ExecutorInfo _iter30 : _iter29.getValue())
              {
                _iter30.write(oprot);
              }
            }
          }
        }
      }
    }

    @Override
    public void read(org.apache.thrift.protocol.TProtocol prot, TopoAssignment struct) throws org.apache.thrift.TException {
      TTupleProtocol iprot = (TTupleProtocol) prot;
      struct.master_code_dir = iprot.readString();
      struct.setMaster_code_dirIsSet(true);
      BitSet incoming = iprot.readBitSet(3);
      if (incoming.get(0)) {
        {
          org.apache.thrift.protocol.TMap _map31 = new org.apache.thrift.protocol.TMap(org.apache.thrift.protocol.TType.STRING, org.apache.thrift.protocol.TType.STRING, iprot.readI32());
          struct.node_host = new HashMap<String,String>(2*_map31.size);
          String _key32;
          String _val33;
          for (int _i34 = 0; _i34 < _map31.size; ++_i34)
          {
            _key32 = iprot.readString();
            _val33 = iprot.readString();
            struct.node_host.put(_key32, _val33);
          }
        }
        struct.setNode_hostIsSet(true);
      }
      if (incoming.get(1)) {
        {
          org.apache.thrift.protocol.TMap _map35 = new org.apache.thrift.protocol.TMap(org.apache.thrift.protocol.TType.STRUCT, org.apache.thrift.protocol.TType.STRUCT, iprot.readI32());
          struct.worker_resources = new HashMap<NodeInfo,WorkerResources>(2*_map35.size);
          NodeInfo _key36;
          WorkerResources _val37;
          for (int _i38 = 0; _i38 < _map35.size; ++_i38)
          {
            _key36 = new NodeInfo();
            _key36.read(iprot);
            _val37 = new WorkerResources();
            _val37.read(iprot);
            struct.worker_resources.put(_key36, _val37);
          }
        }
        struct.setWorker_resourcesIsSet(true);
      }
      if (incoming.get(2)) {
        {
          org.apache.thrift.protocol.TMap _map39 = new org.apache.thrift.protocol.TMap(org.apache.thrift.protocol.TType.STRUCT, org.apache.thrift.protocol.TType.LIST, iprot.readI32());
          struct.node_port_executors = new HashMap<NodeInfo,List<ExecutorInfo>>(2*_map39.size);
          NodeInfo _key40;
          List<ExecutorInfo> _val41;
          for (int _i42 = 0; _i42 < _map39.size; ++_i42)
          {
            _key40 = new NodeInfo();
            _key40.read(iprot);
            {
              org.apache.thrift.protocol.TList _list43 = new org.apache.thrift.protocol.TList(org.apache.thrift.protocol.TType.STRUCT, iprot.readI32());
              _val41 = new ArrayList<ExecutorInfo>(_list43.size);
              ExecutorInfo _elem44;
              for (int _i45 = 0; _i45 < _list43.size; ++_i45)
              {
                _elem44 = new ExecutorInfo();
                _elem44.read(iprot);
                _val41.add(_elem44);
              }
            }
            struct.node_port_executors.put(_key40, _val41);
          }
        }
        struct.setNode_port_executorsIsSet(true);
      }
    }
  }

}

