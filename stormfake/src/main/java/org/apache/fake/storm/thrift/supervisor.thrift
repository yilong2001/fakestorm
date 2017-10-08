
namespace java org.apache.fake.storm.generated

include "exception.thrift"

struct WorkerResources {
    1: optional double mem_on_heap;
    2: optional double mem_off_heap;
    3: optional double cpu;
}

struct ExecutorInfo {
    1: required i32 task_start;
    2: required i32 task_end;
    3: optional i32 start_time_secs;
}

struct NodeInfo {
    1: required string node;
    2: required set<i64> port;
}

//one topo - one assignment : may be multi node
struct TopoAssignment {
    1: required string master_code_dir;
    2: optional map<string, string> node_host = {};
    //3: optional map<list<i64>, NodeInfo> executor_node_port = {};
    //4: optional map<list<i64>, i64> executor_start_time_secs = {};
    5: optional map<NodeInfo, WorkerResources> worker_resources = {};
    6: optional map<NodeInfo, list<ExecutorInfo>> node_port_executors = {};
}

// assignment in one node
struct TopoNodeAssignment {
  1: required string topology_id;
  2: required list<ExecutorInfo> executors;
  3: optional WorkerResources resources;
}

service Worker {
  string getConnectionId() throws (1: exception.AuthorizationException aze, 2:exception.FileOperationException foe);
  bool setAssignmentPath(1: string id, 2:string stormId, 3:string topoId) throws (1: exception.AuthorizationException aze, 2:exception.FileOperationException foe);
  bool setAssignment(1: string id, 2:string assignment) throws (1: exception.AuthorizationException aze, 2:exception.FileOperationException foe);
  bool ping(1: string id) throws (1: exception.AuthorizationException aze, 2:exception.FileOperationException foe);
  bool sendMessage(1: string sessionid, 2: string message, 3: i32 taskid, 4: string messageId) throws (1: exception.AuthorizationException aze, 2:exception.FileOperationException foe);
}
