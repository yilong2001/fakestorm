
namespace java org.apache.fake.storm.generated

include "exception.thrift"

struct DownloadSessionData {
  //Same version as in ReadableBlobMeta
  1: required i64 version;
  2: required string session;
  3: optional i64 data_size;
}

service Nimbus {
  string getLeader() throws (1: exception.AuthorizationException aze, 2:exception.FileOperationException foe);
  string beginFileUpload() throws (1: exception.AuthorizationException aze, 2:exception.FileOperationException foe);
  void uploadChunk(1: string location, 2: binary chunk) throws (1: exception.AuthorizationException aze, 2:exception.FileOperationException foe);
  void finishFileUpload(1: string topoid, 2: string filetype, 3: string location) throws (1: exception.AuthorizationException aze, 2:exception.FileOperationException foe);

  string beginFileDownload(1: string file) throws (1: exception.AuthorizationException aze, 2:exception.FileOperationException foe);
  binary downloadChunk(1: string session) throws (1: exception.AuthorizationException aze, 2:exception.FileOperationException foe);
}
