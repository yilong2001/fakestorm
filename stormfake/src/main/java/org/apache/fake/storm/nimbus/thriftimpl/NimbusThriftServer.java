package org.apache.fake.storm.nimbus.thriftimpl;

import org.apache.fake.storm.generated.AuthorizationException;
import org.apache.fake.storm.generated.FileOperationException;
import org.apache.fake.storm.generated.Nimbus;
import org.apache.fake.storm.model.TopoFileLocationInfo;
import org.apache.fake.storm.store.IResourceStore;
import org.apache.fake.storm.utils.NimbusConfigUtil;
import org.apache.fake.storm.utils.TimeoutCleanMap;
import org.apache.storm.utils.RotatingMap;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.nio.ByteBuffer;
import java.util.Map;
import java.util.UUID;

/**
 * Created by yilong on 2017/6/29.
 */
public class NimbusThriftServer implements Nimbus.Iface {
    public static final Logger LOG = LoggerFactory.getLogger(TimeoutCleanMap.class);

    private TimeoutCleanMap<String, BufferedInputStream> downloadMaps;
    private TimeoutCleanMap<String, BufferedOutputStream> uploadMaps;

    private final static String defaultTopoPackagesPath = "/Users/yilong/work/bigdata/code/stream/stormfake/topo/";
    private final IResourceStore resourceStore;

    private final Map<String, Object> conf;
    private final String stormId;
    private final String host;
    private final int port;

    public class DownloadTimeoutCallback implements RotatingMap.ExpiredCallback<String, BufferedInputStream> {
        @Override
        public void expire(String o, BufferedInputStream o2) {
            try {
                o2.close();
            } catch (IOException e) {
                LOG.error(e.getMessage(), e);
            }
        }
    }

    public class UploadTimeoutCallback implements RotatingMap.ExpiredCallback<String, BufferedOutputStream> {
        @Override
        public void expire(String o, BufferedOutputStream o2) {
            try {
                o2.close();
            } catch (IOException e) {
                LOG.error(e.getMessage(), e);
            }
        }
    }

    public NimbusThriftServer(Map<String, Object> conf, String stormid, String host, int port, IResourceStore resourceStore) {
        this.conf = conf;
        this.stormId = stormid;
        this.host = host;
        this.port = port;
        this.resourceStore = resourceStore;
        downloadMaps = new TimeoutCleanMap<String, BufferedInputStream>(100000, 3, new DownloadTimeoutCallback());
        uploadMaps = new TimeoutCleanMap<String, BufferedOutputStream>(100000, 3, new UploadTimeoutCallback());
    }

    @Override
    public String getLeader() throws AuthorizationException, FileOperationException, TException {
        return "localhost";
    }

    @Override
    public String beginFileUpload() throws AuthorizationException, FileOperationException, TException {

        File dir = new File(NimbusConfigUtil.getNimbusFiledir(conf, stormId));
        if (!dir.exists()) {
            dir.mkdirs();
        }

        String filename = NimbusConfigUtil.getNimbusFilename(conf, stormId);
        File jarfile = new File(filename);
        if (jarfile.exists()) {
            jarfile.delete();
        }

        LOG.info("************* beginFileUpload : "+jarfile.getAbsolutePath());

        try {
            jarfile.createNewFile();
        } catch (IOException e) {
            LOG.error(e.getMessage(), e);
            throw new FileOperationException(e.getMessage());
        }

        try {
            BufferedOutputStream stream = new BufferedOutputStream(new FileOutputStream(filename));
            uploadMaps.put(filename, stream);
        } catch (FileNotFoundException e) {
            LOG.error(e.getMessage(), e);
            throw new FileOperationException(e.getMessage());
        }

        return filename;
    }

    @Override
    public void uploadChunk(String location, ByteBuffer chunk) throws AuthorizationException, FileOperationException, TException {
        BufferedOutputStream stream = uploadMaps.get(location);

        if (stream == null) {
            LOG.error("uploadChunk: " +location + " is not exist!");
            throw new FileOperationException(location + " is not exist!");
        }

        try {
            uploadMaps.put(location, stream);
            stream.write(chunk.array());
        } catch (IOException e) {
            LOG.error(e.getMessage(), e);
            throw  new FileOperationException(e.getMessage());
        }
    }

    @Override
    public void finishFileUpload(String topoid, String filetype, String location) throws AuthorizationException, FileOperationException, TException {
        BufferedOutputStream stream = uploadMaps.get(location);
        if (stream == null) {
            LOG.error("finishFileUpload: " +location + " is not exist!");
            throw new FileOperationException(location + " is not exist!");
        }

        try {
            uploadMaps.remove(location);
            stream.close();

            TopoFileLocationInfo locationInfo = new TopoFileLocationInfo(host, port, location);
            resourceStore.updateTopoFileLocation(stormId, topoid, filetype, locationInfo);
        } catch (Exception e) {
            LOG.error(e.getMessage(), e);
            throw new FileOperationException(e.getMessage());
        }
    }

    @Override
    public String beginFileDownload(String filename) throws AuthorizationException, FileOperationException, TException {
        File file = new File(filename);
        if (!file.exists()) {
            LOG.error("begin file download, "+filename+", but failed!");
            throw new FileOperationException("file is not exist!");
        }

        String session = UUID.randomUUID().toString();
        BufferedInputStream inputStream = null;
        try {
            inputStream = new BufferedInputStream(new FileInputStream(filename));
        } catch (IOException e) {
            LOG.error(e.getMessage(), e);
            throw new FileOperationException(e.getMessage());
        }

        downloadMaps.put(session, inputStream);

        return session;
    }

    @Override
    public ByteBuffer downloadChunk(String session) throws AuthorizationException, FileOperationException, TException {
        if (!downloadMaps.containsKey(session)) {
            return ByteBuffer.wrap(new byte[]{});
        }

        byte[] buffer = new byte[1024 * 2];
        try {
            BufferedInputStream inputStream = downloadMaps.get(session);
            int bytesRead = inputStream.read(buffer);
            if (bytesRead > 0) {
                return ByteBuffer.wrap(buffer, 0, bytesRead);
            }

//            try {
//                downloadMaps.remove(session);
//            } catch (Exception e) {
//                LOG.error("download  " + session +" is over, remove from map error!");
//            } finally {
//                inputStream.close();
//            }
        } catch (Exception e) {
            LOG.error(e.getMessage(), e);
            throw new FileOperationException(e.getMessage());
        }

        return ByteBuffer.wrap(new byte[]{});
    }
}
