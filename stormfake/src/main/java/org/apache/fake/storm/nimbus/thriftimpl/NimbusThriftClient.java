package org.apache.fake.storm.nimbus.thriftimpl;

import org.apache.fake.storm.auth.AuthUtil;
import org.apache.fake.storm.auth.ITransportInterceptor;
import org.apache.fake.storm.generated.Nimbus;
import org.apache.fake.storm.store.IResourceStore;
import org.apache.fake.storm.store.RedisStore;
import org.apache.fake.storm.thriftutil.ThriftClient;
import org.apache.fake.storm.utils.ClusterConfigUtil;
import org.apache.fake.storm.utils.IFileProgress;
import org.apache.fake.storm.utils.NimbusConfigUtil;
import org.apache.fake.storm.utils.Serde;
import org.apache.storm.Config;
import org.apache.storm.generated.StormTopology;
import org.apache.thrift.TException;
import org.json.simple.JSONValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import static org.apache.fake.storm.utils.ClusterConfigUtil.getStormId;

/**
 * Created by yilong on 2017/6/30.
 */
public class NimbusThriftClient implements AutoCloseable {
    public static final Logger LOG = LoggerFactory.getLogger(NimbusThriftClient.class);

    private Nimbus.Client nimbusClient;
    private ThriftClient thriftClient;
    private final IResourceStore resourceStore;
    private final String stormId;

    private final int DefaultBufferSize = 102400;

    public NimbusThriftClient(Map<String, Object> conf) {
        ITransportInterceptor transportInterceptor = AuthUtil.getTransportInterceptor(conf);

        List<String> hosts = NimbusConfigUtil.getNimbusHost(conf);

        resourceStore = new RedisStore(conf);
        stormId = getStormId(conf);

        thriftClient = new ThriftClient(hosts.get(0), NimbusConfigUtil.getNimbusThriftPort(conf), transportInterceptor);
        nimbusClient = new Nimbus.Client(thriftClient.getProtocol());
//        try {
//            String leader = nimbusClient.getLeader();
//            if (leader.equals(hosts.get(0))) {
//                //good
//            } else {
//                thriftClient.close();
//                thriftClient = null;
//                nimbusClient = null;
//                this.thriftClient = new ThriftClient(leader, NimbusConfigUtil.getNimbusThriftPort(conf), transportInterceptor);
//                this.nimbusClient = new Nimbus.Client(thriftClient.getProtocol());
//            }
//        } catch (TException e) {
//            LOG.error(e.getMessage(), e);
//            throw new RuntimeException(e.getMessage());
//        }
    }

    public NimbusThriftClient(String host, int port, Map<String, Object> conf) {
        ITransportInterceptor transportInterceptor = AuthUtil.getTransportInterceptor(conf);

        thriftClient = new ThriftClient(host, port, transportInterceptor);
        nimbusClient = new Nimbus.Client(thriftClient.getProtocol());

        resourceStore = new RedisStore(conf);
        stormId = getStormId(conf);
    }

    public void submitTopology(String jar, String topoName, Config topoConf, StormTopology topology, IFileProgress progress) throws  Exception {
        if (topoName == null || jar == null) {
            throw new RuntimeException("toponame or topojar is null");
        }
        File jarFile = new File(jar);
        long totalLength = jarFile.length();
        if (totalLength <= 0) {
            throw new RuntimeException(" topojar is empty file!");
        }

        int hasUpedLength = 0;

        String topoConfStr = Serde.serialize(topoConf);
        String topoSer = Serde.thriftObjSerialize(topology);
        try {
            String dest = this.nimbusClient.beginFileUpload();
            progress.onStart(jar, dest, totalLength);

            BufferedInputStream jarStream = new BufferedInputStream(new FileInputStream(jar), DefaultBufferSize);
            byte[] buffer = new byte[DefaultBufferSize];

            while(hasUpedLength < totalLength) {
                LOG.info("while upload...");
                int cur = jarStream.read(buffer);
                if (cur <= 0) {
                    break;
                }

                byte[] realBuffer = Arrays.copyOf(buffer, cur);
                this.nimbusClient.uploadChunk(dest, ByteBuffer.wrap(realBuffer));
                hasUpedLength += cur;
                progress.onRunning(jar, dest, hasUpedLength, totalLength);
            }

            progress.onComplete(jar, dest, totalLength);
            this.nimbusClient.finishFileUpload(topoName, ClusterConfigUtil.TOPO_FILE_TYPE_JAR, dest);

            boolean res = false;
            res = resourceStore.updateTopoConf(stormId, topoName, topoConfStr);
            LOG.info(" updateTopoConf res is : " + res);
            res = resourceStore.updateTopoSerial(stormId, topoName, topoSer);
            LOG.info(" updateTopoSerial res is : " + res);

            res = resourceStore.registerTopoId(stormId, topoName);
            LOG.info(" registerTopoId res is : " + res);
        } catch (TException e) {
            e.printStackTrace();
            LOG.error(e.getMessage(), e);
            throw new RuntimeException(e.getMessage());
        } catch (FileNotFoundException e) {
            e.printStackTrace();
            LOG.error(e.getMessage(), e);
            throw new RuntimeException(e.getMessage());
        } catch (IOException e) {
            e.printStackTrace();
            LOG.error(e.getMessage(), e);
            throw new RuntimeException(e.getMessage());
        }
    }

    public void downloadTopo(String remoteFile, String localFile) {
        File lfile = new File(localFile);
        if (lfile.exists()) {
            lfile.delete();
        }

        File dir = lfile.getParentFile();
        if (!dir.exists()) {
            dir.mkdirs();
        }

        LOG.info("****************************************");
        LOG.info("downloadTopo : remote(" + remoteFile + "), to local("+localFile+")" );
        try {
            lfile.createNewFile();

            String session = this.nimbusClient.beginFileDownload(remoteFile);
            BufferedOutputStream stream = new BufferedOutputStream(new FileOutputStream(localFile));

            try {
                while (true) {
                    ByteBuffer bf = this.nimbusClient.downloadChunk(session);
                    if (bf.array().length > 0) {
                        stream.write(bf.array());
                    } else {
                        LOG.info("download from remote(" + remoteFile + "), to local(" + localFile + "), is ok!");
                        break;
                    }
                }
            } catch (Exception e) {
                LOG.error(e.getMessage(), e);
                throw new RuntimeException(e.getMessage());
            } finally {
                stream.flush();
                stream.close();
            }
        } catch (IOException e) {
            LOG.error(e.getMessage(), e);
            throw new RuntimeException(e.getMessage());
        } catch (TException e) {
            LOG.error(e.getMessage(), e);
            throw new RuntimeException(e.getMessage());
        } finally {

        }
    }

    public void close() {
        nimbusClient = null;
        thriftClient.close();
        thriftClient = null;
    }
}
