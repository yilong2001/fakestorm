package org.apache.fake.storm.thriftutil;

import org.apache.fake.storm.auth.ITransportInterceptor;
import org.apache.fake.storm.generated.Nimbus;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * Created by yilong on 2017/6/29.
 */
public class ThriftClient {
    public static final Logger LOG = LoggerFactory.getLogger(ThriftClient.class);

    private Nimbus.Client client;
    private TTransport transport;
    private TProtocol protocol;
    private String host;
    private int port;
    private ITransportInterceptor transportInterceptor;

    public ThriftClient(String host, int port, ITransportInterceptor interceptor) {
        this.host = host;
        this.port = port;
        this.transportInterceptor = interceptor;
        connect();
    }

    public void connect() {
        TSocket socket = null;
        try {
            socket = new TSocket(host, port);
            this.transport = this.transportInterceptor.getAndOpenClientTransport(socket, host, null);
            this.protocol = new TBinaryProtocol(transport);
        } catch (TTransportException e) {
            LOG.error(e.getMessage(), e);
            if(socket != null) {
                try {
                    socket.close();
                } catch (Exception e1) {}
            }
            throw new RuntimeException(e.getMessage());
        } catch (IOException e) {
            LOG.error(e.getMessage(), e);
            if(socket != null) {
                try {
                    socket.close();
                } catch (Exception e1) {}
            }
            throw new RuntimeException(e.getMessage());
        }
    }

    public TProtocol getProtocol() {
        return this.protocol;
    }

    public void close() {
        this.transport.close();
    }

//    public void submitTopology(String name, Map stormConf, String path) {
//        try {
//            // 设置调用的服务地址为本地，端口为 7911
//            TTransport transport = new TSocket("localhost", 7911);
//            transport.open();
//            // 设置传输协议为 TBinaryProtocol
//            TProtocol protocol = new TBinaryProtocol(transport);
//            Hello.Client client = new Hello.Client(protocol);
//            // 调用服务的 helloVoid 方法
//            client.helloVoid();
//            transport.close();
//        } catch (TTransportException e) {
//            e.printStackTrace();
//        } catch (TException e) {
//            e.printStackTrace();
//        }
//    }
}
