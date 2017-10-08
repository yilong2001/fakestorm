package org.apache.fake.storm.auth;

import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;
import org.apache.thrift.transport.TTransportFactory;

import java.io.IOException;
import java.util.Map;

/**
 * Created by yilong on 2017/7/3.
 */
public interface ITransportInterceptor {
    public TTransportFactory getServerTransportFactory() throws IOException;
    public TTransport getAndOpenClientTransport(TTransport transport, String serverHost, String asUser)
            throws TTransportException, IOException;
}
