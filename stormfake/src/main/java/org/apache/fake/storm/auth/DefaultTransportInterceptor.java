package org.apache.fake.storm.auth;

import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;
import org.apache.thrift.transport.TTransportFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Map;

/**
 * Created by yilong on 2017/7/3.
 */
public class DefaultTransportInterceptor implements ITransportInterceptor {
    public static final Logger LOG = LoggerFactory.getLogger(DefaultTransportInterceptor.class);
    private final Map<String, Object> conf;

    public DefaultTransportInterceptor(Map<String, Object> conf) {
        this.conf = conf;
    }

    @Override
    public TTransportFactory getServerTransportFactory() throws IOException {
        return new TTransportFactory();
    }

    @Override
    public TTransport getAndOpenClientTransport(TTransport transport, String serverHost, String asUser)
            throws TTransportException, IOException {
        transport.open();
        return transport;
    }
}
