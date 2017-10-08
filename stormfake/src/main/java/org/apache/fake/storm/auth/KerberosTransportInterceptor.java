package org.apache.fake.storm.auth;

import org.apache.hadoop.security.authentication.util.KerberosName;
import org.apache.thrift.transport.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.security.auth.Subject;
import javax.security.auth.callback.CallbackHandler;
import javax.security.auth.kerberos.KerberosTicket;
import javax.security.auth.login.Configuration;
import javax.security.auth.login.LoginContext;
import javax.security.auth.login.LoginException;
import javax.security.sasl.Sasl;
import java.io.IOException;
import java.security.Principal;
import java.security.PrivilegedActionException;
import java.security.PrivilegedExceptionAction;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

/**
 * Created by yilong on 2017/7/3.
 */
public class KerberosTransportInterceptor implements ITransportInterceptor {
    public static final Logger LOG = LoggerFactory.getLogger(KerberosTransportInterceptor.class);
    public static final String KERBEROS = "GSSAPI";
    private final Map<String, Object> conf;

    public KerberosTransportInterceptor(Map<String, Object> conf) {
        this.conf = conf;
    }

    @Override
    public TTransportFactory getServerTransportFactory() throws IOException {
        Configuration loginConf = AuthUtil.getLoginConf(conf);
        if (loginConf == null) {
            return new TTransportFactory();
        }

        CallbackHandler callbackHandler = new ServerCallbackHandler(loginConf);
        //login our principal
        Subject subject = null;
        try {
            //specify a configuration object to be used
            Configuration.setConfiguration(loginConf);
            LoginContext login = new LoginContext(AuthUtil.LOGIN_CONTEXT_SERVER, callbackHandler);;
            //now login
            login.login();
            subject = login.getSubject();
        } catch (LoginException ex) {
            LOG.error("Server failed to login in principal:" + ex, ex);
            throw new RuntimeException(ex);
        }

        //check the credential of our principal
        if (subject.getPrivateCredentials(KerberosTicket.class).isEmpty()) {
            throw new RuntimeException("Fail to verify user principal with section \""
                    +AuthUtil.LOGIN_CONTEXT_SERVER+"\" in login configuration file "+ loginConf);
        }

        String principal = AuthUtil.getValueBySectionKey(loginConf, AuthUtil.LOGIN_CONTEXT_SERVER, "principal");
        LOG.info("nimbus server principal:"+principal);
        KerberosName serviceKerberosName = new KerberosName(principal);
        String serviceName = serviceKerberosName.getServiceName();
        String hostName = serviceKerberosName.getHostName();
        if (hostName == null || hostName.length() < 1) {
            hostName = "localhost";
        }

        Map<String, String> props = new TreeMap<String,String>();
        props.put(Sasl.QOP, "auth");
        props.put(Sasl.SERVER_AUTH, "false");

        //create a transport factory that will invoke our auth callback for digest
        TSaslServerTransport.Factory factory = new TSaslServerTransport.Factory();
        factory.addServerDefinition(KERBEROS, serviceName, hostName, props, callbackHandler);

        //create a wrap transport factory so that we could apply user credential during connections
        TUGIAssumingTransportFactory wrapFactory = new TUGIAssumingTransportFactory(factory, subject);

        LOG.info("SASL GSSAPI transport factory will be used");
        return wrapFactory;
    }

    @Override
    public TTransport getAndOpenClientTransport(TTransport transport, String serverHost, String asUser)
            throws TTransportException, IOException {
        Configuration loginConf = AuthUtil.getLoginConf(conf);
        if (loginConf == null) {
            throw new IOException("auth config is empty!");
        }

        CallbackHandler callbackHandler = new ClientCallbackHandler(loginConf);
        Subject subject = null;
        try {
            //specify a configuration object to be used
            Configuration.setConfiguration(loginConf);
            LoginContext login = new LoginContext(AuthUtil.LOGIN_CONTEXT_CLIENT, callbackHandler);
            login.login();
            //now login
            subject = login.getSubject();
        } catch (LoginException ex) {
            LOG.error("Server failed to login in principal:" + ex, ex);
            throw new RuntimeException(ex);
        }

        if (subject.getPrivateCredentials(KerberosTicket.class).isEmpty()) { //error
            throw new RuntimeException("Fail to verify user principal with section \""
                    +AuthUtil.LOGIN_CONTEXT_CLIENT+"\" in login configuration file "+ loginConf);
        }

        final String principal = (asUser == null || asUser.length() < 1)?getPrincipal(subject):asUser;

        String serviceName = AuthUtil.getValueBySectionKey(loginConf, AuthUtil.LOGIN_CONTEXT_CLIENT, "serviceName");
        if (serviceName == null) {
            serviceName = AuthUtil.DEFAULT_NIMBUS_SERVICE_NAME;
        }

        Map<String, String> props = new TreeMap<String,String>();
        props.put(Sasl.QOP, "auth");
        props.put(Sasl.SERVER_AUTH, "false");

        LOG.debug("SASL GSSAPI client transport is being established");
        final TTransport sasalTransport = new TSaslClientTransport(KERBEROS,
                principal,
                serviceName,
                serverHost,
                props,
                null,
                transport);

        //open Sasl transport with the login credential
        try {
            Subject.doAs(subject,
                    new PrivilegedExceptionAction<Void>() {
                        public Void run() {
                            try {
                                LOG.debug("do as:"+ principal);
                                sasalTransport.open();
                            } catch (Exception e) {
                                LOG.error("Client failed to open SaslClientTransport to interact with a server during session initiation: " + e, e);
                            }
                            return null;
                        }
                    });
        } catch (PrivilegedActionException e) {
            throw new RuntimeException(e);
        }

        return sasalTransport;
    }

    private String getPrincipal(Subject subject) {
        Set<Principal> principals = (Set<Principal>)subject.getPrincipals();
        if (principals==null || principals.size()<1) {
            LOG.info("No principal found in login subject");
            return null;
        }
        return ((Principal)(principals.toArray()[0])).getName();
    }

    static class TUGIAssumingTransportFactory extends TTransportFactory {
        private final Subject subject;
        private final TTransportFactory wrapped;

        public TUGIAssumingTransportFactory(TTransportFactory wrapped, Subject subject) {
            this.wrapped = wrapped;
            this.subject = subject;

            Set<Principal> principals = (Set<Principal>)subject.getPrincipals();
            if (principals.size()>0)
                LOG.info("Service principal:"+ ((Principal)(principals.toArray()[0])).getName());
        }

        @Override
        public TTransport getTransport(final TTransport trans) {
            try {
                return Subject.doAs(subject,
                        new PrivilegedExceptionAction<TTransport>() {
                            public TTransport run() {
                                try {
                                    return wrapped.getTransport(trans);
                                } catch (Exception e) {
                                    LOG.debug("Storm server failed to open transport " +
                                            "to interact with a client during session initiation: " + e, e);
                                    return null;
                                }
                            }
                        });
            } catch (PrivilegedActionException e) {
                LOG.error("Storm server experienced a PrivilegedActionException exception while creating a transport using a JAAS principal context:" + e, e);
                return null;
            }
        }
    }
}
