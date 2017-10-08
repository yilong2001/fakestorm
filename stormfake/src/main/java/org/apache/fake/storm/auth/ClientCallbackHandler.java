package org.apache.fake.storm.auth;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.security.auth.callback.*;
import javax.security.auth.login.AppConfigurationEntry;
import javax.security.auth.login.Configuration;
import javax.security.sasl.AuthorizeCallback;
import java.io.IOException;

/**
 * Created by yilong on 2017/7/3.
 */
public class ClientCallbackHandler implements CallbackHandler {
    private static final Logger LOG = LoggerFactory.getLogger(ClientCallbackHandler.class);

    public ClientCallbackHandler(Configuration configuration) throws IOException{
        if (configuration == null) {
            return;
        }

        AppConfigurationEntry configurationEntries[] = configuration.getAppConfigurationEntry(AuthUtil.LOGIN_CONTEXT_CLIENT);
        if (configurationEntries == null) {
            String errorMessage = "Could not find a '" + AuthUtil.LOGIN_CONTEXT_CLIENT + "' entry ";
            LOG.error(errorMessage);
            throw new IOException(errorMessage);
        }
    }

    @Override
    public void handle(Callback[] callbacks) throws IOException, UnsupportedCallbackException {
        for (Callback callback : callbacks) {
            if (callback instanceof NameCallback) {
                LOG.error("do not support NameCallback");
            } else if (callback instanceof PasswordCallback) {
                LOG.error("do not support PasswordCallback");
            } else if (callback instanceof AuthorizeCallback) {
                AuthorizeCallback cb = (AuthorizeCallback)callback;
                String authid = cb.getAuthenticationID();
                String authzid = cb.getAuthorizationID();
                if (authid.equals(authzid)) {
                    cb.setAuthorized(true);
                } else {
                    cb.setAuthorized(false);
                }
                if (cb.isAuthorized()) {
                    cb.setAuthorizedID(authzid);
                }
            } else {
                throw new UnsupportedCallbackException(callback);
            }
        }
    }
}
