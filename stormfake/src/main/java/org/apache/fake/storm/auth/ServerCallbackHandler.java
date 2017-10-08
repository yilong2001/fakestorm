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
public class ServerCallbackHandler implements CallbackHandler {
    private static final Logger LOG = LoggerFactory.getLogger(ServerCallbackHandler.class);

    public ServerCallbackHandler(Configuration configuration) throws IOException{
        if (configuration == null) {
            return;
        }

        AppConfigurationEntry configurationEntries[] = configuration.getAppConfigurationEntry(AuthUtil.LOGIN_CONTEXT_SERVER);
        if (configurationEntries == null) {
            String errorMessage = "Could not find a '" + AuthUtil.LOGIN_CONTEXT_SERVER + "' entry ";
            LOG.error(errorMessage);
            throw new IOException(errorMessage);
        }
    }

    @Override
    public void handle(Callback[] callbacks) throws IOException, UnsupportedCallbackException {
        for (Callback callback : callbacks) {
            if (callback instanceof NameCallback) {
                LOG.warn("do not support NameCallback");
            } else if (callback instanceof PasswordCallback) {
                LOG.warn("do not support PasswordCallback");
            } else if (callback instanceof AuthorizeCallback) {
                AuthorizeCallback cb = (AuthorizeCallback)callback;
                String authenId = cb.getAuthenticationID();
                if (cb.getAuthorizationID() == null) cb.setAuthorizedID(authenId);
                cb.setAuthorized(true);
            } else {
                throw new UnsupportedCallbackException(callback);
            }
        }
    }
}
