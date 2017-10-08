package org.apache.fake.storm.auth;

import org.apache.fake.storm.auth.DefaultTransportInterceptor;
import org.apache.fake.storm.auth.ITransportInterceptor;
import org.apache.fake.storm.auth.KerberosTransportInterceptor;

import javax.security.auth.login.AppConfigurationEntry;
import javax.security.auth.login.Configuration;
import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.security.URIParameter;
import java.util.Map;

/**
 * Created by yilong on 2017/7/3.
 */
public class AuthUtil {
    public static final String LOGIN_CONTEXT_SERVER = "StormServer";
    public static final String LOGIN_CONTEXT_CLIENT = "StormClient";
    public static final String DEFAULT_NIMBUS_SERVICE_NAME = "nimbus";

    public static Configuration getLoginConf(Map<String, Object> conf) {
        Configuration loginConf = null;

        //find login file configuration from Storm configuration
        String jaasFile = (String)conf.get("java.security.auth.login.config");
        if ((jaasFile != null) && (jaasFile.length()>0)) {
            File confFile = new File(jaasFile);
            if (! confFile.canRead()) {
                throw new RuntimeException("File " + jaasFile + " cannot be read.");
            }
            try {
                URI config_uri = confFile.toURI();
                loginConf = Configuration.getInstance("JavaLoginConfig", new URIParameter(config_uri));
            } catch (Exception ex) {
                throw new RuntimeException(ex);
            }
        }

        return loginConf;
    }

    public static ITransportInterceptor getTransportInterceptor(Map<String, Object> conf) {
        String jaasFile = (String)conf.get("java.security.auth.login.config");
        if (jaasFile == null || jaasFile.length() < 1) {
            return new DefaultTransportInterceptor(conf);
        }

        return new KerberosTransportInterceptor(conf);
    }

    public static String getValueBySectionKey(Configuration configuration, String section, String key) throws IOException {
        AppConfigurationEntry configurationEntries[] = configuration.getAppConfigurationEntry(section);
        if (configurationEntries == null) {
            String errorMessage = "Could not find a '"+ section + "' entry in this configuration.";
            throw new IOException(errorMessage);
        }

        for(AppConfigurationEntry entry: configurationEntries) {
            Object val = entry.getOptions().get(key);
            if (val != null)
                return (String)val;
        }
        return null;
    }
}
