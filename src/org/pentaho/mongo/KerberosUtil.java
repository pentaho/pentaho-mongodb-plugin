/*!
* Copyright 2010 - 2013 Pentaho Corporation.  All rights reserved.
*
* Licensed under the Apache License, Version 2.0 (the "License");
* you may not use this file except in compliance with the License.
* You may obtain a copy of the License at
*
* http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific language governing permissions and
* limitations under the License.
*
*/

package org.pentaho.mongo;

import java.util.HashMap;
import java.util.Map;

import javax.security.auth.Subject;
import javax.security.auth.login.AppConfigurationEntry;
import javax.security.auth.login.AppConfigurationEntry.LoginModuleControlFlag;
import javax.security.auth.login.Configuration;
import javax.security.auth.login.LoginContext;
import javax.security.auth.login.LoginException;

import com.sun.security.auth.module.Krb5LoginModule;

/**
 * A collection of utilities for working with Kerberos.
 * 
 * Note: This specifically does not support IBM VMs and must be modified to do
 * so: 1) LoginModule name differs, 2) Configuration defaults differ for ticket
 * cache, keytab, and others.
 * 
 * @author Jordan Ganoff <jganoff@pentaho.com>
 */
public class KerberosUtil {
  /**
   * The application name to use when creating login contexts.
   */
  private static final String KERBEROS_APP_NAME = "pentaho";

  /**
   * The environment property to set to enable JAAS debugging for the
   * LoginConfiguration created by this utility.
   */
  private static final String PENTAHO_JAAS_DEBUG = "PENTAHO_JAAS_DEBUG";

  /**
   * Base properties to be inherited by all other LOGIN_CONFIG* configuration
   * maps.
   */
  private static final Map<String, String> LOGIN_CONFIG_BASE;
  static {
    LOGIN_CONFIG_BASE = new HashMap<String, String>();
    // Enable JAAS debug if PENTAHO_JAAS_DEBUG is set
    if (Boolean.parseBoolean(System.getenv(PENTAHO_JAAS_DEBUG))) {
      LOGIN_CONFIG_BASE.put("debug", Boolean.TRUE.toString());
    }
  }

  /**
   * Login Configuration options for KERBEROS_USER mode.
   */
  private static final Map<String, String> LOGIN_CONFIG_OPTS_KERBEROS_USER;
  static {
    LOGIN_CONFIG_OPTS_KERBEROS_USER = new HashMap<String, String>(LOGIN_CONFIG_BASE);
    // Never prompt for passwords
    LOGIN_CONFIG_OPTS_KERBEROS_USER.put("doNotPrompt", Boolean.TRUE.toString());
    LOGIN_CONFIG_OPTS_KERBEROS_USER.put("useTicketCache", Boolean.TRUE.toString());
    // Attempt to renew tickets
    LOGIN_CONFIG_OPTS_KERBEROS_USER.put("renewTGT", Boolean.TRUE.toString());
    // Set the ticket cache if it was defined externally
    String ticketCache = System.getenv("KRB5CCNAME");
    if (ticketCache != null) {
      LOGIN_CONFIG_OPTS_KERBEROS_USER.put("ticketCache", ticketCache);
    }
  }

  /**
   * Login Configuration options for KERBEROS_KEYTAB mode.
   */
  private static final Map<String, String> LOGIN_CONFIG_OPTS_KERBEROS_KEYTAB;
  static {
    LOGIN_CONFIG_OPTS_KERBEROS_KEYTAB = new HashMap<String, String>(LOGIN_CONFIG_BASE);
    // Never prompt for passwords
    LOGIN_CONFIG_OPTS_KERBEROS_KEYTAB.put("doNotPrompt", Boolean.TRUE.toString());
    // Use a keytab file
    LOGIN_CONFIG_OPTS_KERBEROS_KEYTAB.put("useKeyTab", Boolean.TRUE.toString());
    LOGIN_CONFIG_OPTS_KERBEROS_KEYTAB.put("storeKey", Boolean.TRUE.toString());
    // Refresh KRB5 config before logging in
    LOGIN_CONFIG_OPTS_KERBEROS_KEYTAB.put("refreshKrb5Config", Boolean.TRUE.toString());
  }

  /**
   * The Login Configuration entry to use for authenticating with Kerberos.
   */
  private static final AppConfigurationEntry CONFIG_ENTRY_PENTAHO_KERBEROS_USER = new AppConfigurationEntry(
      Krb5LoginModule.class.getName(), LoginModuleControlFlag.REQUIRED, LOGIN_CONFIG_OPTS_KERBEROS_USER);

  /**
   * Static configuration to use when KERBEROS_USER mode is enabled.
   */
  private static final AppConfigurationEntry[] CONFIG_ENTRIES_KERBEROS_USER = new AppConfigurationEntry[] { CONFIG_ENTRY_PENTAHO_KERBEROS_USER };

  /**
   * A Login Configuration that is pre-configured based on our static
   * configuration.
   */
  private static class PentahoLoginConfiguration extends Configuration {
    private AppConfigurationEntry[] entries;

    public PentahoLoginConfiguration(AppConfigurationEntry[] entries) {
      if (entries == null) {
        throw new NullPointerException("AppConfigurationEntry[] is required");
      }
      this.entries = entries;
    }

    @Override
    public AppConfigurationEntry[] getAppConfigurationEntry(String ignored) {
      return entries;
    }
  }

  /**
   * Defines the types of Kerberos authentication modes we support.
   */
  public static enum JaasAuthenticationMode {
    /**
     * User has pre-authenticated with Kerberos (likely via kinit) and has
     * launched this process within that authenticated environment.
     */
    KERBEROS_USER,

    /**
     * A keytab file must be used to authenticate.
     */
    KERBEROS_KEYTAB,

    /**
     * A default authentication mode to bypass our static configuration. This is
     * to be used with an externally configured JAAS Configuration file.
     */
    EXTERNAL;
  };

  /**
   * Log in as the provided principal. If a keytab file is specified in the
   * environment property "PENTAHO_JAAS_KEYTAB_FILE", it will be used during
   * authentication.
   * 
   * @see #loginAs(String, String) loginAs(principal,
   *      env("PENTAHO_KEYTAB_FILE"))
   */
  public static LoginContext loginAs(String principal) throws LoginException {
    return loginAs(JaasAuthenticationMode.KERBEROS_USER, principal, null);
  }

  /**
   * Log in as the provided principal. This assumes the user has already
   * authenticated with kerberos and a TGT exists in the cache.
   * 
   * @param principal
   *          Principal to login in as.
   * @param keytabFile
   * @return The context for the logged in principal.
   * @throws LoginException
   *           Error encountered while logging in.
   */
  public static LoginContext loginAs(JaasAuthenticationMode authMode, String principal, String keytabFile) throws LoginException {
    LoginContext lc;
    Subject subject;
    switch (authMode) {
    case EXTERNAL:
      // Use the default JAAS configuration by only supplying the app name
      lc = new LoginContext(KERBEROS_APP_NAME);
    case KERBEROS_USER:
      subject = new Subject();
      lc = new LoginContext(KERBEROS_APP_NAME, subject, null, new PentahoLoginConfiguration(CONFIG_ENTRIES_KERBEROS_USER));
      break;
    case KERBEROS_KEYTAB:
      lc = createLoginContextWithKeytab(principal, keytabFile);
      break;
    default:
      throw new IllegalArgumentException("Unsupported authentication mode: " + authMode);
    }
    // Perform the login
    lc.login();
    return lc;
  }

  /**
   * Creates a {@link LoginContext} configured to authenticate with the provided
   * credentials.
   * 
   * @param principal
   *          Principal to authenticate as.
   * @param keytabFile
   *          Keytab file with credentials to authenticate as the given
   *          principal.
   * @return A login context configured to authenticate as the provided
   *         principal via a keytab.
   * @throws LoginException
   *           Error creating login context.
   */
  private static LoginContext createLoginContextWithKeytab(String principal, String keytabFile) throws LoginException {
    if (keytabFile == null) {
      throw new IllegalArgumentException("A keytab file is required to authenticate with Kerberos via keytab");
    }

    // Extend the default keytab config properties and set the necessary
    // overrides for this invocation
    Map<String, String> keytabConfig = new HashMap<String, String>(LOGIN_CONFIG_OPTS_KERBEROS_KEYTAB);
    keytabConfig.put("keyTab", keytabFile);
    keytabConfig.put("principal", principal);

    // Create the configuration and from them, a new login context
    AppConfigurationEntry config = new AppConfigurationEntry(Krb5LoginModule.class.getName(), LoginModuleControlFlag.REQUIRED, keytabConfig);
    AppConfigurationEntry[] configEntries = new AppConfigurationEntry[] { config };
    Subject subject = new Subject();
    return new LoginContext(KERBEROS_APP_NAME, subject, null, new PentahoLoginConfiguration(configEntries));
  }
}
