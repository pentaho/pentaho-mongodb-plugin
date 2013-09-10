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

  private static final Map<String, String> LOGIN_CONFIG_KERBEROS;
  static {
    LOGIN_CONFIG_KERBEROS = new HashMap<String, String>();
    // Never prompt for passwords
    LOGIN_CONFIG_KERBEROS.put("doNotPrompt", Boolean.TRUE.toString());
    LOGIN_CONFIG_KERBEROS.put("useTicketCache", Boolean.TRUE.toString());
    // Attempt to renew tickets
    LOGIN_CONFIG_KERBEROS.put("renewTGT", Boolean.TRUE.toString());
    // Set the ticket cache if it was defined externally
    String ticketCache = System.getenv("KRB5CCNAME");
    if (ticketCache != null) {
      LOGIN_CONFIG_KERBEROS.put("ticketCache", ticketCache);
    }
  }

  // The Login Configuration entry to use for authenticating with Kerberos
  private static final AppConfigurationEntry CONFIG_ENTRY_PENTAHO_KERBEROS = new AppConfigurationEntry(Krb5LoginModule.class.getName(),
      LoginModuleControlFlag.REQUIRED, LOGIN_CONFIG_KERBEROS);

  private static final AppConfigurationEntry[] CONFIG_ENTRIES = new AppConfigurationEntry[] { CONFIG_ENTRY_PENTAHO_KERBEROS };

  /**
   * A Login Configuration that is configured statically within this class.
   */
  private static class StaticConfiguration extends Configuration {
    @Override
    public AppConfigurationEntry[] getAppConfigurationEntry(String ignored) {
      return CONFIG_ENTRIES;
    }
  }

  /**
   * Login as the provided principal. This assumes the user has already
   * authenticated with kerberos and a TGT exists in the cache.
   * 
   * @param principal
   *          Principal to login in as.
   * @return
   * @throws LoginException
   */
  public LoginContext loginAs(String principal) throws LoginException {
    Subject subject = new Subject();
    LoginContext lc = new LoginContext(KERBEROS_APP_NAME, subject, null, new StaticConfiguration());
    lc.login();
    return lc;
  }
}
