package com.gabry.shadow.kyuubi.driver;

import com.gabry.shadow.kyuubi.driver.exception.JdbcUrlParseException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.Collections;
import java.util.Map;
import java.util.Properties;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

public class ConnectionInfo {
  private static Logger logger = LoggerFactory.getLogger(ConnectionInfo.class);
  public static final String SCHEMA_GROUP_NAME = "schema";
  public static final String USER_GROUP_NAME = "user";
  public static final String PASSWORD_GROUP_NAME = "password";
  public static final String DB_NAME_GROUP_NAME = "dbName";
  public static final String SESSION_CONFIGS_GROUP_NAME = "sessionConfigs";
  public static final String HOSTS_GROUP_NAME = "hosts";
  public static final String ENGINE_GROUP_NAME = "engineConfigs";
  private static final String schemaReg = "?<" + SCHEMA_GROUP_NAME + ">jdbc:kyuubi";
  private static final String userReg = "?<" + USER_GROUP_NAME + ">\\w+";
  private static final String PASSWD_PREFIX = ":";
  private static final String passwordReg =
      "?<" + PASSWORD_GROUP_NAME + ">" + PASSWD_PREFIX + "\\w+";
  private static final String DB_NAME_PREFIX = "/";
  private static final String SESSION_CONFIGS_PREFIX = "?";
  private static final String ENGINE_CONFIGS_PREFIX = "#";
  private static final String idReg = "(\\w|\\-|\\.)+?";
  private static final String dbNameReg = "?<" + DB_NAME_GROUP_NAME + ">" + DB_NAME_PREFIX + idReg;

  private static final String hostsReg =
      "?<" + HOSTS_GROUP_NAME + ">(" + idReg + "(:\\d+)?)(," + idReg + "(:\\d+)?)*";
  private static final String sessionConfigsReg =
      "?<"
          + SESSION_CONFIGS_GROUP_NAME
          + ">\\"
          + SESSION_CONFIGS_PREFIX
          + "("
          + idReg
          + "="
          + idReg
          + ")(;"
          + idReg
          + "="
          + idReg
          + ")*";
  private static final String engineConfigsReg =
      "?<"
          + ENGINE_GROUP_NAME
          + ">\\"
          + ENGINE_CONFIGS_PREFIX
          + "("
          + idReg
          + "="
          + idReg
          + ")(;"
          + idReg
          + "="
          + idReg
          + ")*";

  public static final Pattern jdbcUrlPattern =
      Pattern.compile(
          new StringBuilder()
              .append("(")
              .append(schemaReg)
              .append(")")
              .append("://(")
              .append("(") // connect url part start
              .append("(") // user info part start
              .append("(")
              .append(userReg) // user part
              .append(")")
              .append("(")
              .append(passwordReg) // password part
              .append(")?")
              .append("@)?") // user info part end
              .append("(")
              .append(hostsReg) // hosts part
              .append(")?)?")
              .append("(")
              .append(dbNameReg) // db name part
              .append(")?") // connect url part end
              .append("(")
              .append(sessionConfigsReg) // session configs part
              .append(")?")
              .append("(")
              .append(engineConfigsReg) // fragment configs part
              .append(")?")
              .append(")?")
              .toString());

  private String schema;
  private String user;
  private String password;
  private HostInfo[] hostInfos;
  private String dbName;
  private Map<String, String> sessionConfigs;
  private Map<String, String> engineConfigs;
  private Properties sessionProperties;

  private ConnectionInfo() {
    hostInfos = HostInfo.empty;
    sessionConfigs = Collections.emptyMap();
    engineConfigs = Collections.emptyMap();
  }

  public static ConnectionInfo parse(String jdbcUrlStr, Properties sessionProps)
      throws JdbcUrlParseException {
    ConnectionInfo connectionInfo = parse(jdbcUrlStr);
    connectionInfo.sessionProperties = sessionProps;
    return connectionInfo;
  }

  public static ConnectionInfo parse(String jdbcUrlStr) throws JdbcUrlParseException {
    logger.debug("jdbcUrlPattern = [{}]", ConnectionInfo.jdbcUrlPattern);
    Matcher matcher = ConnectionInfo.jdbcUrlPattern.matcher(jdbcUrlStr);
    ConnectionInfo connectionInfo = new ConnectionInfo();

    if (!matcher.matches()) {
      throw new JdbcUrlParseException("invalid url " + jdbcUrlStr);
    }
    connectionInfo.schema = matcher.group(SCHEMA_GROUP_NAME);

    connectionInfo.user = matcher.group(USER_GROUP_NAME);
    String password = matcher.group(PASSWORD_GROUP_NAME);
    if (null != password) {
      connectionInfo.password = password.substring(PASSWD_PREFIX.length());
    }

    String hostGroup = matcher.group(HOSTS_GROUP_NAME);
    if (null != connectionInfo.user && null == hostGroup) {
      throw new JdbcUrlParseException(jdbcUrlStr);
    }
    if (null != hostGroup) {
      connectionInfo.hostInfos =
          Arrays.stream(hostGroup.split(",", -1))
              .map(
                  hostPort -> {
                    String[] hostParts = hostPort.split(":", -1);
                    return hostParts.length > 1
                        ? new HostInfo(hostParts[0], Integer.parseInt(hostParts[1]))
                        : new HostInfo(hostParts[0]);
                  })
              .toArray(HostInfo[]::new);
    }
    String dbName = matcher.group(DB_NAME_GROUP_NAME);
    if (null != dbName) {
      connectionInfo.dbName = dbName.substring(DB_NAME_PREFIX.length());
    }

    String sessionConfigs = matcher.group(SESSION_CONFIGS_GROUP_NAME);
    if (null != sessionConfigs) {
      connectionInfo.sessionConfigs =
          Arrays.stream(sessionConfigs.substring(SESSION_CONFIGS_PREFIX.length()).split(";", -1))
              .map(config -> config.split("=", -1))
              .collect(Collectors.toMap(m -> m[0], m -> m[1]));
    }
    String engineConfigs = matcher.group(ENGINE_GROUP_NAME);
    if (null != engineConfigs) {
      connectionInfo.engineConfigs =
          Arrays.stream(engineConfigs.substring(ENGINE_CONFIGS_PREFIX.length()).split(";", -1))
              .map(config -> config.split("=", -1))
              .collect(Collectors.toMap(m -> m[0], m -> m[1]));
    }
    return connectionInfo;
  }

  public String getSchema() {
    return schema;
  }

  public String getUser() {
    return user;
  }

  public String getPassword() {
    return password;
  }

  public HostInfo[] getHosts() {
    return hostInfos;
  }

  public String getDbName() {
    return dbName;
  }

  public Map<String, String> getSessionConfigs() {
    return sessionConfigs;
  }

  public Map<String, String> getEngineConfigs() {
    return engineConfigs;
  }

  public Properties getSessionProperties() {
    return sessionProperties;
  }

  @Override
  public String toString() {
    return "ConnectionInfo{"
        + "schema='"
        + schema
        + '\''
        + ", user='"
        + user
        + '\''
        + ", password='"
        + password
        + '\''
        + ", hostInfos="
        + Arrays.toString(hostInfos)
        + ", dbName='"
        + dbName
        + '\''
        + ", sessionConfigs="
        + sessionConfigs
        + ", engineConfigs="
        + engineConfigs
        + ", sessionProperties="
        + sessionProperties
        + '}';
  }
}
