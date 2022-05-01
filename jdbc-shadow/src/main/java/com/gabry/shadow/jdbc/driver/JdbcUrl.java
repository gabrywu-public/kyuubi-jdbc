package com.gabry.shadow.jdbc.driver;

import com.gabry.shadow.jdbc.driver.exception.JdbcUrlParseException;

import java.util.Arrays;
import java.util.Collections;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

public class JdbcUrl {
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
  private static final String dbNameReg = "?<" + DB_NAME_GROUP_NAME + ">" + DB_NAME_PREFIX + "\\w+";

  private static final String hostsReg =
      "?<" + HOSTS_GROUP_NAME + ">(\\w+(:\\d+)?)(,\\w+(:\\d+)?)*";
  private static final String sessionConfigsReg =
      "?<"
          + SESSION_CONFIGS_GROUP_NAME
          + ">\\"
          + SESSION_CONFIGS_PREFIX
          + "(\\w+=\\w+)(;\\w+=\\w+)*";
  private static final String engineConfigsReg =
      "?<" + ENGINE_GROUP_NAME + ">\\" + ENGINE_CONFIGS_PREFIX + "(\\w+=\\w+)(;\\w+=\\w+)*";

  public static final Pattern jdbcUrlPattern =
      Pattern.compile(
          new StringBuilder()
              .append("(")
              .append(schemaReg)
              .append(")")
              .append("//(")
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

  public static class HostInfo {
    private static final HostInfo[] empty = new HostInfo[0];
    private String host;
    private Integer port;

    private HostInfo() {
      // do nothing
    }

    public String getHost() {
      return host;
    }

    public Integer getPort() {
      return port;
    }

    @Override
    public String toString() {
      return "Host{" + "host='" + host + '\'' + ", port=" + port + '}';
    }
  }

  private String schema;
  private String user;
  private String password;
  private HostInfo[] hostInfos;
  private String dbName;
  private Map<String, String> sessionConfigs;
  private Map<String, String> engineConfigs;

  private JdbcUrl() {
    hostInfos = HostInfo.empty;
    sessionConfigs = Collections.emptyMap();
    engineConfigs = Collections.emptyMap();
  }

  public static JdbcUrl parse(String jdbcUrlStr) throws JdbcUrlParseException {
    Matcher matcher = JdbcUrl.jdbcUrlPattern.matcher(jdbcUrlStr);
    JdbcUrl jdbcUrl = new JdbcUrl();

    if (!matcher.matches()) {
      throw new JdbcUrlParseException(jdbcUrlStr);
    }
    jdbcUrl.schema = matcher.group(SCHEMA_GROUP_NAME);

    jdbcUrl.user = matcher.group(USER_GROUP_NAME);
    String password = matcher.group(PASSWORD_GROUP_NAME);
    if (null != password) {
      jdbcUrl.password = password.substring(PASSWD_PREFIX.length());
    }

    String hostGroup = matcher.group(HOSTS_GROUP_NAME);
    if (null != jdbcUrl.user && null == hostGroup) {
      throw new JdbcUrlParseException(jdbcUrlStr);
    }
    if (null != hostGroup) {
      jdbcUrl.hostInfos =
          Arrays.stream(hostGroup.split(",", -1))
              .map(
                  hostPort -> {
                    HostInfo hostInfo = new HostInfo();
                    String[] hostParts = hostPort.split(":", -1);
                    hostInfo.host = hostParts[0];
                    if (hostParts.length > 1) {
                      hostInfo.port = Integer.parseInt(hostParts[1]);
                    }
                    return hostInfo;
                  })
              .toArray(HostInfo[]::new);
    }
    String dbName = matcher.group(DB_NAME_GROUP_NAME);
    if (null != dbName) {
      jdbcUrl.dbName = dbName.substring(DB_NAME_PREFIX.length());
    }

    String sessionConfigs = matcher.group(SESSION_CONFIGS_GROUP_NAME);
    if (null != sessionConfigs) {
      jdbcUrl.sessionConfigs =
          Arrays.stream(sessionConfigs.substring(SESSION_CONFIGS_PREFIX.length()).split(";", -1))
              .map(config -> config.split("=", -1))
              .collect(Collectors.toMap(m -> m[0], m -> m[1]));
    }
    String engineConfigs = matcher.group(ENGINE_GROUP_NAME);
    if (null != engineConfigs) {
      jdbcUrl.engineConfigs =
          Arrays.stream(engineConfigs.substring(ENGINE_CONFIGS_PREFIX.length()).split(";", -1))
              .map(config -> config.split("=", -1))
              .collect(Collectors.toMap(m -> m[0], m -> m[1]));
    }
    return jdbcUrl;
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

  @Override
  public String toString() {
    return "JdbcUrl{"
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
        + '}';
  }
}
