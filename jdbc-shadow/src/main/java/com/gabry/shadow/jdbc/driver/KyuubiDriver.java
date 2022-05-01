package com.gabry.shadow.jdbc.driver;

import com.gabry.shadow.jdbc.Utils.DriverCommon;

import java.io.IOException;
import java.sql.*;
import java.util.Properties;
import java.util.jar.Attributes;
import java.util.logging.Logger;

public class KyuubiDriver implements Driver {
  private static Attributes manifestAttributes = null;

  static {
    try {
      DriverManager.registerDriver(new KyuubiDriver());
    } catch (SQLException e) {
      throw new RuntimeException("Failed to register driver", e);
    }
  }

  @Override
  public Connection connect(String url, Properties info) throws SQLException {
    return null;
  }

  @Override
  public boolean acceptsURL(String url) throws SQLException {
    return url != null && url.startsWith(DriverCommon.JDBC_URL_PREFIX);
  }

  @Override
  public DriverPropertyInfo[] getPropertyInfo(String url, Properties info) throws SQLException {
    Properties properties = new Properties(info);
    if (acceptsURL(url)) {
      JdbcUrl jdbcUrl = JdbcUrl.parse(url);
      properties.put(KyuubiDriverPropertyInfo.HOST_PROPERTY_KEY, jdbcUrl.getHosts());
      properties.put(KyuubiDriverPropertyInfo.DBNAME_PROPERTY_KEY, jdbcUrl.getDbName());
    }
    DriverPropertyInfo[] dpi = new DriverPropertyInfo[3];
    dpi[0] = KyuubiDriverPropertyInfo.hostPropWith(properties);
    dpi[1] = KyuubiDriverPropertyInfo.portPropWith(properties);
    dpi[2] = KyuubiDriverPropertyInfo.dbPropWith(properties);
    return dpi;
  }

  private static String getDriverManifestVersionAttribute() throws SQLException {
    try {
      if (manifestAttributes != null) {
        manifestAttributes = DriverUtils.loadManifestAttributes(KyuubiDriver.class);
      }
      assert manifestAttributes != null;
      return manifestAttributes.getValue(Attributes.Name.IMPLEMENTATION_VERSION);
    } catch (IOException e) {
      throw new SQLException(
          "Can't find manifest attribute: " + Attributes.Name.IMPLEMENTATION_VERSION, e);
    }
  }

  private int getVersionAt(int index) {
    int majorVersion = -1;
    try {
      String version = getDriverManifestVersionAttribute();
      String[] versionTokens = version.split("\\.", -1);
      if (versionTokens.length >= (index + 1)) {
        majorVersion = Integer.parseInt(versionTokens[index]);
      }
    } catch (SQLException e) {
      throw new RuntimeException(e);
    }

    return majorVersion;
  }

  @Override
  public int getMajorVersion() {
    return getVersionAt(0);
  }

  @Override
  public int getMinorVersion() {
    return getVersionAt(1);
  }

  @Override
  public boolean jdbcCompliant() {
    return false;
  }

  @Override
  public Logger getParentLogger() throws SQLFeatureNotSupportedException {
    throw new SQLFeatureNotSupportedException("Method getParentLogger is not supported");
  }

  public static class ConnectionInfo {
    private String host;
    private int port;
    private String dbName;

    public ConnectionInfo() {
      // do nothing
    }

    public String getHost() {
      return host;
    }

    public void setHost(String host) {
      this.host = host;
    }

    public int getPort() {
      return port;
    }

    public void setPort(int port) {
      this.port = port;
    }

    public String getDbName() {
      return dbName;
    }

    public void setDbName(String dbName) {
      this.dbName = dbName;
    }

    @Override
    public String toString() {
      return "ConnectionInfo{"
          + "host='"
          + host
          + '\''
          + ", port="
          + port
          + ", dbName='"
          + dbName
          + '\''
          + '}';
    }
  }
}
