package com.gabry.kyuubi.driver;

import com.gabry.kyuubi.jdbc.KyuubiConnection;
import com.gabry.kyuubi.utils.DriverCommon;

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
  public Connection connect(String url, Properties sessionProps) throws SQLException {
    return acceptsURL(url)
        ? new KyuubiConnection(ConnectionInfo.parse(url, sessionProps)).open()
        : null;
  }

  @Override
  public boolean acceptsURL(String url) throws SQLException {
    return url != null && url.startsWith(DriverCommon.JDBC_URL_PREFIX);
  }

  @Override
  public DriverPropertyInfo[] getPropertyInfo(String url, Properties info) throws SQLException {
    Properties properties = new Properties(info);
    DriverPropertyInfo[] dpi = new DriverPropertyInfo[3];
    dpi[0] = KyuubiDriverPropertyInfo.hostPropWith(properties);
    dpi[1] = KyuubiDriverPropertyInfo.portPropWith(properties);
    dpi[2] = KyuubiDriverPropertyInfo.dbPropWith(properties);
    return dpi;
  }

  public static String getDriverName() throws SQLException {
    if (manifestAttributes != null) {
      try {
        manifestAttributes = DriverUtils.loadManifestAttributes(KyuubiDriver.class);
      } catch (IOException e) {
        throw new SQLException(e);
      }
    }
    assert manifestAttributes != null;
    return manifestAttributes.getValue(Attributes.Name.IMPLEMENTATION_TITLE);
  }

  public static String getDriverManifestVersionAttribute() throws SQLException {
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

  public static int getVersionAt(int index) {
    int version = -1;
    try {
      String versionStr = getDriverManifestVersionAttribute();
      String[] versionTokens = versionStr.split("\\.", -1);
      if (versionTokens.length >= (index + 1)) {
        version = Integer.parseInt(versionTokens[index]);
      }
    } catch (SQLException e) {
      throw new RuntimeException(e);
    }

    return version;
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
}
