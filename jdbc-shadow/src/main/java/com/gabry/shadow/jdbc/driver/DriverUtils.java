package com.gabry.shadow.jdbc.driver;

import com.gabry.shadow.jdbc.Utils.DriverCommon;
import com.gabry.shadow.jdbc.driver.exception.JdbcUrlParseException;

import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.sql.SQLException;
import java.util.Properties;
import java.util.jar.Attributes;
import java.util.jar.Manifest;
import java.util.regex.Pattern;

public class DriverUtils {
  private DriverUtils() {
    // do nothing
  }

  public static Attributes loadManifestAttributes(Class<?> clazz) throws IOException {
    URL classContainer = clazz.getProtectionDomain().getCodeSource().getLocation();
    URL manifestUrl = new URL("jar:" + classContainer + "!/META-INF/MANIFEST.MF");
    return new Manifest(manifestUrl.openStream()).getMainAttributes();
  }

  public static final Pattern jdbcUrlPattern =
      Pattern.compile(
          "(?<urlPrefix>jdbc:kyuubi)"
              + "//(?<connectUrl>("
              + "(?<user>(\\w+)(:?<passwd>(\\w+))?@)?"
              + "(?<host>(\\w+)(:?<port>(\\d+)))?"
              + "(/?<dbName>(\\w+))?\\?"
              + "(?<sessionConfig>((\\w+=\\w+)?(;\\w+=\\w+)*))?"
              + "(#(?<fragment>((\\w+=\\w)?(;\\w+=\\w+)*)))?"
              + "))?");

  public static KyuubiDriver.ConnectionInfo parseUrl(String urlStr, Properties prop)
      throws SQLException, MalformedURLException {
    KyuubiDriver.ConnectionInfo connectionInfo = new KyuubiDriver.ConnectionInfo();
    if (!urlStr.startsWith(DriverCommon.JDBC_URL_PREFIX)) {
      throw new JdbcUrlParseException(
          "Bad URL format: Missing prefix " + DriverCommon.JDBC_URL_PREFIX);
    }
    String urlWithoutJdbc = urlStr.substring("jdbc:".length());
    // scheme:[//[user[:password]@]host[:port]][/path][?query][#fragment]
    URL url = new URL(urlWithoutJdbc);
    connectionInfo.setHost(url.getHost());
    connectionInfo.setPort(url.getPort());
    connectionInfo.setDbName(url.getPath());
    return connectionInfo;
  }
}
