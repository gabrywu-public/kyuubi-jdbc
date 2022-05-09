package com.gabry.kyuubi.driver;

import java.sql.DriverPropertyInfo;
import java.util.Properties;

public class KyuubiDriverPropertyInfo {
  public static final String HOST_PROPERTY_KEY = "HOST";
  public static final String PORT_PROPERTY_KEY = "PORT";
  public static final String DBNAME_PROPERTY_KEY = "DBNAME";

  public static DriverPropertyInfo hostPropWith(Properties properties) {
    DriverPropertyInfo propertyInfo =
        new DriverPropertyInfo(HOST_PROPERTY_KEY, properties.getProperty(HOST_PROPERTY_KEY, ""));
    propertyInfo.required = false;
    propertyInfo.description = "Hostname of Kyuubi Server";
    return propertyInfo;
  }

  public static DriverPropertyInfo portPropWith(Properties properties) {
    DriverPropertyInfo propertyInfo =
        new DriverPropertyInfo(PORT_PROPERTY_KEY, properties.getProperty(PORT_PROPERTY_KEY, ""));
    propertyInfo.required = false;
    propertyInfo.description = "Port number of Kyuubi Server";
    return propertyInfo;
  }

  public static DriverPropertyInfo dbPropWith(Properties properties) {
    DriverPropertyInfo propertyInfo =
        new DriverPropertyInfo(
            DBNAME_PROPERTY_KEY, properties.getProperty(DBNAME_PROPERTY_KEY, "default"));
    propertyInfo.required = false;
    propertyInfo.description = "Database name";
    return propertyInfo;
  }
}
