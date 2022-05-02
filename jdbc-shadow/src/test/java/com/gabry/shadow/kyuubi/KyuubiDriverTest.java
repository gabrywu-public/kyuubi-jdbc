package com.gabry.shadow.kyuubi;

import com.gabry.shadow.kyuubi.utils.DriverCommon;
import com.gabry.shadow.kyuubi.driver.ConnectionInfo;
import com.gabry.shadow.kyuubi.driver.KyuubiDriver;
import com.gabry.shadow.kyuubi.driver.exception.JdbcUrlParseException;
import org.junit.Assert;
import org.junit.Test;

import java.sql.SQLException;
import java.util.regex.Pattern;

public class KyuubiDriverTest {
  @Test
  public void testAcceptsURL() throws SQLException {
    KyuubiDriver kyuubiDriver = new KyuubiDriver();
    Assert.assertTrue(kyuubiDriver.acceptsURL(DriverCommon.JDBC_URL_PREFIX));
    Assert.assertTrue(kyuubiDriver.acceptsURL(DriverCommon.JDBC_URL_PREFIX + "localhost:port"));
    Assert.assertFalse(kyuubiDriver.acceptsURL("localhost:port" + DriverCommon.JDBC_URL_PREFIX));
  }

  @Test
  public void testInvalidJdbcUrl() throws JdbcUrlParseException {
    Assert.assertThrows(JdbcUrlParseException.class, () -> ConnectionInfo.parse("jdbc:kyuubis//"));
    Assert.assertThrows(JdbcUrlParseException.class, () -> ConnectionInfo.parse("jdbc:hive2//"));
    Assert.assertThrows(JdbcUrlParseException.class, () -> ConnectionInfo.parse("jdbc:hive//"));
    Assert.assertThrows(
        JdbcUrlParseException.class, () -> ConnectionInfo.parse("jdbc:kyuubi://user:password"));
    Assert.assertThrows(
        JdbcUrlParseException.class, () -> ConnectionInfo.parse("jdbc:kyuubi://user:password@"));
    Assert.assertThrows(JdbcUrlParseException.class, () -> ConnectionInfo.parse("jdbc:kyuubi://host:port"));
    Assert.assertThrows(
        JdbcUrlParseException.class, () -> ConnectionInfo.parse("jdbc:kyuubi://host:123,host:sss"));
    ConnectionInfo connectionInfo = ConnectionInfo.parse("jdbc:kyuubi://");
    Assert.assertEquals("jdbc:kyuubi", connectionInfo.getSchema());
    Assert.assertNull(connectionInfo.getUser());
    Assert.assertNull(connectionInfo.getPassword());
    Assert.assertEquals(0,connectionInfo.getHosts().length);
    Assert.assertNull(connectionInfo.getDbName());
    Assert.assertTrue(connectionInfo.getSessionConfigs().isEmpty());
    Assert.assertTrue(connectionInfo.getEngineConfigs().isEmpty());
  }
  @Test
  public void testId(){
    String idReg = "(\\w|\\.|-)+";
    Pattern pattern = Pattern.compile(idReg);
    Assert.assertTrue(pattern.matcher("tes.t").matches());
  }

  @Test
  public void testJdbcUrl() throws JdbcUrlParseException {
    ConnectionInfo connectionInfo = ConnectionInfo.parse("jdbc:kyuubi://localhost:1203");
    Assert.assertEquals("jdbc:kyuubi", connectionInfo.getSchema());
    Assert.assertNull(connectionInfo.getUser());
    Assert.assertNull(connectionInfo.getPassword());
    Assert.assertNotNull(connectionInfo.getHosts());
    Assert.assertEquals(1, connectionInfo.getHosts().length);
    Assert.assertEquals("localhost", connectionInfo.getHosts()[0].getHost());
    Assert.assertNotNull(connectionInfo.getHosts()[0].getPort());
    Assert.assertEquals(1203, connectionInfo.getHosts()[0].getPort().intValue());
    Assert.assertNull(connectionInfo.getDbName());
    Assert.assertTrue(connectionInfo.getSessionConfigs().isEmpty());
    Assert.assertTrue(connectionInfo.getEngineConfigs().isEmpty());

    connectionInfo = ConnectionInfo.parse("jdbc:kyuubi://local-host.name:1203");
    Assert.assertEquals("jdbc:kyuubi", connectionInfo.getSchema());
    Assert.assertNull(connectionInfo.getUser());
    Assert.assertNull(connectionInfo.getPassword());
    Assert.assertNotNull(connectionInfo.getHosts());
    Assert.assertEquals(1, connectionInfo.getHosts().length);
    Assert.assertEquals("local-host.name", connectionInfo.getHosts()[0].getHost());
    Assert.assertNotNull(connectionInfo.getHosts()[0].getPort());
    Assert.assertEquals(1203, connectionInfo.getHosts()[0].getPort().intValue());
    Assert.assertNull(connectionInfo.getDbName());
    Assert.assertTrue(connectionInfo.getSessionConfigs().isEmpty());
    Assert.assertTrue(connectionInfo.getEngineConfigs().isEmpty());

    connectionInfo = ConnectionInfo.parse("jdbc:kyuubi://localhost1:1203,localhost2");
    Assert.assertEquals("jdbc:kyuubi", connectionInfo.getSchema());
    Assert.assertNull(connectionInfo.getUser());
    Assert.assertNull(connectionInfo.getPassword());
    Assert.assertNotNull(connectionInfo.getHosts());
    Assert.assertEquals(2, connectionInfo.getHosts().length);
    Assert.assertEquals("localhost1", connectionInfo.getHosts()[0].getHost());
    Assert.assertNotNull(connectionInfo.getHosts()[0].getPort());
    Assert.assertEquals(1203, connectionInfo.getHosts()[0].getPort().intValue());
    Assert.assertEquals("localhost2", connectionInfo.getHosts()[1].getHost());
    Assert.assertNull(connectionInfo.getHosts()[1].getPort());
    Assert.assertNull(connectionInfo.getDbName());
    Assert.assertTrue(connectionInfo.getSessionConfigs().isEmpty());
    Assert.assertTrue(connectionInfo.getEngineConfigs().isEmpty());

    connectionInfo = ConnectionInfo.parse("jdbc:kyuubi://user@localhost1:1203,localhost2");
    Assert.assertEquals("jdbc:kyuubi", connectionInfo.getSchema());
    Assert.assertEquals("user", connectionInfo.getUser());
    Assert.assertNull(connectionInfo.getPassword());
    Assert.assertNotNull(connectionInfo.getHosts());
    Assert.assertEquals(2, connectionInfo.getHosts().length);
    Assert.assertEquals("localhost1", connectionInfo.getHosts()[0].getHost());
    Assert.assertNotNull(connectionInfo.getHosts()[0].getPort());
    Assert.assertEquals(1203, connectionInfo.getHosts()[0].getPort().intValue());
    Assert.assertEquals("localhost2", connectionInfo.getHosts()[1].getHost());
    Assert.assertNull(connectionInfo.getHosts()[1].getPort());
    Assert.assertNull(connectionInfo.getDbName());
    Assert.assertTrue(connectionInfo.getSessionConfigs().isEmpty());
    Assert.assertTrue(connectionInfo.getEngineConfigs().isEmpty());

    connectionInfo = ConnectionInfo.parse("jdbc:kyuubi://user:passwd@localhost1:1203,localhost2");
    Assert.assertEquals("jdbc:kyuubi", connectionInfo.getSchema());
    Assert.assertEquals("user", connectionInfo.getUser());
    Assert.assertEquals("passwd", connectionInfo.getPassword());
    Assert.assertNotNull(connectionInfo.getHosts());
    Assert.assertEquals(2, connectionInfo.getHosts().length);
    Assert.assertEquals("localhost1", connectionInfo.getHosts()[0].getHost());
    Assert.assertNotNull(connectionInfo.getHosts()[0].getPort());
    Assert.assertEquals(1203, connectionInfo.getHosts()[0].getPort().intValue());
    Assert.assertEquals("localhost2", connectionInfo.getHosts()[1].getHost());
    Assert.assertNull(connectionInfo.getHosts()[1].getPort());
    Assert.assertNull(connectionInfo.getDbName());
    Assert.assertTrue(connectionInfo.getSessionConfigs().isEmpty());
    Assert.assertTrue(connectionInfo.getEngineConfigs().isEmpty());

    connectionInfo = ConnectionInfo.parse("jdbc:kyuubi://user:passwd@localhost1:1203,localhost2/db_name");
    Assert.assertEquals("jdbc:kyuubi", connectionInfo.getSchema());
    Assert.assertEquals("user", connectionInfo.getUser());
    Assert.assertEquals("passwd", connectionInfo.getPassword());
    Assert.assertNotNull(connectionInfo.getHosts());
    Assert.assertEquals(2, connectionInfo.getHosts().length);
    Assert.assertEquals("localhost1", connectionInfo.getHosts()[0].getHost());
    Assert.assertNotNull(connectionInfo.getHosts()[0].getPort());
    Assert.assertEquals(1203, connectionInfo.getHosts()[0].getPort().intValue());
    Assert.assertEquals("localhost2", connectionInfo.getHosts()[1].getHost());
    Assert.assertNull(connectionInfo.getHosts()[1].getPort());
    Assert.assertEquals("db_name", connectionInfo.getDbName());
    Assert.assertTrue(connectionInfo.getSessionConfigs().isEmpty());
    Assert.assertTrue(connectionInfo.getEngineConfigs().isEmpty());

    connectionInfo =
        ConnectionInfo.parse(
            "jdbc:kyuubi://user:passwd@localhost1:1203,localhost2/db_name?sessKey1=sessValue1");
    Assert.assertEquals("jdbc:kyuubi", connectionInfo.getSchema());
    Assert.assertEquals("user", connectionInfo.getUser());
    Assert.assertEquals("passwd", connectionInfo.getPassword());
    Assert.assertNotNull(connectionInfo.getHosts());
    Assert.assertEquals(2, connectionInfo.getHosts().length);
    Assert.assertEquals("localhost1", connectionInfo.getHosts()[0].getHost());
    Assert.assertNotNull(connectionInfo.getHosts()[0].getPort());
    Assert.assertEquals(1203, connectionInfo.getHosts()[0].getPort().intValue());
    Assert.assertEquals("localhost2", connectionInfo.getHosts()[1].getHost());
    Assert.assertNull(connectionInfo.getHosts()[1].getPort());
    Assert.assertEquals("db_name", connectionInfo.getDbName());
    Assert.assertNotNull(connectionInfo.getSessionConfigs());
    Assert.assertEquals(1, connectionInfo.getSessionConfigs().size());
    Assert.assertEquals("sessValue1", connectionInfo.getSessionConfigs().get("sessKey1"));
    Assert.assertTrue(connectionInfo.getEngineConfigs().isEmpty());

    connectionInfo =
        ConnectionInfo.parse(
            "jdbc:kyuubi://user:passwd@localhost1:1203,localhost2/db_name?sessKey1=sessValue1;sessKey2=sessValue2");
    Assert.assertEquals("jdbc:kyuubi", connectionInfo.getSchema());
    Assert.assertEquals("user", connectionInfo.getUser());
    Assert.assertEquals("passwd", connectionInfo.getPassword());
    Assert.assertNotNull(connectionInfo.getHosts());
    Assert.assertEquals(2, connectionInfo.getHosts().length);
    Assert.assertEquals("localhost1", connectionInfo.getHosts()[0].getHost());
    Assert.assertNotNull(connectionInfo.getHosts()[0].getPort());
    Assert.assertEquals(1203, connectionInfo.getHosts()[0].getPort().intValue());
    Assert.assertEquals("localhost2", connectionInfo.getHosts()[1].getHost());
    Assert.assertNull(connectionInfo.getHosts()[1].getPort());
    Assert.assertEquals("db_name", connectionInfo.getDbName());
    Assert.assertNotNull(connectionInfo.getSessionConfigs());
    Assert.assertEquals(2, connectionInfo.getSessionConfigs().size());
    Assert.assertEquals("sessValue1", connectionInfo.getSessionConfigs().get("sessKey1"));
    Assert.assertEquals("sessValue2", connectionInfo.getSessionConfigs().get("sessKey2"));
    Assert.assertTrue(connectionInfo.getEngineConfigs().isEmpty());

    connectionInfo =
        ConnectionInfo.parse(
            "jdbc:kyuubi://user:passwd@localhost1:1203,localhost2/db_name?sessKey1=sessValue1;sessKey2=sessValue2#engineKey=engineValue");
    Assert.assertEquals("jdbc:kyuubi", connectionInfo.getSchema());
    Assert.assertEquals("user", connectionInfo.getUser());
    Assert.assertEquals("passwd", connectionInfo.getPassword());
    Assert.assertNotNull(connectionInfo.getHosts());
    Assert.assertEquals(2, connectionInfo.getHosts().length);
    Assert.assertEquals("localhost1", connectionInfo.getHosts()[0].getHost());
    Assert.assertNotNull(connectionInfo.getHosts()[0].getPort());
    Assert.assertEquals(1203, connectionInfo.getHosts()[0].getPort().intValue());
    Assert.assertEquals("localhost2", connectionInfo.getHosts()[1].getHost());
    Assert.assertNull(connectionInfo.getHosts()[1].getPort());
    Assert.assertEquals("db_name", connectionInfo.getDbName());
    Assert.assertNotNull(connectionInfo.getSessionConfigs());
    Assert.assertEquals(2, connectionInfo.getSessionConfigs().size());
    Assert.assertEquals("sessValue1", connectionInfo.getSessionConfigs().get("sessKey1"));
    Assert.assertEquals("sessValue2", connectionInfo.getSessionConfigs().get("sessKey2"));
    Assert.assertNotNull(connectionInfo.getEngineConfigs());
    Assert.assertEquals(1, connectionInfo.getEngineConfigs().size());
    Assert.assertEquals("engineValue", connectionInfo.getEngineConfigs().get("engineKey"));

    connectionInfo =
        ConnectionInfo.parse(
            "jdbc:kyuubi://user:passwd@localhost1:1203,localhost2/db_name?sessKey1=sessValue1;sessKey2=sessValue2#engineKey1=engineValue1;engineKey2=engineValue2");
    Assert.assertEquals("jdbc:kyuubi", connectionInfo.getSchema());
    Assert.assertEquals("user", connectionInfo.getUser());
    Assert.assertEquals("passwd", connectionInfo.getPassword());
    Assert.assertNotNull(connectionInfo.getHosts());
    Assert.assertEquals(2, connectionInfo.getHosts().length);
    Assert.assertEquals("localhost1", connectionInfo.getHosts()[0].getHost());
    Assert.assertNotNull(connectionInfo.getHosts()[0].getPort());
    Assert.assertEquals(1203, connectionInfo.getHosts()[0].getPort().intValue());
    Assert.assertEquals("localhost2", connectionInfo.getHosts()[1].getHost());
    Assert.assertNull(connectionInfo.getHosts()[1].getPort());
    Assert.assertEquals("db_name", connectionInfo.getDbName());
    Assert.assertNotNull(connectionInfo.getSessionConfigs());
    Assert.assertEquals(2, connectionInfo.getSessionConfigs().size());
    Assert.assertEquals("sessValue1", connectionInfo.getSessionConfigs().get("sessKey1"));
    Assert.assertEquals("sessValue2", connectionInfo.getSessionConfigs().get("sessKey2"));
    Assert.assertNotNull(connectionInfo.getEngineConfigs());
    Assert.assertEquals(2, connectionInfo.getEngineConfigs().size());
    Assert.assertEquals("engineValue1", connectionInfo.getEngineConfigs().get("engineKey1"));
    Assert.assertEquals("engineValue2", connectionInfo.getEngineConfigs().get("engineKey2"));
  }
}
