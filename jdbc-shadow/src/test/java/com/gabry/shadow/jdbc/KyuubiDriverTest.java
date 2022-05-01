package com.gabry.shadow.jdbc;

import com.gabry.shadow.jdbc.Utils.DriverCommon;
import com.gabry.shadow.jdbc.driver.JdbcUrl;
import com.gabry.shadow.jdbc.driver.KyuubiDriver;
import com.gabry.shadow.jdbc.driver.exception.JdbcUrlParseException;
import org.junit.Assert;
import org.junit.Test;

import java.sql.SQLException;

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
    Assert.assertThrows(JdbcUrlParseException.class, () -> JdbcUrl.parse("jdbc:kyuubis//"));
    Assert.assertThrows(JdbcUrlParseException.class, () -> JdbcUrl.parse("jdbc:hive2//"));
    Assert.assertThrows(JdbcUrlParseException.class, () -> JdbcUrl.parse("jdbc:hive//"));
    Assert.assertThrows(
        JdbcUrlParseException.class, () -> JdbcUrl.parse("jdbc:kyuubi//user:password"));
    Assert.assertThrows(
        JdbcUrlParseException.class, () -> JdbcUrl.parse("jdbc:kyuubi//user:password@"));
    Assert.assertThrows(JdbcUrlParseException.class, () -> JdbcUrl.parse("jdbc:kyuubi//host:port"));
    Assert.assertThrows(
        JdbcUrlParseException.class, () -> JdbcUrl.parse("jdbc:kyuubi//host:123,host:sss"));
    JdbcUrl jdbcUrl = JdbcUrl.parse("jdbc:kyuubi//");
    Assert.assertEquals("jdbc:kyuubi", jdbcUrl.getSchema());
    Assert.assertNull(jdbcUrl.getUser());
    Assert.assertNull(jdbcUrl.getPassword());
    Assert.assertNull(jdbcUrl.getHosts());
    Assert.assertNull(jdbcUrl.getDbName());
    Assert.assertTrue(jdbcUrl.getSessionConfigs().isEmpty());
    Assert.assertTrue(jdbcUrl.getEngineConfigs().isEmpty());
  }

  @Test
  public void testJdbcUrl() throws JdbcUrlParseException {
    JdbcUrl jdbcUrl = JdbcUrl.parse("jdbc:kyuubi//localhost:1203");
    Assert.assertEquals("jdbc:kyuubi", jdbcUrl.getSchema());
    Assert.assertNull(jdbcUrl.getUser());
    Assert.assertNull(jdbcUrl.getPassword());
    Assert.assertNotNull(jdbcUrl.getHosts());
    Assert.assertEquals(1, jdbcUrl.getHosts().length);
    Assert.assertEquals("localhost", jdbcUrl.getHosts()[0].getHost());
    Assert.assertNotNull(jdbcUrl.getHosts()[0].getPort());
    Assert.assertEquals(1203, jdbcUrl.getHosts()[0].getPort().intValue());
    Assert.assertNull(jdbcUrl.getDbName());
    Assert.assertTrue(jdbcUrl.getSessionConfigs().isEmpty());
    Assert.assertTrue(jdbcUrl.getEngineConfigs().isEmpty());

    jdbcUrl = JdbcUrl.parse("jdbc:kyuubi//localhost1:1203,localhost2");
    Assert.assertEquals("jdbc:kyuubi", jdbcUrl.getSchema());
    Assert.assertNull(jdbcUrl.getUser());
    Assert.assertNull(jdbcUrl.getPassword());
    Assert.assertNotNull(jdbcUrl.getHosts());
    Assert.assertEquals(2, jdbcUrl.getHosts().length);
    Assert.assertEquals("localhost1", jdbcUrl.getHosts()[0].getHost());
    Assert.assertNotNull(jdbcUrl.getHosts()[0].getPort());
    Assert.assertEquals(1203, jdbcUrl.getHosts()[0].getPort().intValue());
    Assert.assertEquals("localhost2", jdbcUrl.getHosts()[1].getHost());
    Assert.assertNull(jdbcUrl.getHosts()[1].getPort());
    Assert.assertNull(jdbcUrl.getDbName());
    Assert.assertTrue(jdbcUrl.getSessionConfigs().isEmpty());
    Assert.assertTrue(jdbcUrl.getEngineConfigs().isEmpty());

    jdbcUrl = JdbcUrl.parse("jdbc:kyuubi//user@localhost1:1203,localhost2");
    Assert.assertEquals("jdbc:kyuubi", jdbcUrl.getSchema());
    Assert.assertEquals("user", jdbcUrl.getUser());
    Assert.assertNull(jdbcUrl.getPassword());
    Assert.assertNotNull(jdbcUrl.getHosts());
    Assert.assertEquals(2, jdbcUrl.getHosts().length);
    Assert.assertEquals("localhost1", jdbcUrl.getHosts()[0].getHost());
    Assert.assertNotNull(jdbcUrl.getHosts()[0].getPort());
    Assert.assertEquals(1203, jdbcUrl.getHosts()[0].getPort().intValue());
    Assert.assertEquals("localhost2", jdbcUrl.getHosts()[1].getHost());
    Assert.assertNull(jdbcUrl.getHosts()[1].getPort());
    Assert.assertNull(jdbcUrl.getDbName());
    Assert.assertTrue(jdbcUrl.getSessionConfigs().isEmpty());
    Assert.assertTrue(jdbcUrl.getEngineConfigs().isEmpty());

    jdbcUrl = JdbcUrl.parse("jdbc:kyuubi//user:passwd@localhost1:1203,localhost2");
    Assert.assertEquals("jdbc:kyuubi", jdbcUrl.getSchema());
    Assert.assertEquals("user", jdbcUrl.getUser());
    Assert.assertEquals("passwd", jdbcUrl.getPassword());
    Assert.assertNotNull(jdbcUrl.getHosts());
    Assert.assertEquals(2, jdbcUrl.getHosts().length);
    Assert.assertEquals("localhost1", jdbcUrl.getHosts()[0].getHost());
    Assert.assertNotNull(jdbcUrl.getHosts()[0].getPort());
    Assert.assertEquals(1203, jdbcUrl.getHosts()[0].getPort().intValue());
    Assert.assertEquals("localhost2", jdbcUrl.getHosts()[1].getHost());
    Assert.assertNull(jdbcUrl.getHosts()[1].getPort());
    Assert.assertNull(jdbcUrl.getDbName());
    Assert.assertTrue(jdbcUrl.getSessionConfigs().isEmpty());
    Assert.assertTrue(jdbcUrl.getEngineConfigs().isEmpty());

    jdbcUrl = JdbcUrl.parse("jdbc:kyuubi//user:passwd@localhost1:1203,localhost2/db_name");
    Assert.assertEquals("jdbc:kyuubi", jdbcUrl.getSchema());
    Assert.assertEquals("user", jdbcUrl.getUser());
    Assert.assertEquals("passwd", jdbcUrl.getPassword());
    Assert.assertNotNull(jdbcUrl.getHosts());
    Assert.assertEquals(2, jdbcUrl.getHosts().length);
    Assert.assertEquals("localhost1", jdbcUrl.getHosts()[0].getHost());
    Assert.assertNotNull(jdbcUrl.getHosts()[0].getPort());
    Assert.assertEquals(1203, jdbcUrl.getHosts()[0].getPort().intValue());
    Assert.assertEquals("localhost2", jdbcUrl.getHosts()[1].getHost());
    Assert.assertNull(jdbcUrl.getHosts()[1].getPort());
    Assert.assertEquals("db_name", jdbcUrl.getDbName());
    Assert.assertTrue(jdbcUrl.getSessionConfigs().isEmpty());
    Assert.assertTrue(jdbcUrl.getEngineConfigs().isEmpty());

    jdbcUrl =
        JdbcUrl.parse(
            "jdbc:kyuubi//user:passwd@localhost1:1203,localhost2/db_name?sessKey1=sessValue1");
    Assert.assertEquals("jdbc:kyuubi", jdbcUrl.getSchema());
    Assert.assertEquals("user", jdbcUrl.getUser());
    Assert.assertEquals("passwd", jdbcUrl.getPassword());
    Assert.assertNotNull(jdbcUrl.getHosts());
    Assert.assertEquals(2, jdbcUrl.getHosts().length);
    Assert.assertEquals("localhost1", jdbcUrl.getHosts()[0].getHost());
    Assert.assertNotNull(jdbcUrl.getHosts()[0].getPort());
    Assert.assertEquals(1203, jdbcUrl.getHosts()[0].getPort().intValue());
    Assert.assertEquals("localhost2", jdbcUrl.getHosts()[1].getHost());
    Assert.assertNull(jdbcUrl.getHosts()[1].getPort());
    Assert.assertEquals("db_name", jdbcUrl.getDbName());
    Assert.assertNotNull(jdbcUrl.getSessionConfigs());
    Assert.assertEquals(1, jdbcUrl.getSessionConfigs().size());
    Assert.assertEquals("sessValue1", jdbcUrl.getSessionConfigs().get("sessKey1"));
    Assert.assertTrue(jdbcUrl.getEngineConfigs().isEmpty());

    jdbcUrl =
        JdbcUrl.parse(
            "jdbc:kyuubi//user:passwd@localhost1:1203,localhost2/db_name?sessKey1=sessValue1;sessKey2=sessValue2");
    Assert.assertEquals("jdbc:kyuubi", jdbcUrl.getSchema());
    Assert.assertEquals("user", jdbcUrl.getUser());
    Assert.assertEquals("passwd", jdbcUrl.getPassword());
    Assert.assertNotNull(jdbcUrl.getHosts());
    Assert.assertEquals(2, jdbcUrl.getHosts().length);
    Assert.assertEquals("localhost1", jdbcUrl.getHosts()[0].getHost());
    Assert.assertNotNull(jdbcUrl.getHosts()[0].getPort());
    Assert.assertEquals(1203, jdbcUrl.getHosts()[0].getPort().intValue());
    Assert.assertEquals("localhost2", jdbcUrl.getHosts()[1].getHost());
    Assert.assertNull(jdbcUrl.getHosts()[1].getPort());
    Assert.assertEquals("db_name", jdbcUrl.getDbName());
    Assert.assertNotNull(jdbcUrl.getSessionConfigs());
    Assert.assertEquals(2, jdbcUrl.getSessionConfigs().size());
    Assert.assertEquals("sessValue1", jdbcUrl.getSessionConfigs().get("sessKey1"));
    Assert.assertEquals("sessValue2", jdbcUrl.getSessionConfigs().get("sessKey2"));
    Assert.assertTrue(jdbcUrl.getEngineConfigs().isEmpty());

    jdbcUrl =
        JdbcUrl.parse(
            "jdbc:kyuubi//user:passwd@localhost1:1203,localhost2/db_name?sessKey1=sessValue1;sessKey2=sessValue2#engineKey=engineValue");
    Assert.assertEquals("jdbc:kyuubi", jdbcUrl.getSchema());
    Assert.assertEquals("user", jdbcUrl.getUser());
    Assert.assertEquals("passwd", jdbcUrl.getPassword());
    Assert.assertNotNull(jdbcUrl.getHosts());
    Assert.assertEquals(2, jdbcUrl.getHosts().length);
    Assert.assertEquals("localhost1", jdbcUrl.getHosts()[0].getHost());
    Assert.assertNotNull(jdbcUrl.getHosts()[0].getPort());
    Assert.assertEquals(1203, jdbcUrl.getHosts()[0].getPort().intValue());
    Assert.assertEquals("localhost2", jdbcUrl.getHosts()[1].getHost());
    Assert.assertNull(jdbcUrl.getHosts()[1].getPort());
    Assert.assertEquals("db_name", jdbcUrl.getDbName());
    Assert.assertNotNull(jdbcUrl.getSessionConfigs());
    Assert.assertEquals(2, jdbcUrl.getSessionConfigs().size());
    Assert.assertEquals("sessValue1", jdbcUrl.getSessionConfigs().get("sessKey1"));
    Assert.assertEquals("sessValue2", jdbcUrl.getSessionConfigs().get("sessKey2"));
    Assert.assertNotNull(jdbcUrl.getEngineConfigs());
    Assert.assertEquals(1, jdbcUrl.getEngineConfigs().size());
    Assert.assertEquals("engineValue", jdbcUrl.getEngineConfigs().get("engineKey"));

    jdbcUrl =
        JdbcUrl.parse(
            "jdbc:kyuubi//user:passwd@localhost1:1203,localhost2/db_name?sessKey1=sessValue1;sessKey2=sessValue2#engineKey1=engineValue1;engineKey2=engineValue2");
    Assert.assertEquals("jdbc:kyuubi", jdbcUrl.getSchema());
    Assert.assertEquals("user", jdbcUrl.getUser());
    Assert.assertEquals("passwd", jdbcUrl.getPassword());
    Assert.assertNotNull(jdbcUrl.getHosts());
    Assert.assertEquals(2, jdbcUrl.getHosts().length);
    Assert.assertEquals("localhost1", jdbcUrl.getHosts()[0].getHost());
    Assert.assertNotNull(jdbcUrl.getHosts()[0].getPort());
    Assert.assertEquals(1203, jdbcUrl.getHosts()[0].getPort().intValue());
    Assert.assertEquals("localhost2", jdbcUrl.getHosts()[1].getHost());
    Assert.assertNull(jdbcUrl.getHosts()[1].getPort());
    Assert.assertEquals("db_name", jdbcUrl.getDbName());
    Assert.assertNotNull(jdbcUrl.getSessionConfigs());
    Assert.assertEquals(2, jdbcUrl.getSessionConfigs().size());
    Assert.assertEquals("sessValue1", jdbcUrl.getSessionConfigs().get("sessKey1"));
    Assert.assertEquals("sessValue2", jdbcUrl.getSessionConfigs().get("sessKey2"));
    Assert.assertNotNull(jdbcUrl.getEngineConfigs());
    Assert.assertEquals(2, jdbcUrl.getEngineConfigs().size());
    Assert.assertEquals("engineValue1", jdbcUrl.getEngineConfigs().get("engineKey1"));
    Assert.assertEquals("engineValue2", jdbcUrl.getEngineConfigs().get("engineKey2"));
  }
}
