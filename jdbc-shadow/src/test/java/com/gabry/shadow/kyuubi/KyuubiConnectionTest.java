package com.gabry.shadow.kyuubi;

import com.gabry.shadow.kyuubi.driver.KyuubiDriver;
import com.gabry.shadow.kyuubi.jdbc.KyuubiConnection;
import org.junit.Assert;
import org.junit.Test;

import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;

public class KyuubiConnectionTest {
  @Test
  public void testConnect() throws SQLException {
    KyuubiDriver kyuubiDriver = new KyuubiDriver();
    try (KyuubiConnection connection =
        (KyuubiConnection) kyuubiDriver.connect("jdbc:kyuubi://localhost:10009/default", null)) {
      Assert.assertFalse(connection.isClosed());
    }
  }

  @Test
  public void testDatabaseMetadata() throws SQLException {
    KyuubiDriver kyuubiDriver = new KyuubiDriver();
    try (KyuubiConnection connection =
        (KyuubiConnection) kyuubiDriver.connect("jdbc:kyuubi://localhost:10009/default", null)) {
      DatabaseMetaData databaseMetaData = connection.getMetaData();
      try (ResultSet resultSet = databaseMetaData.getCatalogs()) {
        Assert.assertTrue(resultSet.next());
        Assert.assertEquals("spark_catalog", resultSet.getString(1));
        Assert.assertFalse(resultSet.next());
      }
    }
  }
}
