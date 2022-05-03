package com.gabry.shadow.kyuubi;

import com.gabry.shadow.kyuubi.driver.KyuubiDriver;
import org.junit.Assert;
import org.junit.Test;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

public class KyuubiStatementTest {
  @Test
  public void testStatement() throws SQLException {
    KyuubiDriver kyuubiDriver = new KyuubiDriver();
    try (Connection connection =
            kyuubiDriver.connect("jdbc:kyuubi://192.168.3.3:10009/default", null);
        Statement statement = connection.createStatement();
        ResultSet resultSet = statement.executeQuery("select 1 as id,'str' name")) {
      Assert.assertFalse(connection.isClosed());
      Assert.assertNotNull(resultSet);
      Assert.assertTrue(resultSet.next());
      Assert.assertEquals(1, resultSet.getInt("id"));
      Assert.assertEquals("str", resultSet.getString("name"));
    }
  }
}
