package com.gabry.shadow.kyuubi;

import com.gabry.shadow.kyuubi.driver.KyuubiDriver;
import com.gabry.shadow.kyuubi.jdbc.KyuubiConnection;
import org.junit.Assert;
import org.junit.Test;

import java.sql.*;

public class KyuubiStatementTest {
  @Test
  public void testStatement() throws SQLException {
    KyuubiDriver kyuubiDriver = new KyuubiDriver();
    try (Connection connection =
            kyuubiDriver.connect("jdbc:kyuubi://localhost:10009/default", null);
        Statement statement = connection.createStatement();
        ResultSet resultSet = statement.executeQuery("select 1 as id,'str' name")) {
      Assert.assertFalse(connection.isClosed());
      Assert.assertNotNull(resultSet);
      Assert.assertTrue(resultSet.next());
      Assert.assertEquals(1, resultSet.getInt("id"));
      Assert.assertEquals("str", resultSet.getString("name"));
      ResultSetMetaData metaData = resultSet.getMetaData();
      Assert.assertNotNull(metaData);
      Assert.assertEquals(2, metaData.getColumnCount());
      Assert.assertEquals("id", metaData.getColumnName(1));
      Assert.assertEquals("name", metaData.getColumnName(2));
      Assert.assertEquals(JDBCType.INTEGER.getName(), metaData.getColumnTypeName(1));
      Assert.assertEquals(JDBCType.VARCHAR.getName(), metaData.getColumnTypeName(2));
    }
  }

  @Test
  public void testChar() throws SQLException {
    KyuubiDriver kyuubiDriver = new KyuubiDriver();
    try (KyuubiConnection connection =
            (KyuubiConnection) kyuubiDriver.connect("jdbc:kyuubi://localhost:10009/default", null);
        Statement statement = connection.createStatement();
        ResultSet resultSet = statement.executeQuery("select 12 as id,'?? ' name")) {
      TestUtils.printResultSet(resultSet);
    }
  }
}
