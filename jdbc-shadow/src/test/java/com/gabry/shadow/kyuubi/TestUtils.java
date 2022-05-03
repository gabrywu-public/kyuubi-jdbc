package com.gabry.shadow.kyuubi;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;

public class TestUtils {
  private TestUtils() {
    // do nothing
  }

  public static void printResultSet(ResultSet resultSet) throws SQLException {
    ResultSetMetaData metaData = resultSet.getMetaData();
    for (int i = 1; i <= metaData.getColumnCount() - 1; i++) {
      System.out.print(metaData.getColumnName(i) + "(" + metaData.getColumnTypeName(i) + ")\t");
    }
    System.out.println(
        metaData.getColumnName(metaData.getColumnCount())
            + "("
            + metaData.getColumnTypeName(metaData.getColumnCount())
            + ")");
    while (resultSet.next()) {
      for (int i = 1; i <= metaData.getColumnCount() - 1; i++) {
        System.out.print(resultSet.getObject(i) + "\t");
      }
      System.out.println(resultSet.getObject(metaData.getColumnCount()));
    }
  }
}
