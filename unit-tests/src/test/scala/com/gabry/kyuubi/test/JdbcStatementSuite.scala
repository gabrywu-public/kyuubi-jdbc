package com.gabry.kyuubi.test

import org.apache.kyuubi.config.KyuubiConf

import java.sql.JDBCType

class JdbcStatementSuite extends WithKyuubiServer with WithJdbcDriver {
  override protected val conf: KyuubiConf = KyuubiConf()
  test("basic statement") {
    withConnection(s"jdbc:kyuubi://${getConnectionUrl}/default") { connection =>
      val statement = connection.createStatement
      val resultSet = statement.executeQuery("select 1 as id,'str' name")
      assert(!connection.isClosed)
      assert(null != resultSet)
      assert(resultSet.next)
      assert(1 == resultSet.getInt("id"))
      assert("str" == resultSet.getString("name"))
      val metaData = resultSet.getMetaData
      assert(null != metaData)
      assert(2 == metaData.getColumnCount)
      assert("id" == metaData.getColumnName(1))
      assert("name" == metaData.getColumnName(2))
      assert(JDBCType.INTEGER.getName == metaData.getColumnTypeName(1))
      assert(JDBCType.VARCHAR.getName == metaData.getColumnTypeName(2))
    }
  }
}
