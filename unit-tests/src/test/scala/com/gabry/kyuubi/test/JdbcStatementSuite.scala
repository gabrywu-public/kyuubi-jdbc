package com.gabry.kyuubi.test

import com.gabry.kyuubi.jdbc.KyuubiStatement
import org.apache.kyuubi.config.KyuubiConf

import java.sql.{JDBCType, SQLException, SQLFeatureNotSupportedException}
import scala.util.Random

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
  test("fetch size") {
    withConnection(s"jdbc:kyuubi://${getConnectionUrl}/default") { connection =>
      val statement = connection.createStatement
      assert(KyuubiStatement.DEFAULT_FETCH_SIZE == statement.getFetchSize)
      val fetchSize = Random.nextInt().abs
      statement.setFetchSize(fetchSize)
      assert(fetchSize == statement.getFetchSize)
      statement.setFetchSize(0)
      assert(KyuubiStatement.DEFAULT_FETCH_SIZE == statement.getFetchSize)
      assertThrows[SQLException](statement.setFetchDirection(-2))
      statement.close()
    }
  }
  test("unsupported method") {
    withConnection(s"jdbc:kyuubi://${getConnectionUrl}/default") { connection =>
      val statement = connection.createStatement
      assertThrows[SQLFeatureNotSupportedException] {
        statement.execute("", 1)
        statement.execute("", new Array[Int](0))
        statement.execute("", new Array[String](0))
        statement.unwrap(classOf[KyuubiStatement])
        statement.executeUpdate("", 1)
        statement.executeUpdate("", new Array[Int](0))
        statement.executeUpdate("", new Array[String](0))
        statement.getMaxFieldSize
        statement.setMaxFieldSize(10)
        statement.setEscapeProcessing(true)
        statement.addBatch("")
        statement.getWarnings
        statement.clearWarnings
        statement.setCursorName("")
        statement.getUpdateCount
        statement.getMoreResults
        statement.getMoreResults(10)
        statement.setFetchDirection(1)
        statement.getResultSetConcurrency
        statement.getResultSetType
        statement.clearBatch
        statement.executeBatch
        statement.getGeneratedKeys
        statement.getResultSetHoldability
        statement.setPoolable(true)
        statement.closeOnCompletion
        statement.getLargeUpdateCount
        statement.setLargeMaxRows(1)
        statement.getLargeMaxRows
        statement.executeLargeBatch
        statement.executeLargeUpdate("")
        statement.executeLargeUpdate("", 1)
        statement.executeLargeUpdate("", new Array[Int](0))
        statement.executeLargeUpdate("", new Array[String](0))
        statement.isWrapperFor(classOf[KyuubiStatement])
      }
      statement.close()
    }
  }
  test("scala statement") {
    withConnection(s"jdbc:kyuubi://${getConnectionUrl}/default") { connection =>
      val statement = connection.createStatement.asInstanceOf[KyuubiStatement]
      val result = statement.executeScala("val a = 123")
      assert(result != null)
      assert(result.next())
      assert("a: Int = 123" == result.getString(1))
      assert("a: Int = 123" == result.getString("output"))
      statement.close()
    }
  }
}
