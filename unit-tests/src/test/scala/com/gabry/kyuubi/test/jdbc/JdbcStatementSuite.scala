package com.gabry.kyuubi.test.jdbc

import com.gabry.kyuubi.jdbc.KyuubiStatement
import com.gabry.kyuubi.test.base.WithJdbcDriver
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

      assertThrows[SQLFeatureNotSupportedException](statement.execute("", 1))
      assertThrows[SQLFeatureNotSupportedException](statement.execute("", new Array[Int](0)))
      assertThrows[SQLFeatureNotSupportedException](statement.execute("", new Array[String](0)))
      assertThrows[SQLFeatureNotSupportedException](statement.unwrap(classOf[KyuubiStatement]))
      assertThrows[SQLFeatureNotSupportedException](statement.executeUpdate("", 1))
      assertThrows[SQLFeatureNotSupportedException](statement.executeUpdate("", new Array[Int](0)))
      assertThrows[SQLFeatureNotSupportedException](statement.executeUpdate("", new Array[String](0)))
      assertThrows[SQLFeatureNotSupportedException](statement.getMaxFieldSize)
      assertThrows[SQLFeatureNotSupportedException](statement.setMaxFieldSize(10))
      assertThrows[SQLFeatureNotSupportedException](statement.setEscapeProcessing(true))
      assertThrows[SQLFeatureNotSupportedException](statement.addBatch(""))
      assertThrows[SQLFeatureNotSupportedException](statement.getWarnings)
      assertThrows[SQLFeatureNotSupportedException](statement.clearWarnings)
      assertThrows[SQLFeatureNotSupportedException](statement.setCursorName(""))
      assertThrows[SQLFeatureNotSupportedException](statement.getUpdateCount)
      assertThrows[SQLFeatureNotSupportedException](statement.getMoreResults)
      assertThrows[SQLFeatureNotSupportedException](statement.getMoreResults(10))
      assertThrows[SQLFeatureNotSupportedException](statement.setFetchDirection(1))
      assertThrows[SQLFeatureNotSupportedException](statement.getResultSetConcurrency)
      assertThrows[SQLFeatureNotSupportedException](statement.getResultSetType)
      assertThrows[SQLFeatureNotSupportedException](statement.clearBatch)
      assertThrows[SQLFeatureNotSupportedException](statement.executeBatch)
      assertThrows[SQLFeatureNotSupportedException](statement.getGeneratedKeys)
      assertThrows[SQLFeatureNotSupportedException](statement.getResultSetHoldability)
      assertThrows[SQLFeatureNotSupportedException](statement.setPoolable(true))
      assertThrows[SQLFeatureNotSupportedException](statement.closeOnCompletion)
      assertThrows[SQLFeatureNotSupportedException](statement.getLargeUpdateCount)
      assertThrows[SQLFeatureNotSupportedException](statement.setLargeMaxRows(1))
      assertThrows[SQLFeatureNotSupportedException](statement.getLargeMaxRows)
      assertThrows[SQLFeatureNotSupportedException](statement.executeLargeBatch)
      assertThrows[SQLFeatureNotSupportedException](statement.executeLargeUpdate(""))
      assertThrows[SQLFeatureNotSupportedException](statement.executeLargeUpdate("", 1))
      assertThrows[SQLFeatureNotSupportedException](statement.executeLargeUpdate("", new Array[Int](0)))
      assertThrows[SQLFeatureNotSupportedException](statement.executeLargeUpdate("", new Array[String](0)))
      assertThrows[SQLFeatureNotSupportedException](statement.isWrapperFor(classOf[KyuubiStatement]))

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
