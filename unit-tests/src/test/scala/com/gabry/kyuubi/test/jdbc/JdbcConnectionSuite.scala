package com.gabry.kyuubi.test.jdbc

import com.gabry.kyuubi.driver.ConnectionInfo
import com.gabry.kyuubi.jdbc.KyuubiConnection
import com.gabry.kyuubi.test.base.WithJdbcDriver
import org.apache.kyuubi.config.KyuubiConf

class JdbcConnectionSuite extends WithKyuubiServer with WithJdbcDriver {
  override protected val conf: KyuubiConf = KyuubiConf()
  test("basic connect") {
    withConnection(s"jdbc:kyuubi://${getConnectionUrl}/default") {
      connection =>
        assert(connection.isInstanceOf[KyuubiConnection])
        assert(!connection.isClosed)
    }
  }
  test("testDatabaseMetadata") {
    withConnection(s"jdbc:kyuubi://${getConnectionUrl}/default") { connection =>
      val databaseMetaData = connection.getMetaData
      assert(null != databaseMetaData)
      val resultSet = databaseMetaData.getCatalogs
      assert(null != resultSet)
      assert(!resultSet.isClosed)
      assert(resultSet.next)
      assert("spark_catalog" == resultSet.getString(1))
      assert(!resultSet.next)
      resultSet.close()
    }
  }
  test("test open") {
    val connectionInfo = ConnectionInfo.parse(s"jdbc:kyuubi://${getConnectionUrl}/default")
    val connection = new KyuubiConnection(connectionInfo)
    assert(connection.isClosed)
    connection.open()
    assert(!connection.isClosed)
  }
}
