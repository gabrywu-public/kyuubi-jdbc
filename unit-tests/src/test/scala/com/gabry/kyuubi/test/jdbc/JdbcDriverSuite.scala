package com.gabry.kyuubi.test.jdbc

import com.gabry.kyuubi.driver.KyuubiDriver
import com.gabry.kyuubi.utils.Commons
import org.scalatest.funsuite.AnyFunSuite

import java.sql.{DriverManager, SQLException, SQLFeatureNotSupportedException}

class JdbcDriverSuite extends AnyFunSuite {

  test("accept url") {
    val kyuubiDriver = new KyuubiDriver
    assert(kyuubiDriver.acceptsURL(Commons.JDBC_URL_PREFIX))
    assert(kyuubiDriver.acceptsURL(Commons.JDBC_URL_PREFIX + "localhost:port"))
    assert(!kyuubiDriver.acceptsURL("localhost:port" + Commons.JDBC_URL_PREFIX))
  }

  test("connect invalid jdbc") {
    assertThrows[SQLException](DriverManager.getConnection("jdbc:hive2://"))
  }
  test("test all of the supported & unsupported methods") {
    val driver = new KyuubiDriver
    assertThrows[SQLFeatureNotSupportedException](driver.getParentLogger)
    assert(!driver.jdbcCompliant())
  }
}
