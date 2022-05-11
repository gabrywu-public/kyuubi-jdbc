package com.gabry.kyuubi.test

import com.gabry.kyuubi.driver.KyuubiDriver
import com.gabry.kyuubi.utils.DriverCommon
import org.scalatest.funsuite.AnyFunSuite

import java.sql.{DriverManager, SQLException, SQLFeatureNotSupportedException}

class JdbcDriverSuite extends AnyFunSuite {

  test("accept url") {
    val kyuubiDriver = new KyuubiDriver
    assert(kyuubiDriver.acceptsURL(DriverCommon.JDBC_URL_PREFIX))
    assert(kyuubiDriver.acceptsURL(DriverCommon.JDBC_URL_PREFIX + "localhost:port"))
    assert(!kyuubiDriver.acceptsURL("localhost:port" + DriverCommon.JDBC_URL_PREFIX))
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
