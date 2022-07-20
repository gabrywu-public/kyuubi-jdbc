package com.gabry.kyuubi.test.base

import com.gabry.kyuubi.driver.KyuubiDriver
import com.gabry.kyuubi.utils.Utils

import java.sql.Connection

trait WithJdbcDriver {
  val kyuubiDriver = new KyuubiDriver

  def withConnection(url: String)(f: (Connection) => Unit): Unit = {
    var connection: Connection = null
    try {
      connection = kyuubiDriver.connect(url, null)
      f(connection)
    } finally {
      Utils.cleanup(connection)
    }
  }
}
