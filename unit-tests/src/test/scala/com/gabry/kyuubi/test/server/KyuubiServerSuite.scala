package com.gabry.kyuubi.test.server

import com.gabry.kyuubi.jdbc.KyuubiConnection
import com.gabry.kyuubi.test.base.WithJdbcDriver

class KyuubiServerSuite extends WithKyuubiServer with WithJdbcDriver {
  test("basic connect") {
    withConnection(s"jdbc:kyuubi://${getConnectionUrl}/default") {
      connection =>
        assert(connection.isInstanceOf[KyuubiConnection])
        assert(!connection.isClosed)
    }
  }
}
