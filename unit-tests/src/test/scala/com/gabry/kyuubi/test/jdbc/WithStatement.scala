package com.gabry.kyuubi.test.jdbc

import com.gabry.kyuubi.test.base.WithJdbcDriver
import com.gabry.kyuubi.utils.Utils

import java.sql.Statement
import scala.util.Random

trait WithStatement extends WithKyuubiServer with WithJdbcDriver {
  def withAllOfTypesTable(f: (Statement, String) => Unit): Unit = withStatement {
    statement =>
      val tableName = s"table_with_all_types_${Random.nextInt(1000)}"
      statement.execute(
        s"""create table ${tableName}(
           |deci decimal(18,2),
           |str string,
           |bl boolean,
           |byt tinyint,
           |shrt smallint,
           |it int,
           |lng bigint,
           |flt float,
           |dbl double,
           |dt date,
           |ts timestamp) using csv""".stripMargin)
      f(statement, tableName)
  }

  def withStatement(f: (Statement) => Unit): Unit =
    withConnection(s"jdbc:kyuubi://${getConnectionUrl}/default") {
      connection =>
        var statement: Statement = null
        try {
          statement = connection.createStatement()
          f(statement)
        } finally {
          Utils.cleanup(statement)
        }
    }
}
