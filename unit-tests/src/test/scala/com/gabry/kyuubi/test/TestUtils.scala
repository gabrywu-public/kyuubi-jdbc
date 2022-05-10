package com.gabry.kyuubi.test

import java.sql.ResultSet

object TestUtils {
  def printResult(resultSet: ResultSet): Unit = {
    val metadata = resultSet.getMetaData
    val columnTypes = 1 to metadata.getColumnCount map (i => s"${metadata.getColumnName(i)}(${metadata.getColumnTypeName(i)})")
    println(columnTypes.mkString("\r"))
    while (resultSet.next()) {
      val columnValues = 1 to metadata.getColumnCount map (i => resultSet.getObject(i))
      println(columnValues.mkString("\t"))
    }
  }
}
