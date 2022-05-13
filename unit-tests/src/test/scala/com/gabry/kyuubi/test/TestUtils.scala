package com.gabry.kyuubi.test

import org.slf4j.LoggerFactory

import java.sql.ResultSet

object TestUtils {
  private val logger = LoggerFactory.getLogger(TestUtils.getClass)
  def printResult(resultSet: ResultSet): Unit = {
    val metadata = resultSet.getMetaData
    val columnTypes = 1 to metadata.getColumnCount map (i => s"${metadata.getColumnName(i)}(${metadata.getColumnTypeName(i)})")
    logger.warn(columnTypes.mkString("\r"))
    while (resultSet.next()) {
      val columnValues = 1 to metadata.getColumnCount map (i => resultSet.getObject(i))
      logger.warn(columnValues.mkString("\t"))
    }
  }
}
