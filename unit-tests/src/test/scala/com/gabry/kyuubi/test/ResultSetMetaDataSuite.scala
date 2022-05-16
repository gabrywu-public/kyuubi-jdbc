package com.gabry.kyuubi.test

import com.gabry.kyuubi.jdbc.KyuubiStatement
import org.apache.kyuubi.config.KyuubiConf

import java.sql.{ResultSetMetaData, SQLFeatureNotSupportedException}

class ResultSetMetaDataSuite extends WithStatement {
  override protected val conf: KyuubiConf = KyuubiConf()
  test("result set metadata not supported") {
    withAllOfTypesTable { (statement, tableName) =>
      val resultSet = statement.executeQuery(s"select * from ${tableName}")
      val resultSetMetaData = resultSet.getMetaData

      assertThrows[SQLFeatureNotSupportedException](resultSetMetaData.getCatalogName(1))
      assertThrows[SQLFeatureNotSupportedException](resultSetMetaData.isCurrency(1))
      assertThrows[SQLFeatureNotSupportedException](resultSetMetaData.isSigned(1))
      assertThrows[SQLFeatureNotSupportedException](resultSetMetaData.getSchemaName(1))
      assertThrows[SQLFeatureNotSupportedException](resultSetMetaData.getTableName(1))
      assertThrows[SQLFeatureNotSupportedException](resultSetMetaData.isWritable(1))
      assertThrows[SQLFeatureNotSupportedException](resultSetMetaData.isDefinitelyWritable(1))
      assertThrows[SQLFeatureNotSupportedException](resultSetMetaData.isWrapperFor(classOf[KyuubiStatement]))
      assertThrows[SQLFeatureNotSupportedException](resultSetMetaData.unwrap(classOf[KyuubiStatement]))

    }
  }
  test("basic result set metadata") {
    withAllOfTypesTable { (statement, tableName) =>
      val resultSet = statement.executeQuery(s"select * from ${tableName}")
      val resultSetMetaData = resultSet.getMetaData
      assert(11 == resultSetMetaData.getColumnCount)
      1 to resultSetMetaData.getColumnCount foreach { i =>
        assert(!resultSetMetaData.isAutoIncrement(i))
        assert(resultSetMetaData.isSearchable(i))
        assert(ResultSetMetaData.columnNullable == resultSetMetaData.isNullable(i))
        assert(resultSetMetaData.isReadOnly(i))
      }
      1 to resultSetMetaData.getColumnCount foreach { i =>
        if (i == 2) {
          assert(resultSetMetaData.isCaseSensitive(i))
        } else {
          assert(!resultSetMetaData.isCaseSensitive(i))
        }
      }
      // how to define column display size?
      assert(18 == resultSetMetaData.getColumnDisplaySize(1))
      assert(Integer.MAX_VALUE == resultSetMetaData.getColumnDisplaySize(2))
      assert(4 == resultSetMetaData.getColumnDisplaySize(3))
      assert(3 == resultSetMetaData.getColumnDisplaySize(4))
      assert(5 == resultSetMetaData.getColumnDisplaySize(5))
      assert(10 == resultSetMetaData.getColumnDisplaySize(6))
      assert(19 == resultSetMetaData.getColumnDisplaySize(7))
      assert(7 == resultSetMetaData.getColumnDisplaySize(8))
      assert(15 == resultSetMetaData.getColumnDisplaySize(9))
      assert(10 == resultSetMetaData.getColumnDisplaySize(10))
      assert(29 == resultSetMetaData.getColumnDisplaySize(11))

      assert("deci" == resultSetMetaData.getColumnLabel(1))
      assert("str" == resultSetMetaData.getColumnLabel(2))
      assert("bl" == resultSetMetaData.getColumnLabel(3))
      assert("byt" == resultSetMetaData.getColumnLabel(4))
      assert("shrt" == resultSetMetaData.getColumnLabel(5))
      assert("it" == resultSetMetaData.getColumnLabel(6))
      assert("lng" == resultSetMetaData.getColumnLabel(7))
      assert("flt" == resultSetMetaData.getColumnLabel(8))
      assert("dbl" == resultSetMetaData.getColumnLabel(9))
      assert("dt" == resultSetMetaData.getColumnLabel(10))
      assert("ts" == resultSetMetaData.getColumnLabel(11))

      assert("deci" == resultSetMetaData.getColumnName(1))
      assert("str" == resultSetMetaData.getColumnName(2))
      assert("bl" == resultSetMetaData.getColumnName(3))
      assert("byt" == resultSetMetaData.getColumnName(4))
      assert("shrt" == resultSetMetaData.getColumnName(5))
      assert("it" == resultSetMetaData.getColumnName(6))
      assert("lng" == resultSetMetaData.getColumnName(7))
      assert("flt" == resultSetMetaData.getColumnName(8))
      assert("dbl" == resultSetMetaData.getColumnName(9))
      assert("dt" == resultSetMetaData.getColumnName(10))
      assert("ts" == resultSetMetaData.getColumnName(11))

      assert(18 == resultSetMetaData.getPrecision(1))
      assert(2 == resultSetMetaData.getScale(1))
    }
  }
}
