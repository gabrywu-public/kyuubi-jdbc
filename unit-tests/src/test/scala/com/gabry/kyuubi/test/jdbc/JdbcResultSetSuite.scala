package com.gabry.kyuubi.test.jdbc

import com.gabry.kyuubi.jdbc.KyuubiStatement
import com.gabry.kyuubi.test.base.WithJdbcDriver
import org.apache.kyuubi.config.KyuubiConf

import java.sql.{Date, JDBCType, ResultSet, Timestamp}
import java.text.SimpleDateFormat

class JdbcResultSetSuite extends WithKyuubiServer with WithJdbcDriver {
  override protected val conf: KyuubiConf = KyuubiConf()
  test("basic result set") {
    withConnection(s"jdbc:kyuubi://${getConnectionUrl}/default") { connection =>
      val statement = connection.createStatement
      val resultSet = statement.executeQuery("select 1 as id,'str' name")
      assert(!connection.isClosed)
      assert(null != resultSet)
      assert(0 == resultSet.getRow)
      assert(ResultSet.FETCH_FORWARD == resultSet.getFetchDirection)
      assert(ResultSet.TYPE_FORWARD_ONLY == resultSet.getType)
      assert(ResultSet.CONCUR_READ_ONLY == resultSet.getConcurrency)
      assert(KyuubiStatement.DEFAULT_FETCH_SIZE == resultSet.getFetchSize)

      assert(resultSet.next)
      assert(1 == resultSet.getRow)
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
  test("all of data types") {
    withConnection(s"jdbc:kyuubi://${getConnectionUrl}/default") { connection =>
      val statement = connection.createStatement
      statement.execute(
        """create table table_with_all_types(
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
      val date = new Date(System.currentTimeMillis())
      val ts = new Timestamp(System.currentTimeMillis())

      val insert =
        s"""insert into table table_with_all_types
           |select 12345,'string content', true,1,23,456,789,
           |1.123,2.345,
           |to_date('${new SimpleDateFormat("yyyy-MM-dd").format(date)}','yyyy-MM-dd') ,
           |to_timestamp('${new SimpleDateFormat("yyy-MM-dd HH:mm:ss.SSS").format(ts)}','yyy-MM-dd HH:mm:ss.SSS')"""
          .stripMargin
      println(s"insert ${insert}")
      statement.execute(insert)
      val result = statement.executeQuery("select * from table_with_all_types")
      assert(result.next())

      assert(0 == BigDecimal("12345").compareTo(result.getBigDecimal(1)))
      assert("string content" == result.getString(2))
      assert(result.getBoolean(3))
      assert(1.asInstanceOf[Byte] == result.getByte(4))
      assert(23.asInstanceOf[Short] == result.getShort(5))
      assert(456 == result.getInt(6))
      assert(789 == result.getLong(7))
      assert("1.123" == String.valueOf(result.getFloat(8)))
      assert("2.345" == String.valueOf(result.getDouble(9)))
      assert(new SimpleDateFormat("yyyy-MM-dd").format(date) ==
        new SimpleDateFormat("yyyy-MM-dd").format(result.getDate(10)))
      assert(new SimpleDateFormat("yyy-MM-dd HH:mm:ss.SSS").format(ts) ==
        new SimpleDateFormat("yyy-MM-dd HH:mm:ss.SSS").format(result.getTimestamp(11)))

      assert(0 == BigDecimal("12345").compareTo(result.getBigDecimal("deci")))
      assert("string content" == result.getString("str"))
      assert(result.getBoolean("bl"))
      assert(1.asInstanceOf[Byte] == result.getByte("byt"))
      assert(23.asInstanceOf[Short] == result.getShort("shrt"))
      assert(456 == result.getInt("it"))
      assert(789 == result.getLong("lng"))
      assert("1.123" == String.valueOf(result.getFloat("flt")))
      assert("2.345" == String.valueOf(result.getDouble("dbl")))
      assert(new SimpleDateFormat("yyyy-MM-dd").format(date) ==
        new SimpleDateFormat("yyyy-MM-dd").format(result.getDate("dt")))
      assert(new SimpleDateFormat("yyy-MM-dd HH:mm:ss.SSS").format(ts) ==
        new SimpleDateFormat("yyy-MM-dd HH:mm:ss.SSS").format(result.getTimestamp("ts")))

      statement.execute("drop table if exists table_with_all_types")
    }
  }
}
