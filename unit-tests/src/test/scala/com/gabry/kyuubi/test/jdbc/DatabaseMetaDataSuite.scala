package com.gabry.kyuubi.test.jdbc

import com.gabry.kyuubi.test.base.WithJdbcDriver
import org.apache.kyuubi.config.KyuubiConf

import java.sql.ResultSet

class DatabaseMetaDataSuite extends WithKyuubiServer with WithJdbcDriver {
  override protected val conf: KyuubiConf = KyuubiConf()
  test("basic database metadata") {
    withConnection(s"jdbc:kyuubi://${getConnectionUrl}/default") {
      connection =>
        val metadata = connection.getMetaData
        assert("Apache Kyuubi (Incubating)" == metadata.getDatabaseProductName)
        assert("1.6.0-SNAPSHOT" == metadata.getDatabaseProductVersion)
        assert(null != metadata.getURL)
        assert(s"jdbc:kyuubi://${getConnectionUrl}/default" == metadata.getURL)
        assert(null == metadata.getUserName)
        assertThrows[NullPointerException](metadata.getDriverVersion)
        assertThrows[NullPointerException](metadata.getDriverName)
        assertThrows[NullPointerException](metadata.getDriverMajorVersion)
        assertThrows[NullPointerException](metadata.getDriverMinorVersion)

    }
  }
  test("database metadata catalogs") {
    withConnection(s"jdbc:kyuubi://${getConnectionUrl}/default") {
      connection =>
        val metadata = connection.getMetaData
        val catalogs = metadata.getCatalogs
        assert(catalogs.next())
        assert("spark_catalog" == catalogs.getString(1))
        assert(!catalogs.next())
        catalogs.close()
    }
  }

  test("database metadata all of schemas") {
    withConnection(s"jdbc:kyuubi://${getConnectionUrl}/default") {
      connection =>
        val metadata = connection.getMetaData
        val schemas = metadata.getSchemas()
        assert(schemas.next())
        assert("default" == schemas.getString(1))
        assert("spark_catalog" == schemas.getString(2))
        assert(schemas.next())
        assert("global_temp" == schemas.getString(1))
        assert("spark_catalog" == schemas.getString(2))
        assert(!schemas.next())
        schemas.close()
    }
  }

  test("database metadata a schema") {
    withConnection(s"jdbc:kyuubi://${getConnectionUrl}/default") {
      connection =>
        val metadata = connection.getMetaData
        val schemas = metadata.getSchemas("spark_catalog", "global%")
        assert(schemas.next())
        assert("global_temp" == schemas.getString(1))
        assert("spark_catalog" == schemas.getString(2))
        assert(!schemas.next())
        schemas.close()
    }
  }
  test("database metadata table types") {
    withConnection(s"jdbc:kyuubi://${getConnectionUrl}/default") {
      connection =>
        val metadata = connection.getMetaData
        val tableTypes = metadata.getTableTypes
        val tableTypeList = new Array[String](2)
        assert(tableTypes.next())
        tableTypeList(0) = tableTypes.getString(1)
        assert(tableTypes.next())
        tableTypeList(1) = tableTypes.getString(1)
        assert(!tableTypes.next())
        assert(tableTypeList.contains("VIEW"))
        assert(tableTypeList.contains("TABLE"))
        tableTypes.close()
    }
  }
  test("database metadata tables") {
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
      val metadata = connection.getMetaData
      val tables = metadata.getTables("spark_catalog", "default", "table_with_all_types", null)
      assert(tables.next())
      assert("spark_catalog" == tables.getString("TABLE_CAT"))
      assert("default" == tables.getString("TABLE_SCHEM"))
      assert("table_with_all_types" == tables.getString("TABLE_NAME"))
      assert("TABLE" == tables.getString("TABLE_TYPE"))
      assert(!tables.next())
      tables.close()
      statement.execute("drop table table_with_all_types")
    }
  }

  test("database metadata columns") {
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
      val metadata = connection.getMetaData
      val columnResult = metadata.getColumns("spark_catalog",
        "default",
        "table_with_all_types",
        null)
      val columns = createColumns(columnResult)
      assert(11 == columns.size)
      assert(3 == columns("deci").dataType)
      assert("DECIMAL(18,2)" == columns("deci").dataTypeName)
      columnResult.close()
      statement.execute("drop table table_with_all_types")
    }
  }

  def createColumns(resultSet: ResultSet): Map[String, Column] = {
    var columns: List[Column] = List.empty
    while (resultSet.next()) {
      columns = createColumn(resultSet) :: columns
    }
    columns.map(col => (col.name, col)).toMap
  }

  def createColumn(resultSet: ResultSet): Column = {
    Column(resultSet.getString("TABLE_CAT"),
      resultSet.getString("TABLE_SCHEM"),
      resultSet.getString("TABLE_NAME"),
      resultSet.getString("COLUMN_NAME"),
      resultSet.getInt("DATA_TYPE"),
      resultSet.getString("TYPE_NAME"),
      if (resultSet.getObject("COLUMN_SIZE") == null)
        None else Some(resultSet.getInt("COLUMN_SIZE")),
      if (resultSet.getObject("DECIMAL_DIGITS") == null)
        None else Some(resultSet.getInt("DECIMAL_DIGITS")),
      resultSet.getInt("ORDINAL_POSITION"))
  }

  case class Column(catalog: String,
                    schema: String,
                    tableName: String,
                    name: String,
                    dataType: Int,
                    dataTypeName: String,
                    size: Option[Int],
                    decimalDigits: Option[Int],
                    position: Int)

  test("database metadata type info") {
    withConnection(s"jdbc:kyuubi://${getConnectionUrl}/default") {
      connection =>
        val metadata = connection.getMetaData
        val typeInfo = metadata.getTypeInfo
        0 until 17 foreach { i =>
          assert(typeInfo.next())
        }
        typeInfo.close()
    }
  }
}
