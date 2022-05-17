package com.gabry.kyuubi.jdbc;

import com.gabry.kyuubi.cli.KyuubiColumn;
import com.gabry.kyuubi.cli.KyuubiTableSchema;

import java.sql.*;

import static com.gabry.kyuubi.cli.KyuubiColumnType.BOOLEAN_TYPE;

public class KyuubiResultSetMetaData implements ResultSetMetaData {
  private final KyuubiTableSchema tableSchema;

  public KyuubiResultSetMetaData(KyuubiTableSchema tableSchema) {
    this.tableSchema = tableSchema;
  }

  @Override
  public int getColumnCount() throws SQLException {
    return tableSchema.getColumnSize();
  }

  @Override
  public boolean isAutoIncrement(int columnIndex) throws SQLException {
    KyuubiColumn column = tableSchema.getColumn(toZeroIndex(columnIndex));
    return column.getType().isAutoIncrement();
  }

  @Override
  public boolean isCaseSensitive(int columnIndex) throws SQLException {
    KyuubiColumn column = tableSchema.getColumn(toZeroIndex(columnIndex));
    return column.getType().isCaseSensitive();
  }

  @Override
  public boolean isSearchable(int columnIndex) throws SQLException {
    KyuubiColumn column = tableSchema.getColumn(toZeroIndex(columnIndex));
    return column.getType().getSearchable() == DatabaseMetaData.typeSearchable;
  }

  @Override
  public boolean isCurrency(int columnIndex) throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  @Override
  public int isNullable(int columnIndex) throws SQLException {
    return ResultSetMetaData.columnNullable;
  }

  @Override
  public boolean isSigned(int columnIndex) throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  @Override
  public int getColumnDisplaySize(int columnIndex) throws SQLException {
    KyuubiColumn column = tableSchema.getColumn(toZeroIndex(columnIndex));
    return column.getType() == BOOLEAN_TYPE ? 4 : column.getColumnSize();
  }

  @Override
  public String getColumnLabel(int columnIndex) throws SQLException {
    return getColumnName(columnIndex);
  }

  @Override
  public String getColumnName(int columnIndex) throws SQLException {
    return tableSchema.getColumn(toZeroIndex(columnIndex)).getName();
  }

  @Override
  public String getSchemaName(int columnIndex) throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  @Override
  public int getPrecision(int columnIndex) throws SQLException {
    return tableSchema.getColumn(toZeroIndex(columnIndex)).getPrecision();
  }

  @Override
  public int getScale(int columnIndex) throws SQLException {
    Integer scale = tableSchema.getColumn(toZeroIndex(columnIndex)).getDecimalDigits();
    return scale == null ? 0 : scale;
  }

  @Override
  public String getTableName(int columnIndex) throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  @Override
  public String getCatalogName(int columnIndex) throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  @Override
  public int getColumnType(int columnIndex) throws SQLException {
    return tableSchema.getColumn(toZeroIndex(columnIndex)).getType().toJavaSQLType();
  }

  @Override
  public String getColumnTypeName(int columnIndex) throws SQLException {
    return JDBCType.valueOf(getColumnType(columnIndex)).name();
  }

  @Override
  public boolean isReadOnly(int columnIndex) throws SQLException {
    return true;
  }

  @Override
  public boolean isWritable(int columnIndex) throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  @Override
  public boolean isDefinitelyWritable(int columnIndex) throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  @Override
  public String getColumnClassName(int columnIndex) throws SQLException {
    return JDBCType.valueOf(
            tableSchema.getColumn(toZeroIndex(columnIndex)).getType().toJavaSQLType())
        .getClass()
        .getName();
  }

  @Override
  public <T> T unwrap(Class<T> iface) throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  @Override
  public boolean isWrapperFor(Class<?> iface) throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  private int toZeroIndex(int columnIndex) {
    return columnIndex - 1;
  }
}
