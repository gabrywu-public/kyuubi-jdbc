package com.gabry.shadow.kyuubi.jdbc;

import com.gabry.shadow.kyuubi.cli.KyuubiTableSchema;
import org.apache.hive.service.cli.ColumnDescriptor;

import java.sql.*;

public class KyuubiResultSetMetaData implements ResultSetMetaData {
  private final KyuubiTableSchema tableSchema;

  public KyuubiResultSetMetaData(KyuubiTableSchema tableSchema) {
    this.tableSchema = tableSchema;
  }

  @Override
  public int getColumnCount() throws SQLException {
    return tableSchema.getColumnDescriptors().size();
  }

  @Override
  public boolean isAutoIncrement(int columnIndex) throws SQLException {
    ColumnDescriptor column = tableSchema.getColumnDescriptorAt(toZeroIndex(columnIndex));
    return column.getType().isAutoIncrement();
  }

  @Override
  public boolean isCaseSensitive(int columnIndex) throws SQLException {
    ColumnDescriptor column = tableSchema.getColumnDescriptorAt(toZeroIndex(columnIndex));
    return column.getType().isCaseSensitive();
  }

  @Override
  public boolean isSearchable(int columnIndex) throws SQLException {
    ColumnDescriptor column = tableSchema.getColumnDescriptorAt(toZeroIndex(columnIndex));
    return column.getType().getSearchable() == DatabaseMetaData.typeSearchable;
  }

  @Override
  public boolean isCurrency(int columnIndex) throws SQLException {
    return false;
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
    ColumnDescriptor column = tableSchema.getColumnDescriptorAt(toZeroIndex(columnIndex));
    Integer columnSize = column.getTypeDescriptor().getColumnSize();
    return columnSize == null ? Integer.MAX_VALUE : columnSize.intValue();
  }

  @Override
  public String getColumnLabel(int columnIndex) throws SQLException {
    return getColumnName(columnIndex);
  }

  @Override
  public String getColumnName(int columnIndex) throws SQLException {
    return tableSchema.getColumnDescriptorAt(toZeroIndex(columnIndex)).getName();
  }

  @Override
  public String getSchemaName(int columnIndex) throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  @Override
  public int getPrecision(int columnIndex) throws SQLException {
    return tableSchema.getColumnDescriptorAt(toZeroIndex(columnIndex)).getTypeDescriptor().getPrecision();
  }

  @Override
  public int getScale(int columnIndex) throws SQLException {
    Integer scale =
        tableSchema.getColumnDescriptorAt(toZeroIndex(columnIndex)).getTypeDescriptor().getDecimalDigits();
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
    return tableSchema.getColumnDescriptorAt(toZeroIndex(columnIndex)).getType().toJavaSQLType();
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
    return JDBCType.valueOf(tableSchema.getColumnDescriptorAt(toZeroIndex(columnIndex)).getType().toJavaSQLType()).getClass().getName();
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
