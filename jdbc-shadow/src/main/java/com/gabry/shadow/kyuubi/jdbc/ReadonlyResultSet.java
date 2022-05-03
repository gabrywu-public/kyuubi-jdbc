package com.gabry.shadow.kyuubi.jdbc;

import java.io.InputStream;
import java.io.Reader;
import java.math.BigDecimal;
import java.sql.*;

public abstract class ReadonlyResultSet implements ResultSet {
  @Override
  public void updateNCharacterStream(int columnIndex, Reader x, long length) throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  @Override
  public void updateNCharacterStream(String columnLabel, Reader reader, long length)
          throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }
  @Override
  public void updateSQLXML(int columnIndex, SQLXML xmlObject) throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  @Override
  public void updateSQLXML(String columnLabel, SQLXML xmlObject) throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  @Override
  public void updateNString(int columnIndex, String nString) throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  @Override
  public void updateNString(String columnLabel, String nString) throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  @Override
  public void updateNClob(int columnIndex, NClob nClob) throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  @Override
  public void updateNClob(String columnLabel, NClob nClob) throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }
  @Override
  public void updateRowId(int columnIndex, RowId x) throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  @Override
  public void updateRowId(String columnLabel, RowId x) throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  @Override
  public void updateRef(int columnIndex, Ref x) throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  @Override
  public void updateRef(String columnLabel, Ref x) throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  @Override
  public void updateBlob(int columnIndex, Blob x) throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  @Override
  public void updateBlob(String columnLabel, Blob x) throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  @Override
  public void updateClob(int columnIndex, Clob x) throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  @Override
  public void updateClob(String columnLabel, Clob x) throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  @Override
  public void updateArray(int columnIndex, Array x) throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  @Override
  public void updateArray(String columnLabel, Array x) throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  @Override
  public void moveToCurrentRow() throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  @Override
  public void updateObject(int columnIndex, Object x, SQLType targetSqlType, int scaleOrLength)
      throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  @Override
  public void updateObject(String columnLabel, Object x, SQLType targetSqlType, int scaleOrLength)
      throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  @Override
  public void updateObject(int columnIndex, Object x, SQLType targetSqlType) throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  @Override
  public void updateObject(String columnLabel, Object x, SQLType targetSqlType)
      throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  @Override
  public boolean rowUpdated() throws SQLException {
    return false;
  }

  @Override
  public boolean rowInserted() throws SQLException {
    return false;
  }

  @Override
  public boolean rowDeleted() throws SQLException {
    return false;
  }

  @Override
  public void updateNull(int columnIndex) throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  @Override
  public void updateBoolean(int columnIndex, boolean x) throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  @Override
  public void updateByte(int columnIndex, byte x) throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  @Override
  public void updateShort(int columnIndex, short x) throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  @Override
  public void updateInt(int columnIndex, int x) throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  @Override
  public void updateLong(int columnIndex, long x) throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  @Override
  public void updateFloat(int columnIndex, float x) throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  @Override
  public void updateDouble(int columnIndex, double x) throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  @Override
  public void updateBigDecimal(int columnIndex, BigDecimal x) throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  @Override
  public void updateString(int columnIndex, String x) throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  @Override
  public void updateBytes(int columnIndex, byte[] x) throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  @Override
  public void updateDate(int columnIndex, Date x) throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  @Override
  public void updateTime(int columnIndex, Time x) throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  @Override
  public void updateTimestamp(int columnIndex, Timestamp x) throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  @Override
  public void updateAsciiStream(int columnIndex, InputStream x, int length) throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  @Override
  public void updateBinaryStream(int columnIndex, InputStream x, int length) throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  @Override
  public void updateCharacterStream(int columnIndex, Reader x, int length) throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  @Override
  public void updateObject(int columnIndex, Object x, int scaleOrLength) throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  @Override
  public void updateObject(int columnIndex, Object x) throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  @Override
  public void updateNull(String columnLabel) throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  @Override
  public void updateBoolean(String columnLabel, boolean x) throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  @Override
  public void updateByte(String columnLabel, byte x) throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  @Override
  public void updateShort(String columnLabel, short x) throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  @Override
  public void updateInt(String columnLabel, int x) throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  @Override
  public void updateLong(String columnLabel, long x) throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  @Override
  public void updateFloat(String columnLabel, float x) throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  @Override
  public void updateDouble(String columnLabel, double x) throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  @Override
  public void updateBigDecimal(String columnLabel, BigDecimal x) throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  @Override
  public void updateString(String columnLabel, String x) throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  @Override
  public void updateBytes(String columnLabel, byte[] x) throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  @Override
  public void updateDate(String columnLabel, Date x) throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  @Override
  public void updateTime(String columnLabel, Time x) throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  @Override
  public void updateTimestamp(String columnLabel, Timestamp x) throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  @Override
  public void updateAsciiStream(String columnLabel, InputStream x, int length) throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  @Override
  public void updateBinaryStream(String columnLabel, InputStream x, int length)
      throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  @Override
  public void updateCharacterStream(String columnLabel, Reader reader, int length)
      throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  @Override
  public void updateObject(String columnLabel, Object x, int scaleOrLength) throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  @Override
  public void updateObject(String columnLabel, Object x) throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  @Override
  public void insertRow() throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  @Override
  public void updateRow() throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  @Override
  public void deleteRow() throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  @Override
  public void refreshRow() throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  @Override
  public void cancelRowUpdates() throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  @Override
  public void moveToInsertRow() throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  @Override
  public void updateAsciiStream(int columnIndex, InputStream x, long length) throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  @Override
  public void updateBinaryStream(int columnIndex, InputStream x, long length) throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  @Override
  public void updateCharacterStream(int columnIndex, Reader x, long length) throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  @Override
  public void updateAsciiStream(String columnLabel, InputStream x, long length)
          throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  @Override
  public void updateBinaryStream(String columnLabel, InputStream x, long length)
          throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  @Override
  public void updateCharacterStream(String columnLabel, Reader reader, long length)
          throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  @Override
  public void updateBlob(int columnIndex, InputStream inputStream, long length)
          throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  @Override
  public void updateBlob(String columnLabel, InputStream inputStream, long length)
          throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  @Override
  public void updateClob(int columnIndex, Reader reader, long length) throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  @Override
  public void updateClob(String columnLabel, Reader reader, long length) throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  @Override
  public void updateNClob(int columnIndex, Reader reader, long length) throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  @Override
  public void updateNClob(String columnLabel, Reader reader, long length) throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  @Override
  public void updateNCharacterStream(int columnIndex, Reader x) throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  @Override
  public void updateNCharacterStream(String columnLabel, Reader reader) throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  @Override
  public void updateAsciiStream(int columnIndex, InputStream x) throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  @Override
  public void updateBinaryStream(int columnIndex, InputStream x) throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  @Override
  public void updateCharacterStream(int columnIndex, Reader x) throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  @Override
  public void updateAsciiStream(String columnLabel, InputStream x) throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  @Override
  public void updateBinaryStream(String columnLabel, InputStream x) throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  @Override
  public void updateCharacterStream(String columnLabel, Reader reader) throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  @Override
  public void updateBlob(int columnIndex, InputStream inputStream) throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  @Override
  public void updateBlob(String columnLabel, InputStream inputStream) throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  @Override
  public void updateClob(int columnIndex, Reader reader) throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  @Override
  public void updateClob(String columnLabel, Reader reader) throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  @Override
  public void updateNClob(int columnIndex, Reader reader) throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  @Override
  public void updateNClob(String columnLabel, Reader reader) throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }
}
