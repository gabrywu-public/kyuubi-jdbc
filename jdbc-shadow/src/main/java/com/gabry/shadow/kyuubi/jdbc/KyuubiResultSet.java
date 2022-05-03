package com.gabry.shadow.kyuubi.jdbc;

import com.gabry.shadow.kyuubi.cli.KyuubiTableSchema;
import com.gabry.shadow.kyuubi.utils.Utils;
import org.apache.hive.service.cli.RowSetFactory;
import org.apache.hive.service.cli.TableSchema;
import org.apache.hive.service.rpc.thrift.*;
import org.apache.thrift.TException;

import java.io.InputStream;
import java.io.Reader;
import java.math.BigDecimal;
import java.sql.*;
import java.util.Map;

public class KyuubiResultSet extends KyuubiBaseResultSet {
  private final TCLIService.Iface boundClient;
  private final TOperationHandle boundOperationHandle;
  private final TSessionHandle boundSessionHandle;

  public KyuubiResultSet(
      KyuubiStatement statement,
      TCLIService.Iface client,
      TOperationHandle operationHandle,
      TSessionHandle sessionHandle) {
    super(statement, statement.getMaxRows(), statement.getFetchSize());
    this.boundClient = client;
    this.boundOperationHandle = operationHandle;
    this.boundSessionHandle = sessionHandle;
  }

  private TableSchema retrieveSchema() throws SQLException {
    TGetResultSetMetadataReq metadataReq = new TGetResultSetMetadataReq(boundOperationHandle);
    try {
      TGetResultSetMetadataResp metadataResp = boundClient.GetResultSetMetadata(metadataReq);
      Utils.throwIfFail(metadataResp.getStatus());
      TTableSchema tTableSchema = metadataResp.getSchema();
      if (tTableSchema == null || !tTableSchema.isSetColumns()) {
        throw new SQLException("table not found: " + boundOperationHandle);
      }
      return new TableSchema(tTableSchema);
    } catch (TException e) {
      throw new SQLException(e);
    }
  }

  @Override
  public boolean next() throws SQLException {
    if (maxRows > 0 && rowsFetched >= maxRows) {
      return false;
    }
    TFetchOrientation orientation =
        isBeforeFirst() ? TFetchOrientation.FETCH_FIRST : TFetchOrientation.FETCH_NEXT;
    if (isBeforeFirst()) {
      setTableSchema(new KyuubiTableSchema(retrieveSchema()));
    }
    if (!rowsIter.hasNext()) {
      TFetchResultsReq fetchReq =
          new TFetchResultsReq(boundOperationHandle, orientation, fetchSize);
      try {
        TFetchResultsResp fetchResp = boundClient.FetchResults(fetchReq);
        Utils.throwIfFail(fetchResp.getStatus());
        TRowSet results = fetchResp.getResults();
        rowsIter =
            RowSetFactory.create(
                    results,
                    ((KyuubiConnection) boundStatement.getConnection()).getProtocolVersion())
                .iterator();
      } catch (TException e) {
        throw new SQLException(e);
      }
    }
    if (rowsIter.hasNext()) {
      rowsFetched++;
      currentRow = rowsIter.next();
      return true;
    } else {
      return false;
    }
  }

  @Override
  public void close() throws SQLException {}


  @Override
  public SQLWarning getWarnings() throws SQLException {
    return null;
  }

  @Override
  public void clearWarnings() throws SQLException {}

  @Override
  public String getCursorName() throws SQLException {
    return null;
  }

  @Override
  public boolean isAfterLast() throws SQLException {
    return false;
  }

  @Override
  public boolean isFirst() throws SQLException {
    return false;
  }

  @Override
  public boolean isLast() throws SQLException {
    return false;
  }

  @Override
  public void afterLast() throws SQLException {}

  @Override
  public boolean first() throws SQLException {
    return false;
  }

  @Override
  public boolean last() throws SQLException {
    return false;
  }

  @Override
  public boolean absolute(int row) throws SQLException {
    return false;
  }

  @Override
  public boolean relative(int rows) throws SQLException {
    return false;
  }

  @Override
  public boolean previous() throws SQLException {
    return false;
  }

  @Override
  public void setFetchDirection(int direction) throws SQLException {}

  @Override
  public int getFetchDirection() throws SQLException {
    return 0;
  }

  @Override
  public void setFetchSize(int rows) throws SQLException {}

  @Override
  public int getFetchSize() throws SQLException {
    return 0;
  }

  @Override
  public int getType() throws SQLException {
    return 0;
  }

  @Override
  public int getConcurrency() throws SQLException {
    return 0;
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
  public void updateNull(int columnIndex) throws SQLException {}

  @Override
  public void updateBoolean(int columnIndex, boolean x) throws SQLException {}

  @Override
  public void updateByte(int columnIndex, byte x) throws SQLException {}

  @Override
  public void updateShort(int columnIndex, short x) throws SQLException {}

  @Override
  public void updateInt(int columnIndex, int x) throws SQLException {}

  @Override
  public void updateLong(int columnIndex, long x) throws SQLException {}

  @Override
  public void updateFloat(int columnIndex, float x) throws SQLException {}

  @Override
  public void updateDouble(int columnIndex, double x) throws SQLException {}

  @Override
  public void updateBigDecimal(int columnIndex, BigDecimal x) throws SQLException {}

  @Override
  public void updateString(int columnIndex, String x) throws SQLException {}

  @Override
  public void updateBytes(int columnIndex, byte[] x) throws SQLException {}

  @Override
  public void updateDate(int columnIndex, Date x) throws SQLException {}

  @Override
  public void updateTime(int columnIndex, Time x) throws SQLException {}

  @Override
  public void updateTimestamp(int columnIndex, Timestamp x) throws SQLException {}

  @Override
  public void updateAsciiStream(int columnIndex, InputStream x, int length) throws SQLException {}

  @Override
  public void updateBinaryStream(int columnIndex, InputStream x, int length) throws SQLException {}

  @Override
  public void updateCharacterStream(int columnIndex, Reader x, int length) throws SQLException {}

  @Override
  public void updateObject(int columnIndex, Object x, int scaleOrLength) throws SQLException {}

  @Override
  public void updateObject(int columnIndex, Object x) throws SQLException {}

  @Override
  public void updateNull(String columnLabel) throws SQLException {}

  @Override
  public void updateBoolean(String columnLabel, boolean x) throws SQLException {}

  @Override
  public void updateByte(String columnLabel, byte x) throws SQLException {}

  @Override
  public void updateShort(String columnLabel, short x) throws SQLException {}

  @Override
  public void updateInt(String columnLabel, int x) throws SQLException {}

  @Override
  public void updateLong(String columnLabel, long x) throws SQLException {}

  @Override
  public void updateFloat(String columnLabel, float x) throws SQLException {}

  @Override
  public void updateDouble(String columnLabel, double x) throws SQLException {}

  @Override
  public void updateBigDecimal(String columnLabel, BigDecimal x) throws SQLException {}

  @Override
  public void updateString(String columnLabel, String x) throws SQLException {}

  @Override
  public void updateBytes(String columnLabel, byte[] x) throws SQLException {}

  @Override
  public void updateDate(String columnLabel, Date x) throws SQLException {}

  @Override
  public void updateTime(String columnLabel, Time x) throws SQLException {}

  @Override
  public void updateTimestamp(String columnLabel, Timestamp x) throws SQLException {}

  @Override
  public void updateAsciiStream(String columnLabel, InputStream x, int length)
      throws SQLException {}

  @Override
  public void updateBinaryStream(String columnLabel, InputStream x, int length)
      throws SQLException {}

  @Override
  public void updateCharacterStream(String columnLabel, Reader reader, int length)
      throws SQLException {}

  @Override
  public void updateObject(String columnLabel, Object x, int scaleOrLength) throws SQLException {}

  @Override
  public void updateObject(String columnLabel, Object x) throws SQLException {}

  @Override
  public void insertRow() throws SQLException {}

  @Override
  public void updateRow() throws SQLException {}

  @Override
  public void deleteRow() throws SQLException {}

  @Override
  public void refreshRow() throws SQLException {}

  @Override
  public void cancelRowUpdates() throws SQLException {}

  @Override
  public void moveToInsertRow() throws SQLException {}

  @Override
  public void moveToCurrentRow() throws SQLException {}

  @Override
  public Statement getStatement() throws SQLException {
    return null;
  }

  @Override
  public Object getObject(int columnIndex, Map<String, Class<?>> map) throws SQLException {
    return null;
  }

  @Override
  public void updateRef(int columnIndex, Ref x) throws SQLException {}

  @Override
  public void updateRef(String columnLabel, Ref x) throws SQLException {}

  @Override
  public void updateBlob(int columnIndex, Blob x) throws SQLException {}

  @Override
  public void updateBlob(String columnLabel, Blob x) throws SQLException {}

  @Override
  public void updateClob(int columnIndex, Clob x) throws SQLException {}

  @Override
  public void updateClob(String columnLabel, Clob x) throws SQLException {}

  @Override
  public void updateArray(int columnIndex, Array x) throws SQLException {}

  @Override
  public void updateArray(String columnLabel, Array x) throws SQLException {}

  @Override
  public RowId getRowId(int columnIndex) throws SQLException {
    return null;
  }

  @Override
  public RowId getRowId(String columnLabel) throws SQLException {
    return null;
  }

  @Override
  public void updateRowId(int columnIndex, RowId x) throws SQLException {}

  @Override
  public void updateRowId(String columnLabel, RowId x) throws SQLException {}

  @Override
  public int getHoldability() throws SQLException {
    return 0;
  }

  @Override
  public boolean isClosed() throws SQLException {
    return false;
  }

  @Override
  public void updateNString(int columnIndex, String nString) throws SQLException {}

  @Override
  public void updateNString(String columnLabel, String nString) throws SQLException {}

  @Override
  public void updateNClob(int columnIndex, NClob nClob) throws SQLException {}

  @Override
  public void updateNClob(String columnLabel, NClob nClob) throws SQLException {}

  @Override
  public NClob getNClob(int columnIndex) throws SQLException {
    return null;
  }

  @Override
  public NClob getNClob(String columnLabel) throws SQLException {
    return null;
  }

  @Override
  public SQLXML getSQLXML(int columnIndex) throws SQLException {
    return null;
  }

  @Override
  public SQLXML getSQLXML(String columnLabel) throws SQLException {
    return null;
  }

  @Override
  public void updateSQLXML(int columnIndex, SQLXML xmlObject) throws SQLException {}

  @Override
  public void updateSQLXML(String columnLabel, SQLXML xmlObject) throws SQLException {}

  @Override
  public String getNString(int columnIndex) throws SQLException {
    return null;
  }

  @Override
  public String getNString(String columnLabel) throws SQLException {
    return null;
  }

  @Override
  public Reader getNCharacterStream(int columnIndex) throws SQLException {
    return null;
  }

  @Override
  public Reader getNCharacterStream(String columnLabel) throws SQLException {
    return null;
  }

  @Override
  public void updateNCharacterStream(int columnIndex, Reader x, long length) throws SQLException {}

  @Override
  public void updateNCharacterStream(String columnLabel, Reader reader, long length)
      throws SQLException {}

  @Override
  public void updateAsciiStream(int columnIndex, InputStream x, long length) throws SQLException {}

  @Override
  public void updateBinaryStream(int columnIndex, InputStream x, long length) throws SQLException {}

  @Override
  public void updateCharacterStream(int columnIndex, Reader x, long length) throws SQLException {}

  @Override
  public void updateAsciiStream(String columnLabel, InputStream x, long length)
      throws SQLException {}

  @Override
  public void updateBinaryStream(String columnLabel, InputStream x, long length)
      throws SQLException {}

  @Override
  public void updateCharacterStream(String columnLabel, Reader reader, long length)
      throws SQLException {}

  @Override
  public void updateBlob(int columnIndex, InputStream inputStream, long length)
      throws SQLException {}

  @Override
  public void updateBlob(String columnLabel, InputStream inputStream, long length)
      throws SQLException {}

  @Override
  public void updateClob(int columnIndex, Reader reader, long length) throws SQLException {}

  @Override
  public void updateClob(String columnLabel, Reader reader, long length) throws SQLException {}

  @Override
  public void updateNClob(int columnIndex, Reader reader, long length) throws SQLException {}

  @Override
  public void updateNClob(String columnLabel, Reader reader, long length) throws SQLException {}

  @Override
  public void updateNCharacterStream(int columnIndex, Reader x) throws SQLException {}

  @Override
  public void updateNCharacterStream(String columnLabel, Reader reader) throws SQLException {}

  @Override
  public void updateAsciiStream(int columnIndex, InputStream x) throws SQLException {}

  @Override
  public void updateBinaryStream(int columnIndex, InputStream x) throws SQLException {}

  @Override
  public void updateCharacterStream(int columnIndex, Reader x) throws SQLException {}

  @Override
  public void updateAsciiStream(String columnLabel, InputStream x) throws SQLException {}

  @Override
  public void updateBinaryStream(String columnLabel, InputStream x) throws SQLException {}

  @Override
  public void updateCharacterStream(String columnLabel, Reader reader) throws SQLException {}

  @Override
  public void updateBlob(int columnIndex, InputStream inputStream) throws SQLException {}

  @Override
  public void updateBlob(String columnLabel, InputStream inputStream) throws SQLException {}

  @Override
  public void updateClob(int columnIndex, Reader reader) throws SQLException {}

  @Override
  public void updateClob(String columnLabel, Reader reader) throws SQLException {}

  @Override
  public void updateNClob(int columnIndex, Reader reader) throws SQLException {}

  @Override
  public void updateNClob(String columnLabel, Reader reader) throws SQLException {}

  @Override
  public <T> T getObject(int columnIndex, Class<T> type) throws SQLException {
    return null;
  }

  @Override
  public <T> T getObject(String columnLabel, Class<T> type) throws SQLException {
    return null;
  }

  @Override
  public <T> T unwrap(Class<T> iface) throws SQLException {
    return null;
  }

  @Override
  public boolean isWrapperFor(Class<?> iface) throws SQLException {
    return false;
  }
}
