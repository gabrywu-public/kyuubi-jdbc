package com.gabry.shadow.kyuubi.jdbc;

import com.gabry.shadow.kyuubi.cli.KyuubiTableSchema;
import com.gabry.shadow.kyuubi.utils.Utils;
import org.apache.hadoop.hive.common.type.HiveIntervalDayTime;
import org.apache.hadoop.hive.common.type.HiveIntervalYearMonth;
import org.apache.hadoop.hive.serde2.thrift.Type;
import org.apache.hive.service.cli.ColumnDescriptor;
import org.apache.hive.service.cli.RowSetFactory;
import org.apache.hive.service.cli.TableSchema;
import org.apache.hive.service.rpc.thrift.*;
import org.apache.thrift.TException;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.io.Reader;
import java.math.BigDecimal;
import java.math.MathContext;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.sql.*;
import java.util.Calendar;
import java.util.Collections;
import java.util.Iterator;
import java.util.Map;

public class KyuubiResultSet implements ResultSet {
  protected final KyuubiStatement boundStatement;
  protected final int maxRows;
  protected final int fetchSize;
  protected int rowsFetched;
  protected Object[] currentRow;
  protected Iterator<Object[]> rowsIter;
  private KyuubiTableSchema tableSchema;
  protected boolean wasNull;
  protected KyuubiResultSetMetaData metaData;
  private final TCLIService.Iface boundClient;
  private final TOperationHandle boundOperationHandle;
  private final TSessionHandle boundSessionHandle;

  public KyuubiResultSet(
      KyuubiStatement boundStatement,
      TCLIService.Iface client,
      TOperationHandle operationHandle,
      TSessionHandle sessionHandle) {
    this.boundStatement = boundStatement;
    this.maxRows = boundStatement.getMaxRows();
    this.fetchSize = boundStatement.getFetchSize();
    this.rowsFetched = 0;
    this.rowsIter = Collections.emptyIterator();
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
  public boolean isBeforeFirst() throws SQLException {
    return rowsFetched == 0;
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

  protected void setTableSchema(KyuubiTableSchema tableSchema) {
    this.tableSchema = tableSchema;
    this.metaData = new KyuubiResultSetMetaData(tableSchema);
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
  public int getRow() throws SQLException {
    return rowsFetched;
  }

  @Override
  public void beforeFirst() throws SQLException {
    throw new SQLFeatureNotSupportedException("beforeFirst not supported yet");
  }

  @Override
  public void afterLast() throws SQLException {}

  @Override
  public boolean absolute(int row) throws SQLException {
    throw new SQLFeatureNotSupportedException("absolute not supported");
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
  public int findColumn(String columnLabel) throws SQLException {
    ColumnDescriptor columnDescriptor = tableSchema.getColumnDescriptorOf(columnLabel);
    if (columnDescriptor == null) {
      throw new SQLException("can't find column " + columnLabel);
    }
    return columnDescriptor.getOrdinalPosition();
  }

  @Override
  public Reader getCharacterStream(int columnIndex) throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  @Override
  public Reader getCharacterStream(String columnLabel) throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  private <T> T tryGet(int columnIndex, Class<T> clazz) throws SQLException {
    Object obj = getObject(columnIndex);
    return wasNull ? null : (T) obj;
  }

  @Override
  public BigDecimal getBigDecimal(int columnIndex) throws SQLException {
    return tryGet(columnIndex, BigDecimal.class);
  }

  @Override
  public BigDecimal getBigDecimal(String columnLabel) throws SQLException {
    return getBigDecimal(findColumn(columnLabel));
  }

  @Override
  public boolean first() throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  @Override
  public boolean last() throws SQLException {
    return false;
  }

  @Override
  public Array getArray(int i) throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  @Override
  public Array getArray(String colName) throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  @Override
  public String getString(int columnIndex) throws SQLException {
    Object value = getObject(columnIndex);
    if (wasNull) {
      return null;
    }
    if (value instanceof byte[]) {
      return new String((byte[]) value);
    }
    return value.toString();
  }

  @Override
  public boolean getBoolean(int columnIndex) throws SQLException {
    Object obj = getObject(columnIndex);
    if (Boolean.class.isInstance(obj)) {
      return (Boolean) obj;
    } else if (obj == null) {
      return false;
    } else if (Number.class.isInstance(obj)) {
      return ((Number) obj).intValue() != 0;
    } else if (String.class.isInstance(obj)) {
      return !((String) obj).equals("0");
    }
    throw new SQLException("Cannot convert column " + columnIndex + " to boolean");
  }

  @Override
  public byte getByte(int columnIndex) throws SQLException {
    return 0;
  }

  @Override
  public short getShort(int columnIndex) throws SQLException {
    return 0;
  }

  @Override
  public int getInt(int columnIndex) throws SQLException {
    try {
      Object obj = getObject(columnIndex);
      if (Number.class.isInstance(obj)) {
        return ((Number) obj).intValue();
      } else if (obj == null) {
        return 0;
      } else if (String.class.isInstance(obj)) {
        return Integer.parseInt((String) obj);
      }
      throw new Exception("Illegal conversion");
    } catch (Exception e) {
      throw new SQLException(
          "Cannot convert column " + columnIndex + " to integer" + e.toString(), e);
    }
  }

  @Override
  public long getLong(int columnIndex) throws SQLException {
    return 0;
  }

  @Override
  public float getFloat(int columnIndex) throws SQLException {
    return 0;
  }

  @Override
  public double getDouble(int columnIndex) throws SQLException {
    return 0;
  }

  @Override
  public BigDecimal getBigDecimal(int columnIndex, int scale) throws SQLException {
    MathContext mc = new MathContext(scale);
    return getBigDecimal(columnIndex).round(mc);
  }

  @Override
  public byte[] getBytes(int columnIndex) throws SQLException {
    return new byte[0];
  }

  @Override
  public Date getDate(int columnIndex) throws SQLException {
    return null;
  }

  @Override
  public Time getTime(int columnIndex) throws SQLException {
    return null;
  }

  @Override
  public Timestamp getTimestamp(int columnIndex) throws SQLException {
    return null;
  }

  @Override
  public InputStream getAsciiStream(int columnIndex) throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  @Override
  public InputStream getUnicodeStream(int columnIndex) throws SQLException {
    return null;
  }

  @Override
  public InputStream getBinaryStream(int columnIndex) throws SQLException {
    Object obj = getObject(columnIndex);
    if (obj == null) {
      return null;
    } else if (obj instanceof InputStream) {
      return (InputStream) obj;
    } else if (obj instanceof byte[]) {
      byte[] byteArray = (byte[]) obj;
      return new ByteArrayInputStream(byteArray);
    } else if (obj instanceof String) {
      String str = (String) obj;
      return new ByteArrayInputStream(str.getBytes(StandardCharsets.UTF_8));
    } else {
      throw new SQLException("Illegal conversion to binary stream from column " + columnIndex);
    }
  }

  @Override
  public String getString(String columnLabel) throws SQLException {
    return getString(findColumn(columnLabel));
  }

  @Override
  public boolean getBoolean(String columnLabel) throws SQLException {
    return getBoolean(findColumn(columnLabel));
  }

  @Override
  public byte getByte(String columnLabel) throws SQLException {
    return 0;
  }

  @Override
  public short getShort(String columnLabel) throws SQLException {
    return 0;
  }

  @Override
  public int getInt(String columnLabel) throws SQLException {
    return getInt(findColumn(columnLabel));
  }

  @Override
  public long getLong(String columnLabel) throws SQLException {
    return 0;
  }

  @Override
  public float getFloat(String columnLabel) throws SQLException {
    return 0;
  }

  @Override
  public double getDouble(String columnLabel) throws SQLException {
    return 0;
  }

  @Override
  public BigDecimal getBigDecimal(String columnLabel, int scale) throws SQLException {
    return getBigDecimal(findColumn(columnLabel), scale);
  }

  @Override
  public byte[] getBytes(String columnLabel) throws SQLException {
    return new byte[0];
  }

  @Override
  public Date getDate(String columnLabel) throws SQLException {
    return null;
  }

  @Override
  public Time getTime(String columnLabel) throws SQLException {
    return null;
  }

  @Override
  public Timestamp getTimestamp(String columnLabel) throws SQLException {
    return null;
  }

  @Override
  public InputStream getAsciiStream(String columnName) throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  @Override
  public InputStream getUnicodeStream(String columnLabel) throws SQLException {
    return null;
  }

  @Override
  public InputStream getBinaryStream(String columnLabel) throws SQLException {
    return getBinaryStream(findColumn(columnLabel));
  }

  @Override
  public SQLWarning getWarnings() throws SQLException {
    return null;
  }

  @Override
  public void clearWarnings() throws SQLException {}

  @Override
  public Blob getBlob(int i) throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  @Override
  public Blob getBlob(String colName) throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  @Override
  public String getCursorName() throws SQLException {
    return null;
  }

  @Override
  public ResultSetMetaData getMetaData() throws SQLException {
    return metaData;
  }

  @Override
  public Object getObject(int columnIndex) throws SQLException {
    if (currentRow == null) {
      throw new SQLException("No row found.");
    }
    if (currentRow.length == 0) {
      throw new SQLException("RowSet does not contain any columns!");
    }
    if (columnIndex > currentRow.length) {
      throw new SQLException("Invalid columnIndex: " + columnIndex);
    }
    Type columnType = tableSchema.getColumnDescriptorAt(columnIndex).getType();
    Object rawObj = currentRow[columnIndex];
    Object convertedObj = rawObj;
    switch (columnType) {
      case BINARY_TYPE:
        if (rawObj instanceof String) {
          convertedObj = ((String) rawObj).getBytes();
        }
        break;
      case TIMESTAMP_TYPE:
        convertedObj = Timestamp.valueOf((String) rawObj);
        break;
      case DECIMAL_TYPE:
        convertedObj = new BigDecimal((String) rawObj);
        break;
      case DATE_TYPE:
        convertedObj = Date.valueOf((String) rawObj);
        break;
      case INTERVAL_YEAR_MONTH_TYPE:
        convertedObj = HiveIntervalYearMonth.valueOf((String) rawObj);
        break;
      case INTERVAL_DAY_TIME_TYPE:
        convertedObj = HiveIntervalDayTime.valueOf((String) rawObj);
        break;
    }
    wasNull = convertedObj == null;
    return convertedObj;
  }

  @Override
  public Object getObject(String columnLabel) throws SQLException {
    return getObject(findColumn(columnLabel));
  }

  @Override
  public Statement getStatement() throws SQLException {
    return null;
  }

  @Override
  public Object getObject(int columnIndex, Map<String, Class<?>> map) throws SQLException {
    return null;
  }

  @Override
  public Ref getRef(int columnIndex) throws SQLException {
    return null;
  }

  @Override
  public Clob getClob(int columnIndex) throws SQLException {
    return null;
  }

  @Override
  public Object getObject(String columnLabel, Map<String, Class<?>> map) throws SQLException {
    return null;
  }

  @Override
  public Ref getRef(String columnLabel) throws SQLException {
    return null;
  }

  @Override
  public Clob getClob(String columnLabel) throws SQLException {
    return null;
  }

  @Override
  public Date getDate(int columnIndex, Calendar cal) throws SQLException {
    return null;
  }

  @Override
  public Date getDate(String columnLabel, Calendar cal) throws SQLException {
    return null;
  }

  @Override
  public Time getTime(int columnIndex, Calendar cal) throws SQLException {
    return null;
  }

  @Override
  public Time getTime(String columnLabel, Calendar cal) throws SQLException {
    return null;
  }

  @Override
  public Timestamp getTimestamp(int columnIndex, Calendar cal) throws SQLException {
    return null;
  }

  @Override
  public Timestamp getTimestamp(String columnLabel, Calendar cal) throws SQLException {
    return null;
  }

  @Override
  public URL getURL(int columnIndex) throws SQLException {
    return null;
  }

  @Override
  public URL getURL(String columnLabel) throws SQLException {
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
  public boolean wasNull() throws SQLException {
    return wasNull;
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
