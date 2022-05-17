package com.gabry.kyuubi.jdbc;

import com.gabry.kyuubi.cli.KyuubiColumn;
import com.gabry.kyuubi.cli.KyuubiColumnType;
import com.gabry.kyuubi.cli.KyuubiTableSchema;
import com.gabry.kyuubi.utils.Utils;
import org.apache.hadoop.hive.common.type.HiveIntervalDayTime;
import org.apache.hadoop.hive.common.type.HiveIntervalYearMonth;
import org.apache.hive.service.cli.FetchType;
import org.apache.hive.service.cli.RowSetFactory;
import org.apache.hive.service.rpc.thrift.*;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.math.BigDecimal;
import java.math.MathContext;
import java.nio.charset.StandardCharsets;
import java.sql.*;
import java.util.Collections;
import java.util.Iterator;

public class KyuubiResultSet extends AbstractKyuubiResultSet {
  private static final Logger logger = LoggerFactory.getLogger(KyuubiResultSet.class);
  protected final KyuubiConnection boundConnection;
  protected final KyuubiStatement boundStatement;
  protected final int maxRows;
  protected int fetchSize;
  protected int currentRowNumber;
  protected Object[] currentRow;
  protected Iterator<Object[]> rowsIter;
  private final KyuubiTableSchema tableSchema;
  protected boolean wasNull;
  protected KyuubiResultSetMetaData metaData;
  private final TCLIService.Iface boundClient;
  private final TOperationHandle boundOperationHandle;
  private final TSessionHandle boundSessionHandle;

  private final FetchType fetchType;

  public static KyuubiResultSet create(
      KyuubiStatement statement,
      TCLIService.Iface client,
      TOperationHandle operationHandle,
      TSessionHandle sessionHandle,
      FetchType fetchType)
      throws SQLException {
    KyuubiTableSchema tableSchema = retrieveSchema(client, operationHandle);

    return new KyuubiResultSet(
        statement,
        client,
        operationHandle,
        fetchType,
        sessionHandle,
        tableSchema,
        statement.getMaxRows(),
        statement.getFetchSize());
  }

  private KyuubiResultSet(
      KyuubiStatement statement,
      TCLIService.Iface client,
      TOperationHandle operationHandle,
      FetchType fetchType,
      TSessionHandle sessionHandle,
      KyuubiTableSchema tableSchema,
      int maxRows,
      int fetchSize) {
    this(
        statement,
        (KyuubiConnection) statement.getConnection(),
        client,
        operationHandle,
        fetchType,
        sessionHandle,
        tableSchema,
        maxRows,
        fetchSize);
  }

  private KyuubiResultSet(
      KyuubiStatement statement,
      KyuubiConnection connection,
      TCLIService.Iface client,
      TOperationHandle operationHandle,
      FetchType fetchType,
      TSessionHandle sessionHandle,
      KyuubiTableSchema tableSchema,
      int maxRows,
      int fetchSize) {
    this.boundStatement = statement;
    this.boundConnection = connection;
    this.maxRows = maxRows;
    this.fetchSize = fetchSize;
    this.currentRowNumber = 0;
    this.rowsIter = Collections.emptyIterator();
    this.boundClient = client;
    this.boundOperationHandle = operationHandle;
    this.fetchType = fetchType;
    this.boundSessionHandle = sessionHandle;
    this.tableSchema = tableSchema;
    this.metaData = new KyuubiResultSetMetaData(this.tableSchema);
  }

  private static KyuubiTableSchema retrieveSchema(
      TCLIService.Iface client, TOperationHandle operationHandle) throws SQLException {
    TGetResultSetMetadataReq metadataReq = new TGetResultSetMetadataReq(operationHandle);
    try {
      TGetResultSetMetadataResp metadataResp = client.GetResultSetMetadata(metadataReq);
      Utils.throwIfFail(metadataResp.getStatus());
      TTableSchema tTableSchema = metadataResp.getSchema();
      if (tTableSchema == null || !tTableSchema.isSetColumns()) {
        throw new SQLException("table not found: " + operationHandle);
      }
      return new KyuubiTableSchema(tTableSchema);
    } catch (TException e) {
      throw new SQLException(e);
    }
  }

  @Override
  public boolean isBeforeFirst() throws SQLException {
    return currentRowNumber == 0;
  }

  @Override
  public boolean next() throws SQLException {
    if (!rowsIter.hasNext()) {
      TFetchOrientation orientation =
          isBeforeFirst() ? TFetchOrientation.FETCH_FIRST : TFetchOrientation.FETCH_NEXT;
      TFetchResultsReq fetchReq =
          new TFetchResultsReq(boundOperationHandle, orientation, fetchSize);
      fetchReq.setFetchType(fetchType.toTFetchType());
      try {
        TFetchResultsResp fetchResp = boundClient.FetchResults(fetchReq);
        Utils.throwIfFail(fetchResp.getStatus());
        TRowSet results = fetchResp.getResults();
        rowsIter = RowSetFactory.create(results, boundConnection.getProtocolVersion()).iterator();
      } catch (TException e) {
        throw new SQLException("failed to fetch result {}", e.getMessage(), e);
      }
    }
    currentRow = rowsIter.hasNext() ? rowsIter.next() : null;
    if (currentRow != null) {
      currentRowNumber++;
    }
    return currentRow != null;
  }

  @Override
  public void close() throws SQLException {
    if (!boundStatement.isClosed()) {
      boundStatement.closeOperation();
      currentRow = null;
      currentRowNumber = 0;
      rowsIter = Collections.emptyIterator();
    }
  }

  /**
   * Retrieves the current row number. The first row is number 1, the second number 2, and so on.
   */
  @Override
  public int getRow() throws SQLException {
    return currentRowNumber;
  }

  @Override
  public int getFetchDirection() throws SQLException {
    return ResultSet.FETCH_FORWARD;
  }

  @Override
  public void setFetchSize(int rows) throws SQLException {
    this.fetchSize = rows;
  }

  @Override
  public int getFetchSize() throws SQLException {
    return fetchSize;
  }

  @Override
  public int getType() throws SQLException {
    return ResultSet.TYPE_FORWARD_ONLY;
  }

  @Override
  public int getConcurrency() throws SQLException {
    return ResultSet.CONCUR_READ_ONLY;
  }

  /**
   * @param columnLabel the label for the column specified with the SQL AS clause. If the SQL AS
   *     clause was not specified, then the label is the name of the column
   * @return first column index is 1, second is 2
   * @throws SQLException if the ResultSet object does not contain a column labeled columnLabel, a
   *     database access error occurs or this method is called on a closed result set
   */
  @Override
  public int findColumn(String columnLabel) throws SQLException {
    KyuubiColumn columnDescriptor = tableSchema.getColumn(columnLabel);
    if (columnDescriptor == null) {
      throw new SQLException("can't find column " + columnLabel);
    }
    return columnDescriptor.getPosition() + 1;
  }

  @Override
  public BigDecimal getBigDecimal(int columnIndex) throws SQLException {
    return (BigDecimal) getObject(columnIndex);
  }

  @Override
  public BigDecimal getBigDecimal(String columnLabel) throws SQLException {
    return getBigDecimal(findColumn(columnLabel));
  }

  @Override
  public boolean last() throws SQLException {
    return false;
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
    Object obj = getObject(columnIndex);
    if (obj instanceof Number) {
      return ((Number) obj).byteValue();
    } else if (obj == null) {
      return 0;
    }
    throw new SQLException("Cannot convert column " + columnIndex + " to byte");
  }

  @Override
  public short getShort(int columnIndex) throws SQLException {
    try {
      Object obj = getObject(columnIndex);
      if (obj instanceof Number) {
        return ((Number) obj).shortValue();
      } else if (obj == null) {
        return 0;
      } else if (obj instanceof String) {
        return Short.parseShort((String) obj);
      }
      throw new Exception("Illegal conversion");
    } catch (Exception e) {
      throw new SQLException(
          "Cannot convert column " + columnIndex + " to short: " + e.toString(), e);
    }
  }

  @Override
  public int getInt(int columnIndex) throws SQLException {
    try {
      Object obj = getObject(columnIndex);
      if (obj instanceof Number) {
        return ((Number) obj).intValue();
      } else if (obj == null) {
        return 0;
      } else if (obj instanceof String) {
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
    try {
      Object obj = getObject(columnIndex);
      if (obj instanceof Number) {
        return ((Number) obj).longValue();
      } else if (obj == null) {
        return 0;
      } else if (obj instanceof String) {
        return Long.parseLong((String) obj);
      }
      throw new Exception("Illegal conversion");
    } catch (Exception e) {
      throw new SQLException(
          "Cannot convert column " + columnIndex + " to long: " + e.toString(), e);
    }
  }

  @Override
  public float getFloat(int columnIndex) throws SQLException {
    try {
      Object obj = getObject(columnIndex);
      if (obj instanceof Number) {
        return ((Number) obj).floatValue();
      } else if (obj == null) {
        return 0;
      } else if (obj instanceof String) {
        return Float.parseFloat((String) obj);
      }
      throw new Exception("Illegal conversion");
    } catch (Exception e) {
      throw new SQLException(
          "Cannot convert column " + columnIndex + " to float: " + e.toString(), e);
    }
  }

  @Override
  public double getDouble(int columnIndex) throws SQLException {
    try {
      Object obj = getObject(columnIndex);
      if (obj instanceof Number) {
        return ((Number) obj).doubleValue();
      } else if (obj == null) {
        return 0;
      } else if (obj instanceof String) {
        return Double.parseDouble((String) obj);
      }
      throw new Exception("Illegal conversion");
    } catch (Exception e) {
      throw new SQLException(
          "Cannot convert column " + columnIndex + " to double: " + e.toString(), e);
    }
  }

  @Override
  public BigDecimal getBigDecimal(int columnIndex, int scale) throws SQLException {
    MathContext mc = new MathContext(scale);
    return getBigDecimal(columnIndex).round(mc);
  }

  @Override
  public Date getDate(int columnIndex) throws SQLException {
    Object obj = getObject(columnIndex);
    if (obj == null) {
      return null;
    }
    if (obj instanceof Date) {
      return (Date) obj;
    }
    try {
      if (obj instanceof String) {
        return Date.valueOf((String) obj);
      }
    } catch (Exception e) {
      throw new SQLException(
          "Cannot convert column " + columnIndex + " to date: " + e.toString(), e);
    }
    // If we fell through to here this is not a valid type conversion
    throw new SQLException("Cannot convert column " + columnIndex + " to date: Illegal conversion");
  }

  @Override
  public Timestamp getTimestamp(int columnIndex) throws SQLException {
    Object obj = getObject(columnIndex);
    if (obj == null) {
      return null;
    }
    if (obj instanceof Timestamp) {
      return (Timestamp) obj;
    }
    if (obj instanceof String) {
      return Timestamp.valueOf((String) obj);
    }
    throw new SQLException("Illegal conversion");
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
    return getByte(findColumn(columnLabel));
  }

  @Override
  public short getShort(String columnLabel) throws SQLException {
    return getShort(findColumn(columnLabel));
  }

  @Override
  public int getInt(String columnLabel) throws SQLException {
    return getInt(findColumn(columnLabel));
  }

  @Override
  public long getLong(String columnLabel) throws SQLException {
    return getLong(findColumn(columnLabel));
  }

  @Override
  public float getFloat(String columnLabel) throws SQLException {
    return getFloat(findColumn(columnLabel));
  }

  @Override
  public double getDouble(String columnLabel) throws SQLException {
    return getDouble(findColumn(columnLabel));
  }

  @Override
  public BigDecimal getBigDecimal(String columnLabel, int scale) throws SQLException {
    return getBigDecimal(findColumn(columnLabel), scale);
  }

  @Override
  public byte[] getBytes(String columnLabel) throws SQLException {
    return getBytes(findColumn(columnLabel));
  }

  @Override
  public Date getDate(String columnLabel) throws SQLException {
    return getDate(findColumn(columnLabel));
  }

  @Override
  public Time getTime(String columnLabel) throws SQLException {
    return getTime(findColumn(columnLabel));
  }

  @Override
  public Timestamp getTimestamp(String columnLabel) throws SQLException {
    return getTimestamp(findColumn(columnLabel));
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
  public ResultSetMetaData getMetaData() throws SQLException {
    return metaData;
  }

  @Override
  public Object getObject(int columnIndex) throws SQLException {
    int zeroBasedIndex = toZeroIndex(columnIndex);
    KyuubiColumnType columnType = tableSchema.getColumn(zeroBasedIndex).getType();
    Object rawObj = currentRow[zeroBasedIndex];
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
    return boundStatement;
  }

  @Override
  public boolean isClosed() throws SQLException {
    return boundStatement.isClosed();
  }

  @Override
  public boolean wasNull() throws SQLException {
    return wasNull;
  }

  private int toZeroIndex(int columnIndex) {
    return columnIndex - 1;
  }
}
