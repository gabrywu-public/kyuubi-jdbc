package com.gabry.shadow.kyuubi.jdbc;

import com.gabry.shadow.kyuubi.cli.KyuubiTableSchema;
import org.apache.hadoop.hive.common.type.HiveIntervalDayTime;
import org.apache.hadoop.hive.common.type.HiveIntervalYearMonth;
import org.apache.hadoop.hive.serde2.thrift.Type;
import org.apache.hive.service.cli.ColumnDescriptor;

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

public abstract class KyuubiBaseResultSet implements ResultSet {
  protected final KyuubiStatement boundStatement;
  protected final int maxRows;
  protected final int fetchSize;
  protected int rowsFetched;
  protected Object[] currentRow;
  protected Iterator<Object[]> rowsIter;
  protected KyuubiTableSchema tableSchema;
  protected boolean wasNull;

  public KyuubiBaseResultSet(KyuubiStatement boundStatement, int maxRows, int fetchSize) {
    this.boundStatement = boundStatement;
    this.maxRows = maxRows;
    this.fetchSize = fetchSize;
    this.rowsFetched = 0;
    this.rowsIter = Collections.emptyIterator();
  }

  @Override
  public boolean isBeforeFirst() throws SQLException {
    return rowsFetched == 0;
  }

  @Override
  public int getRow() throws SQLException {
    return rowsFetched;
  }

  @Override
  public void beforeFirst() throws SQLException {
    throw new SQLFeatureNotSupportedException("beforeFirst not supported yet");
  }

  @Override
  public boolean absolute(int row) throws SQLException {
    throw new SQLFeatureNotSupportedException("absolute not supported");
  }

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
    return null;
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
  public boolean wasNull() throws SQLException {
    return wasNull;
  }
}
