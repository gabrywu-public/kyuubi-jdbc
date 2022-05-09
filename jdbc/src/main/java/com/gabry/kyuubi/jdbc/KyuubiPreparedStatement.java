package com.gabry.kyuubi.jdbc;

import org.apache.hive.service.rpc.thrift.TCLIService;
import org.apache.hive.service.rpc.thrift.TSessionHandle;

import java.io.InputStream;
import java.io.Reader;
import java.math.BigDecimal;
import java.net.URL;
import java.sql.Date;
import java.sql.*;
import java.text.MessageFormat;
import java.util.*;

public class KyuubiPreparedStatement extends KyuubiStatement implements PreparedStatement {
  private String sqlTemplate;
  /** save the SQL parameters {paramLoc:paramValue} */
  private final HashMap<Integer, String> parameters = new HashMap<Integer, String>();

  public KyuubiPreparedStatement(
      KyuubiConnection connection,
      TCLIService.Iface client,
      TSessionHandle sessionHandle,
      String sqlTemplate) {
    super(connection, client, sessionHandle, DEFAULT_FETCH_SIZE);
    this.sqlTemplate = sqlTemplate;
  }

  private List<String> splitSqlStatement(String sql) {
    List<String> parts = new ArrayList<>();
    int apCount = 0;
    int off = 0;
    boolean skip = false;

    for (int i = 0; i < sql.length(); i++) {
      char c = sql.charAt(i);
      if (skip) {
        skip = false;
        continue;
      }
      switch (c) {
        case '\'':
          apCount++;
          break;
        case '\\':
          skip = true;
          break;
        case '?':
          if ((apCount & 1) == 0) {
            parts.add(sql.substring(off, i));
            off = i + 1;
          }
          break;
        default:
          break;
      }
    }
    parts.add(sql.substring(off, sql.length()));
    return parts;
  }

  private String buildSql(String sqlTemplate, HashMap<Integer, String> parameters)
      throws SQLException {
    List<String> parts = splitSqlStatement(sqlTemplate);
    StringBuilder newSql = new StringBuilder(parts.get(0));
    for (int i = 1; i < parts.size(); i++) {
      if (!parameters.containsKey(i)) {
        throw new SQLException("Parameter #" + i + " is unset");
      }
      newSql.append(parameters.get(i));
      newSql.append(parts.get(i));
    }
    return newSql.toString();
  }

  @Override
  public ResultSet executeQuery() throws SQLException {
    return super.executeQuery(buildSql(sqlTemplate, parameters));
  }

  @Override
  public int executeUpdate() throws SQLException {
    super.executeUpdate(buildSql(sqlTemplate, parameters));
    return 0;
  }

  @Override
  public void setNull(int parameterIndex, int sqlType) throws SQLException {
    this.parameters.put(parameterIndex, "NULL");
  }

  @Override
  public void setBoolean(int parameterIndex, boolean x) throws SQLException {
    this.parameters.put(parameterIndex, "" + x);
  }

  @Override
  public void setByte(int parameterIndex, byte x) throws SQLException {
    this.parameters.put(parameterIndex, "" + x);
  }

  @Override
  public void setShort(int parameterIndex, short x) throws SQLException {
    this.parameters.put(parameterIndex, "" + x);
  }

  @Override
  public void setInt(int parameterIndex, int x) throws SQLException {
    this.parameters.put(parameterIndex, "" + x);
  }

  @Override
  public void setLong(int parameterIndex, long x) throws SQLException {
    this.parameters.put(parameterIndex, "" + x);
  }

  @Override
  public void setFloat(int parameterIndex, float x) throws SQLException {
    this.parameters.put(parameterIndex, "" + x);
  }

  @Override
  public void setDouble(int parameterIndex, double x) throws SQLException {
    this.parameters.put(parameterIndex, "" + x);
  }

  @Override
  public void setBigDecimal(int parameterIndex, BigDecimal x) throws SQLException {
    this.parameters.put(parameterIndex, "" + x);
  }

  private String replaceBackSlashSingleQuote(String x) {
    // scrutinize escape pair, specifically, replace \' to '
    StringBuffer newX = new StringBuffer();
    for (int i = 0; i < x.length(); i++) {
      char c = x.charAt(i);
      if (c == '\\' && i < x.length() - 1) {
        char c1 = x.charAt(i + 1);
        if (c1 == '\'') {
          newX.append(c1);
        } else {
          newX.append(c);
          newX.append(c1);
        }
        i++;
      } else {
        newX.append(c);
      }
    }
    return newX.toString();
  }

  @Override
  public void setString(int parameterIndex, String x) throws SQLException {
    x = replaceBackSlashSingleQuote(x);
    x = x.replace("'", "\\'");
    this.parameters.put(parameterIndex, "'" + x + "'");
  }

  @Override
  public void setBytes(int parameterIndex, byte[] x) throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  @Override
  public void setDate(int parameterIndex, Date x) throws SQLException {
    this.parameters.put(parameterIndex, "'" + x + "'");
  }

  @Override
  public void setTime(int parameterIndex, Time x) throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  @Override
  public void setTimestamp(int parameterIndex, Timestamp x) throws SQLException {
    this.parameters.put(parameterIndex, "'" + x + "'");
  }

  @Override
  public void setAsciiStream(int parameterIndex, InputStream x, int length) throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  @Override
  public void setUnicodeStream(int parameterIndex, InputStream x, int length) throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  @Override
  public void setBinaryStream(int parameterIndex, InputStream x, int length) throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  @Override
  public void clearParameters() throws SQLException {
    this.parameters.clear();
  }

  @Override
  public void setObject(int parameterIndex, Object x, int targetSqlType) throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  @Override
  public void setObject(int parameterIndex, Object x) throws SQLException {
    if (x == null) {
      setNull(parameterIndex, Types.NULL);
    } else if (x instanceof String) {
      setString(parameterIndex, (String) x);
    } else if (x instanceof Short) {
      setShort(parameterIndex, ((Short) x).shortValue());
    } else if (x instanceof Integer) {
      setInt(parameterIndex, ((Integer) x).intValue());
    } else if (x instanceof Long) {
      setLong(parameterIndex, ((Long) x).longValue());
    } else if (x instanceof Float) {
      setFloat(parameterIndex, ((Float) x).floatValue());
    } else if (x instanceof Double) {
      setDouble(parameterIndex, ((Double) x).doubleValue());
    } else if (x instanceof Boolean) {
      setBoolean(parameterIndex, ((Boolean) x).booleanValue());
    } else if (x instanceof Byte) {
      setByte(parameterIndex, ((Byte) x).byteValue());
    } else if (x instanceof Character) {
      setString(parameterIndex, x.toString());
    } else if (x instanceof Timestamp) {
      setTimestamp(parameterIndex, (Timestamp) x);
    } else if (x instanceof BigDecimal) {
      setString(parameterIndex, x.toString());
    } else {
      // Can't infer a type.
      throw new SQLException(
          MessageFormat.format(
              "Can't infer the SQL type to use for an instance of {0}. Use setObject() with an explicit Types value to specify the type to use.",
              x.getClass().getName()));
    }
  }

  @Override
  public boolean execute() throws SQLException {
    return super.execute(buildSql(sqlTemplate, parameters));
  }

  @Override
  public void addBatch() throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  @Override
  public void setCharacterStream(int parameterIndex, Reader reader, int length)
      throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  @Override
  public void setRef(int parameterIndex, Ref x) throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  @Override
  public void setBlob(int parameterIndex, Blob x) throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  @Override
  public void setClob(int parameterIndex, Clob x) throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  @Override
  public void setArray(int parameterIndex, Array x) throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  @Override
  public ResultSetMetaData getMetaData() throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  @Override
  public void setDate(int parameterIndex, Date x, Calendar cal) throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  @Override
  public void setTime(int parameterIndex, Time x, Calendar cal) throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  @Override
  public void setTimestamp(int parameterIndex, Timestamp x, Calendar cal) throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  @Override
  public void setNull(int parameterIndex, int sqlType, String typeName) throws SQLException {
    this.setNull(parameterIndex, sqlType);
  }

  @Override
  public void setURL(int parameterIndex, URL x) throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  @Override
  public ParameterMetaData getParameterMetaData() throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  @Override
  public void setRowId(int parameterIndex, RowId x) throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  @Override
  public void setNString(int parameterIndex, String value) throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  @Override
  public void setNCharacterStream(int parameterIndex, Reader value, long length)
      throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  @Override
  public void setNClob(int parameterIndex, NClob value) throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  @Override
  public void setClob(int parameterIndex, Reader reader, long length) throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  @Override
  public void setBlob(int parameterIndex, InputStream inputStream, long length)
      throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  @Override
  public void setNClob(int parameterIndex, Reader reader, long length) throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  @Override
  public void setSQLXML(int parameterIndex, SQLXML xmlObject) throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  @Override
  public void setObject(int parameterIndex, Object x, int targetSqlType, int scaleOrLength)
      throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  @Override
  public void setAsciiStream(int parameterIndex, InputStream x, long length) throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  @Override
  public void setBinaryStream(int parameterIndex, InputStream x, long length) throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  @Override
  public void setCharacterStream(int parameterIndex, Reader reader, long length)
      throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  @Override
  public void setAsciiStream(int parameterIndex, InputStream x) throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  @Override
  public void setBinaryStream(int parameterIndex, InputStream x) throws SQLException {
    String str = new Scanner(x, "UTF-8").useDelimiter("\\A").next();
    setString(parameterIndex, str);
  }

  @Override
  public void setCharacterStream(int parameterIndex, Reader reader) throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  @Override
  public void setNCharacterStream(int parameterIndex, Reader value) throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  @Override
  public void setClob(int parameterIndex, Reader reader) throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  @Override
  public void setBlob(int parameterIndex, InputStream inputStream) throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  @Override
  public void setNClob(int parameterIndex, Reader reader) throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }
}
