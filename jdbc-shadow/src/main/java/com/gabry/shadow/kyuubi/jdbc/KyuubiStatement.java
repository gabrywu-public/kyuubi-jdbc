package com.gabry.shadow.kyuubi.jdbc;

import com.gabry.shadow.kyuubi.utils.Utils;
import org.apache.hive.service.cli.HiveSQLException;
import org.apache.hive.service.rpc.thrift.*;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;
import java.util.Map;

public class KyuubiStatement implements java.sql.Statement {
  public static final Logger logger = LoggerFactory.getLogger(KyuubiStatement.class);

  public static final int DEFAULT_FETCH_SIZE = 1000;
  public static final int MAX_FETCH_SIZE = Integer.MAX_VALUE;
  public static final int DEFAULT_MAX_ROWS = 0;
  private final KyuubiConnection boundConnection;
  private final TCLIService.Iface boundClient;
  private final TSessionHandle boundSessionHandle;
  private int fetchSize;
  private int maxRows = DEFAULT_MAX_ROWS;
  private int queryTimeout = 0;
  private TOperationHandle currentOperationHandle = null;
  private ResultSet currentResultSet = null;

  public static KyuubiStatement createStatementForOperation(
      KyuubiConnection connection,
      TCLIService.Iface client,
      TSessionHandle sessionHandle,
      TOperationHandle operationHandle)
      throws SQLException {
    return new KyuubiStatement(connection, client, sessionHandle, operationHandle);
  }

  private KyuubiStatement(
      KyuubiConnection connection,
      TCLIService.Iface client,
      TSessionHandle sessionHandle,
      TOperationHandle operationHandle)
      throws SQLException {
    this.boundConnection = connection;
    this.boundClient = client;
    this.boundSessionHandle = sessionHandle;
    this.fetchSize = KyuubiStatement.MAX_FETCH_SIZE;
    this.currentOperationHandle = operationHandle;
    currentResultSet =
        KyuubiResultSet.create(this, boundClient, currentOperationHandle, boundSessionHandle);
  }

  public KyuubiStatement(
      KyuubiConnection connection,
      TCLIService.Iface client,
      TSessionHandle sessionHandle,
      int fetchSize) {
    this.boundConnection = connection;
    this.boundClient = client;
    this.boundSessionHandle = sessionHandle;
    this.fetchSize = fetchSize;
  }

  @Override
  public ResultSet executeQuery(String sql) throws SQLException {
    if (!execute(sql)) {
      throw new SQLException("The query did not generate a result set!");
    }
    return currentResultSet;
  }

  @Override
  public int executeUpdate(String sql) throws SQLException {
    execute(sql);
    return 0;
  }

  public static void closeOperation(TCLIService.Iface client, TOperationHandle operationHandle)
      throws SQLException {
    if (operationHandle != null) {
      TCloseOperationReq closeReq = new TCloseOperationReq(operationHandle);
      try {
        TCloseOperationResp closeResp = client.CloseOperation(closeReq);
        Utils.throwIfFail(closeResp.getStatus());
      } catch (TException e) {
        throw new SQLException(e);
      }
    }
  }

  @Override
  public void close() throws SQLException {
    if (currentOperationHandle != null) {
      closeOperation();
    }
    if (currentResultSet != null) {
      currentResultSet.close();
      currentResultSet = null;
    }
  }

  public void closeOperation() throws SQLException {
    closeOperation(boundClient, currentOperationHandle);
    currentOperationHandle = null;
  }

  @Override
  public int getMaxFieldSize() throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  @Override
  public void setMaxFieldSize(int max) throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  @Override
  public int getMaxRows() {
    return this.maxRows;
  }

  @Override
  public void setMaxRows(int max) {
    this.maxRows = Math.max(max, 0);
  }

  @Override
  public void setEscapeProcessing(boolean enable) throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  @Override
  public int getQueryTimeout() throws SQLException {
    return queryTimeout;
  }

  @Override
  public void setQueryTimeout(int seconds) throws SQLException {
    this.queryTimeout = seconds;
  }

  @Override
  public void cancel() throws SQLException {
    if (currentOperationHandle != null) {
      TCancelOperationReq cancelReq = new TCancelOperationReq(currentOperationHandle);
      try {
        TCancelOperationResp cancelResp = boundClient.CancelOperation(cancelReq);
        Utils.throwIfFail(cancelResp.getStatus());
      } catch (TException e) {
        throw new SQLException(e);
      } finally {
        currentOperationHandle = null;
      }
    }
  }

  @Override
  public SQLWarning getWarnings() throws SQLException {
    return null;
  }

  @Override
  public void clearWarnings() throws SQLException {}

  @Override
  public void setCursorName(String name) throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  private TOperationHandle runAsyncOnServer(String sql, Map<String, String> conf)
      throws TException, HiveSQLException {
    TExecuteStatementReq execReq = new TExecuteStatementReq(boundSessionHandle, sql);
    execReq.setRunAsync(true);
    if (conf != null) {
      execReq.setConfOverlay(conf);
    }
    execReq.setQueryTimeout(queryTimeout);

    TExecuteStatementResp execResp = boundClient.ExecuteStatement(execReq);
    Utils.throwIfFail(execResp.getStatus());
    return execResp.getOperationHandle();
  }

  private TGetOperationStatusResp waitForOperationToComplete() throws SQLException {
    if (currentOperationHandle == null) {
      throw new SQLException("there is not a running SQL");
    }
    TGetOperationStatusReq statusReq = new TGetOperationStatusReq(currentOperationHandle);
    statusReq.setGetProgressUpdate(true);
    TGetOperationStatusResp statusResp = null;
    boolean isOperationComplete = false;
    while (!isOperationComplete) {
      try {
        statusResp = boundClient.GetOperationStatus(statusReq);
        Utils.throwIfFail(statusResp.getStatus());
        if (statusResp.isSetOperationState()) {
          switch (statusResp.getOperationState()) {
            case INITIALIZED_STATE:
            case RUNNING_STATE:
            case PENDING_STATE:
              break;
            case CLOSED_STATE:
            case FINISHED_STATE:
              isOperationComplete = true;
              break;
            case ERROR_STATE:
              throw new SQLException(
                  statusResp.getErrorMessage(),
                  statusResp.getSqlState(),
                  statusResp.getErrorCode());
            case CANCELED_STATE:
              throw new SQLException("query is cancelled: " + currentOperationHandle);
            case UKNOWN_STATE:
              throw new SQLException("Unknown error");
            case TIMEDOUT_STATE:
              throw new SQLTimeoutException("Query timed out after " + queryTimeout + " seconds");
          }
        }

      } catch (TException e) {
        throw new SQLException(e.getMessage(), e);
      }
    }
    return statusResp;
  }

  private boolean executeWith(String sql, Map<String, String> confOverlay) throws SQLException {
    try {
      currentOperationHandle = runAsyncOnServer(sql, confOverlay);
      TGetOperationStatusResp status = waitForOperationToComplete();
      if (!status.isHasResultSet() && !currentOperationHandle.isHasResultSet()) {
        // reset result to null
        currentResultSet = null;
      } else {
        currentResultSet =
            KyuubiResultSet.create(this, boundClient, currentOperationHandle, boundSessionHandle);
      }
      return currentResultSet != null;
    } catch (TException e) {
      throw new SQLException(e);
    }
  }

  @Override
  public boolean execute(String sql) throws SQLException {
    return executeWith(sql, null);
  }

  @Override
  public ResultSet getResultSet() throws SQLException {
    return currentResultSet;
  }

  @Override
  public int getUpdateCount() throws SQLException {
    waitForOperationToComplete();
    return -1;
  }

  @Override
  public boolean getMoreResults() throws SQLException {
    return false;
  }

  @Override
  public void setFetchDirection(int direction) throws SQLException {
    if (direction != ResultSet.FETCH_FORWARD) {
      throw new SQLException("Not supported direction " + direction);
    }
  }

  @Override
  public int getFetchDirection() throws SQLException {
    return ResultSet.FETCH_FORWARD;
  }

  @Override
  public void setFetchSize(int fetchSize) throws SQLException {
    if (fetchSize > 0) {
      this.fetchSize = fetchSize;
    } else if (fetchSize == 0) {
      // Javadoc for Statement interface states that if the value is zero
      // then "fetch size" hint is ignored.
      // In this case it means reverting it to the default value.
      this.fetchSize = DEFAULT_FETCH_SIZE;
    } else {
      throw new SQLException("Fetch size must be greater or equal to 0");
    }
  }

  @Override
  public int getFetchSize() {
    return fetchSize;
  }

  @Override
  public int getResultSetConcurrency() throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  @Override
  public int getResultSetType() throws SQLException {
    return ResultSet.TYPE_FORWARD_ONLY;
  }

  @Override
  public void addBatch(String sql) throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  @Override
  public void clearBatch() throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  @Override
  public int[] executeBatch() throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  @Override
  public Connection getConnection() {
    return boundConnection;
  }

  @Override
  public boolean getMoreResults(int current) throws SQLException {
    return false;
  }

  @Override
  public ResultSet getGeneratedKeys() throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  @Override
  public int executeUpdate(String sql, int autoGeneratedKeys) throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  @Override
  public int executeUpdate(String sql, int[] columnIndexes) throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  @Override
  public int executeUpdate(String sql, String[] columnNames) throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  @Override
  public boolean execute(String sql, int autoGeneratedKeys) throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  @Override
  public boolean execute(String sql, int[] columnIndexes) throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  @Override
  public boolean execute(String sql, String[] columnNames) throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  @Override
  public int getResultSetHoldability() throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  @Override
  public boolean isClosed() throws SQLException {
    return currentOperationHandle == null;
  }

  @Override
  public void setPoolable(boolean poolable) throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  @Override
  public boolean isPoolable() throws SQLException {
    return false;
  }

  @Override
  public void closeOnCompletion() throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  @Override
  public boolean isCloseOnCompletion() throws SQLException {
    return false;
  }

  @Override
  public <T> T unwrap(Class<T> iface) throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  @Override
  public boolean isWrapperFor(Class<?> iface) throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }
}
