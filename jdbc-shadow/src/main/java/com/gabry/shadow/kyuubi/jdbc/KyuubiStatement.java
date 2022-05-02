package com.gabry.shadow.kyuubi.jdbc;

import com.gabry.shadow.kyuubi.utils.Utils;
import org.apache.hive.service.cli.HiveSQLException;
import org.apache.hive.service.rpc.thrift.*;
import org.apache.thrift.TException;

import java.sql.*;
import java.util.Map;

public class KyuubiStatement implements java.sql.Statement {
  public static final int DEFAULT_FETCH_SIZE = 1000;
  private final KyuubiConnection boundConnection;
  private final TCLIService.Iface boundClient;
  private final TSessionHandle boundSessionHandle;
  private int fetchSize;
  private int maxRows = 0;

  private int queryTimeout = 0;
  private TOperationHandle operationHandle = null;
  private ResultSet resultSet = null;

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
    return null;
  }

  @Override
  public int executeUpdate(String sql) throws SQLException {
    return 0;
  }

  @Override
  public void close() throws SQLException {}

  @Override
  public int getMaxFieldSize() throws SQLException {
    return 0;
  }

  @Override
  public void setMaxFieldSize(int max) throws SQLException {}

  @Override
  public int getMaxRows() {
    return this.maxRows;
  }

  @Override
  public void setMaxRows(int max) {
    this.maxRows = max;
  }

  @Override
  public void setEscapeProcessing(boolean enable) throws SQLException {}

  @Override
  public int getQueryTimeout() throws SQLException {
    return queryTimeout;
  }

  @Override
  public void setQueryTimeout(int seconds) throws SQLException {
    this.queryTimeout = seconds;
  }

  @Override
  public void cancel() throws SQLException {}

  @Override
  public SQLWarning getWarnings() throws SQLException {
    return null;
  }

  @Override
  public void clearWarnings() throws SQLException {}

  @Override
  public void setCursorName(String name) throws SQLException {}

  private TOperationHandle runAsyncOnServer(String sql, Map<String, String> conf) {
    TExecuteStatementReq execReq = new TExecuteStatementReq(boundSessionHandle, sql);
    execReq.setRunAsync(true);
    if (conf != null) {
      execReq.setConfOverlay(conf);
    }
    execReq.setQueryTimeout(queryTimeout);
    try {
      TExecuteStatementResp execResp = boundClient.ExecuteStatement(execReq);
      Utils.throwIfFail(execResp.getStatus());
      return execResp.getOperationHandle();
    } catch (TException | HiveSQLException e) {
      throw new RuntimeException(e);
    }
  }

  private TGetOperationStatusResp waitForOperationToComplete() throws SQLException {
    TGetOperationStatusReq statusReq = new TGetOperationStatusReq(operationHandle);
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
              throw new SQLException("query is cancelled: " + operationHandle);
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
    operationHandle = runAsyncOnServer(sql, confOverlay);
    TGetOperationStatusResp status = waitForOperationToComplete();
    if (!status.isHasResultSet() && !operationHandle.isHasResultSet()) {
      return false;
    }
    resultSet = new KyuubiResultSet(this, boundClient, operationHandle, boundSessionHandle);
    return true;
  }

  @Override
  public boolean execute(String sql) throws SQLException {
    return executeWith(sql, null);
  }

  @Override
  public ResultSet getResultSet() throws SQLException {
    return null;
  }

  @Override
  public int getUpdateCount() throws SQLException {
    return 0;
  }

  @Override
  public boolean getMoreResults() throws SQLException {
    return false;
  }

  @Override
  public void setFetchDirection(int direction) throws SQLException {}

  @Override
  public int getFetchDirection() throws SQLException {
    return this.fetchSize;
  }

  @Override
  public void setFetchSize(int fetchSize) {
    this.fetchSize = fetchSize;
  }

  @Override
  public int getFetchSize() {
    return fetchSize;
  }

  @Override
  public int getResultSetConcurrency() throws SQLException {
    return 0;
  }

  @Override
  public int getResultSetType() throws SQLException {
    return 0;
  }

  @Override
  public void addBatch(String sql) throws SQLException {}

  @Override
  public void clearBatch() throws SQLException {}

  @Override
  public int[] executeBatch() throws SQLException {
    return new int[0];
  }

  @Override
  public Connection getConnection() throws SQLException {
    return null;
  }

  @Override
  public boolean getMoreResults(int current) throws SQLException {
    return false;
  }

  @Override
  public ResultSet getGeneratedKeys() throws SQLException {
    return null;
  }

  @Override
  public int executeUpdate(String sql, int autoGeneratedKeys) throws SQLException {
    return 0;
  }

  @Override
  public int executeUpdate(String sql, int[] columnIndexes) throws SQLException {
    return 0;
  }

  @Override
  public int executeUpdate(String sql, String[] columnNames) throws SQLException {
    return 0;
  }

  @Override
  public boolean execute(String sql, int autoGeneratedKeys) throws SQLException {
    return false;
  }

  @Override
  public boolean execute(String sql, int[] columnIndexes) throws SQLException {
    return false;
  }

  @Override
  public boolean execute(String sql, String[] columnNames) throws SQLException {
    return false;
  }

  @Override
  public int getResultSetHoldability() throws SQLException {
    return 0;
  }

  @Override
  public boolean isClosed() throws SQLException {
    return false;
  }

  @Override
  public void setPoolable(boolean poolable) throws SQLException {}

  @Override
  public boolean isPoolable() throws SQLException {
    return false;
  }

  @Override
  public void closeOnCompletion() throws SQLException {}

  @Override
  public boolean isCloseOnCompletion() throws SQLException {
    return false;
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
