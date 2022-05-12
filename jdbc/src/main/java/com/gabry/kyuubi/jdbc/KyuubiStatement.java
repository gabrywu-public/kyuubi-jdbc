package com.gabry.kyuubi.jdbc;

import com.gabry.kyuubi.utils.Utils;
import org.apache.hive.service.cli.FetchType;
import org.apache.hive.service.rpc.thrift.*;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLTimeoutException;
import java.util.Collections;
import java.util.Map;

public class KyuubiStatement extends AbstractKyuubiStatement {
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
  private KyuubiResultSet currentResultSet = null;
  private final FetchType fetchType;
  private boolean isCancelled;
  private boolean isClosed;

  public static KyuubiStatement createStatementForOperation(
      KyuubiConnection connection,
      TCLIService.Iface client,
      TSessionHandle sessionHandle,
      TOperationHandle operationHandle,
      int fetchSize,
      FetchType fetchType) {
    return new KyuubiStatement(
        connection, client, sessionHandle, operationHandle, fetchSize, fetchType);
  }

  public static KyuubiStatement createStatementForOperation(
      KyuubiConnection connection,
      TCLIService.Iface client,
      TSessionHandle sessionHandle,
      TOperationHandle operationHandle) {
    return createStatementForOperation(
        connection,
        client,
        sessionHandle,
        operationHandle,
        KyuubiStatement.DEFAULT_FETCH_SIZE,
        FetchType.QUERY_OUTPUT);
  }

  private KyuubiStatement(
      KyuubiConnection connection,
      TCLIService.Iface client,
      TSessionHandle sessionHandle,
      TOperationHandle operationHandle,
      int fetchSize,
      FetchType fetchType) {
    this(connection, client, sessionHandle, fetchSize, fetchType);
    this.currentOperationHandle = operationHandle;
  }

  public KyuubiStatement(
      KyuubiConnection connection,
      TCLIService.Iface client,
      TSessionHandle sessionHandle,
      int fetchSize) {
    this(connection, client, sessionHandle, fetchSize, FetchType.QUERY_OUTPUT);
  }

  public KyuubiStatement(
      KyuubiConnection connection,
      TCLIService.Iface client,
      TSessionHandle sessionHandle,
      int fetchSize,
      FetchType fetchType) {
    this.boundConnection = connection;
    this.boundClient = client;
    this.boundSessionHandle = sessionHandle;
    this.fetchSize = fetchSize;
    this.fetchType = fetchType;
    this.isCancelled = false;
    this.isClosed = false;
  }

  public KyuubiResultSet executeOperation() throws SQLException {
    logger.info(
        "execute current operation handle {}, fetch type {}", currentOperationHandle, fetchType);
    return executeOperation(
        this, boundClient, currentOperationHandle, boundSessionHandle, fetchType);
  }

  private static KyuubiResultSet executeOperation(
      KyuubiStatement statement,
      TCLIService.Iface client,
      TOperationHandle operationHandle,
      TSessionHandle boundSessionHandle,
      FetchType fetchType)
      throws SQLException {
    if (operationHandle == null) {
      throw new SQLException("there is not a running SQL or operation");
    }
    return KyuubiResultSet.create(
        statement, client, operationHandle, boundSessionHandle, fetchType);
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
      throws SQLException, TException {
    if (operationHandle != null) {
      TCloseOperationReq closeReq = new TCloseOperationReq(operationHandle);
      TCloseOperationResp closeResp = client.CloseOperation(closeReq);
      Utils.throwIfFail(closeResp.getStatus());
    }
  }

  public static void cancelOperation(TCLIService.Iface client, TOperationHandle operationHandle)
      throws SQLException, TException {
    if (operationHandle != null) {
      TCancelOperationReq closeReq = new TCancelOperationReq(operationHandle);
      TCancelOperationResp closeResp = client.CancelOperation(closeReq);
      Utils.throwIfFail(closeResp.getStatus());
    }
  }

  @Override
  public void close() throws SQLException {
    if (!isClosed) {
      isClosed = true;
      closeOperation();
      if (currentResultSet != null) {
        currentResultSet.close();
        currentResultSet = null;
      }
    }
  }

  public void closeOperation() throws SQLException {
    try {
      closeOperation(boundClient, currentOperationHandle);
    } catch (TException e) {
      throw new SQLException(e);
    }
    currentOperationHandle = null;
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
  public int getQueryTimeout() throws SQLException {
    return queryTimeout;
  }

  @Override
  public void setQueryTimeout(int seconds) throws SQLException {
    this.queryTimeout = seconds;
  }

  @Override
  public void cancel() throws SQLException {
    if (!isCancelled) {
      isCancelled = true;
      try {
        cancelOperation(boundClient, currentOperationHandle);
      } catch (TException e) {
        throw new SQLException(e);
      } finally {
        currentOperationHandle = null;
      }
    }
  }

  private void reInitState() throws SQLException {
    closeOperation();
    isCancelled = false;
    isClosed = false;
  }

  private TOperationHandle runAsyncOnServer(String sql, Map<String, String> conf)
      throws TException, SQLException {
    reInitState();
    TExecuteStatementReq execReq = new TExecuteStatementReq(boundSessionHandle, sql);
    execReq.setRunAsync(true);

    execReq.setConfOverlay(conf != null ? conf : Collections.emptyMap());
    execReq.setQueryTimeout(queryTimeout);

    TExecuteStatementResp execResp = boundClient.ExecuteStatement(execReq);
    Utils.throwIfFail(execResp.getStatus());
    return execResp.getOperationHandle();
  }

  public void waitForOperationToComplete() throws SQLException {
    waitForOperationToComplete(currentOperationHandle);
  }

  private TGetOperationStatusResp waitForOperationToComplete(TOperationHandle operationHandle)
      throws SQLException {
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
    try {
      currentOperationHandle = runAsyncOnServer(sql, confOverlay);
      TGetOperationStatusResp statusResp = waitForOperationToComplete(currentOperationHandle);

      setLogsResultSet(
          KyuubiResultSet.create(
              this, boundClient, currentOperationHandle, boundSessionHandle, FetchType.LOG));

      currentResultSet =
          !statusResp.isHasResultSet() && !currentOperationHandle.isHasResultSet()
              ? null
              : KyuubiResultSet.create(
                  this, boundClient, currentOperationHandle, boundSessionHandle, fetchType);
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
  public int getFetchDirection() throws SQLException {
    return ResultSet.FETCH_FORWARD;
  }

  @Override
  public void setFetchSize(int fetchSize) throws SQLException {
    if (fetchSize > 0) {
      this.fetchSize = fetchSize;
    } else if (fetchSize == 0) {
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
  public int getResultSetType() throws SQLException {
    return ResultSet.TYPE_FORWARD_ONLY;
  }

  @Override
  public Connection getConnection() {
    return boundConnection;
  }

  @Override
  public boolean isClosed() throws SQLException {
    return isClosed;
  }

  @Override
  public boolean isPoolable() throws SQLException {
    return false;
  }

  @Override
  public boolean isCloseOnCompletion() throws SQLException {
    return false;
  }
}
