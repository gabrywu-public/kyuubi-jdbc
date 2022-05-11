package com.gabry.kyuubi.jdbc;

import com.gabry.kyuubi.common.Common;
import com.gabry.kyuubi.driver.ConnectionInfo;
import com.gabry.kyuubi.driver.HostInfo;
import com.gabry.kyuubi.utils.Utils;
import org.apache.hadoop.hive.common.auth.HiveAuthUtils;
import org.apache.hive.service.cli.FetchType;
import org.apache.hive.service.cli.HiveSQLException;
import org.apache.hive.service.rpc.thrift.*;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.nio.ByteBuffer;
import java.sql.*;
import java.util.*;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;
import java.util.stream.Collectors;

public class KyuubiConnection implements java.sql.Connection, IKyuubiLoggable {
  private static final Logger logger = LoggerFactory.getLogger(KyuubiConnection.class);
  private static final List<TProtocolVersion> supportedProtocols = new ArrayList<>(11);

  static {
    supportedProtocols.add(TProtocolVersion.HIVE_CLI_SERVICE_PROTOCOL_V1);
    supportedProtocols.add(TProtocolVersion.HIVE_CLI_SERVICE_PROTOCOL_V2);
    supportedProtocols.add(TProtocolVersion.HIVE_CLI_SERVICE_PROTOCOL_V3);
    supportedProtocols.add(TProtocolVersion.HIVE_CLI_SERVICE_PROTOCOL_V4);
    supportedProtocols.add(TProtocolVersion.HIVE_CLI_SERVICE_PROTOCOL_V5);
    supportedProtocols.add(TProtocolVersion.HIVE_CLI_SERVICE_PROTOCOL_V6);
    supportedProtocols.add(TProtocolVersion.HIVE_CLI_SERVICE_PROTOCOL_V7);
    supportedProtocols.add(TProtocolVersion.HIVE_CLI_SERVICE_PROTOCOL_V8);
    supportedProtocols.add(TProtocolVersion.HIVE_CLI_SERVICE_PROTOCOL_V9);
    supportedProtocols.add(TProtocolVersion.HIVE_CLI_SERVICE_PROTOCOL_V10);
    supportedProtocols.add(TProtocolVersion.HIVE_CLI_SERVICE_PROTOCOL_V11);
  }

  public static final String BEELINE_MODE_PROPERTY = "BEELINE_MODE";

  private final ConnectionInfo connectionInfo;
  private final int fetchSize;
  private final int loginTimeout;
  private TTransport transport;
  private TCLIService.Iface cliClient;
  private TProtocolVersion serverProtocolVersion;
  private TSessionHandle sessionHandle;
  private KyuubiStatement execLogStatement;
  private KyuubiResultSet execLogResultSet;
  private HostInfo connectedHost;
  private SQLWarning warningChain = null;
  private final Properties clientInfo;
  private Thread engineLogThread;
  private boolean stopping;
  private boolean isBeeLineMode;

  public KyuubiConnection(ConnectionInfo connectionInfo) {
    this.connectionInfo = connectionInfo;
    this.stopping = false;
    long timeOut = TimeUnit.SECONDS.toMillis(DriverManager.getLoginTimeout());
    if (timeOut > Integer.MAX_VALUE) {
      loginTimeout = Integer.MAX_VALUE;
    } else {
      loginTimeout = (int) timeOut;
    }
    fetchSize =
        connectionInfo.getSessionConfigs().containsKey("fetchSize")
            ? Integer.parseInt(connectionInfo.getSessionConfigs().get("fetchSize"))
            : KyuubiStatement.DEFAULT_FETCH_SIZE;
    clientInfo = new Properties();
    isBeeLineMode =
        Boolean.parseBoolean(connectionInfo.getSessionConfigs().get(BEELINE_MODE_PROPERTY));
  }

  private void checkOpen() throws SQLException {
    if (isClosed()) {
      throw new SQLException("Can't create Statement, connection is closed");
    }
  }

  @Override
  public Statement createStatement() throws SQLException {
    checkOpen();
    return new KyuubiStatement(this, cliClient, sessionHandle, fetchSize);
  }

  @Override
  public PreparedStatement prepareStatement(String sql) throws SQLException {
    return new KyuubiPreparedStatement(this, cliClient, sessionHandle, sql);
  }

  @Override
  public CallableStatement prepareCall(String sql) throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  @Override
  public String nativeSQL(String sql) throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  @Override
  public void setAutoCommit(boolean autoCommit) throws SQLException {
    if (!autoCommit) {
      logger.warn("Request to set autoCommit to false; Hive does not support autoCommit=false.");
      SQLWarning warning = new SQLWarning("Hive does not support autoCommit=false");
      if (warningChain == null) warningChain = warning;
      else warningChain.setNextWarning(warning);
    }
  }

  @Override
  public boolean getAutoCommit() throws SQLException {
    return true;
  }

  @Override
  public void commit() throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  @Override
  public void rollback() throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  private HostInfo getServerHost() {
    return connectionInfo.getHosts()[0];
  }

  private TTransport createUnderlyingTransport(HostInfo hostInfo) {
    return HiveAuthUtils.getSocketTransport(hostInfo.getHost(), hostInfo.getPort(), loginTimeout);
  }

  @Override
  public boolean hasMoreLogs() {
    boolean hasLogs = false;
    try {
      hasLogs = execLogResultSet.next();
    } catch (SQLException e) {
      logger.error("can't get engine log {}", e.getMessage(), e);
    }
    return hasLogs;
  }

  @Override
  public List<String> getExecLog() throws SQLException {
    return Collections.singletonList(execLogResultSet.getString(1));
  }

  private static class Tupple2<T1, T2> {
    public final T1 first;
    public final T2 second;

    public Tupple2(T1 first, T2 second) {
      this.first = first;
      this.second = second;
    }
  }

  private Tupple2<TTransport, HostInfo> createAndOpenBinaryTransport() throws TTransportException {
    Tupple2<TTransport, HostInfo> result = null;
    for (HostInfo hostInfo : connectionInfo.getHosts()) {
      logger.info("trying to connect to {}", hostInfo);
      TTransport socketTransport = createUnderlyingTransport(hostInfo);
      // KerberosSaslHelper.getKerberosTransport
      if (!socketTransport.isOpen()) {
        try {
          socketTransport.open();
          result = new Tupple2<>(socketTransport, hostInfo);
          break;
        } catch (TTransportException e) {
          logger.error("can't connect to {} because of {}", hostInfo, e.getMessage(), e);
          Utils.cleanup(socketTransport);
        }
      }
    }
    if (result == null) {
      throw new TTransportException(
          "can't open to all of the hosts " + Arrays.toString(connectionInfo.getHosts()));
    } else {
      return result;
    }
  }

  private static class SynchronizedHandler implements InvocationHandler {
    private final TCLIService.Iface client;
    private final ReentrantLock lock = new ReentrantLock(true);

    SynchronizedHandler(TCLIService.Iface client) {
      this.client = client;
    }

    @Override
    public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
      try {
        lock.lock();
        return method.invoke(client, args);
      } catch (InvocationTargetException e) {
        // all IFace APIs throw TException
        if (e.getTargetException() instanceof TException) {
          throw (TException) e.getTargetException();
        } else {
          // should not happen
          throw new TException(
              "Error in calling method " + method.getName(), e.getTargetException());
        }
      } catch (Exception e) {
        throw new TException("Error in calling method " + method.getName(), e);
      } finally {
        lock.unlock();
      }
    }
  }

  public static TCLIService.Iface newSynchronizedClient(TCLIService.Iface client) {
    return (TCLIService.Iface)
        Proxy.newProxyInstance(
            KyuubiConnection.class.getClassLoader(),
            new Class[] {TCLIService.Iface.class},
            new SynchronizedHandler(client));
  }

  public KyuubiConnection open() throws SQLException {
    try {
      Tupple2<TTransport, HostInfo> result = createAndOpenBinaryTransport();
      transport = result.first;
      connectedHost = result.second;
      logger.info("connected to {}:{}", connectedHost.getHost(), connectedHost.getPort());
      TCLIService.Iface underlyingClient = new TCLIService.Client(new TBinaryProtocol(transport));
      cliClient = newSynchronizedClient(underlyingClient);
      try {
        OpenedSessionInfo sessionResult = openSession();
        serverProtocolVersion = sessionResult.getProtocolVersion();

        if (!isBeeLineMode) {
          execLogStatement =
              KyuubiStatement.createStatementForOperation(
                  this,
                  cliClient,
                  sessionHandle,
                  sessionResult.getLaunchEngineOperationHandle(),
                  FetchType.LOG);
          execLogResultSet = execLogStatement.executeOperation();
          printLaunchEngineLogInBackend();
          execLogStatement.waitForOperationToComplete();
        }

        sessionHandle = sessionResult.getSessionHandle();
      } catch (TException e) {
        throw new SQLException("can't open session " + e.getMessage(), e);
      }
      logger.info("open session {} to {} successfully", sessionHandle, connectedHost);
    } catch (TTransportException e) {
      throw new SQLException(e);
    }
    return this;
  }

  private void printLaunchEngineLogInBackend() {
    logger.info("Starting to get launch engine log.");
    engineLogThread =
        new Thread("print-engine-launch-log") {
          @Override
          public void run() {
            while (!stopping && hasMoreLogs()) {
              try {
                getExecLog().forEach(logger::info);
                Thread.sleep(500);
              } catch (Exception e) {
                logger.warn("failed to get engine log {}", e.getMessage(), e);
              }
            }
            logger.info("Finished to get launch engine log.");
          }
        };
    engineLogThread.start();
  }

  public TProtocolVersion getProtocolVersion() {
    return serverProtocolVersion;
  }

  private static class OpenedSessionInfo {
    private TSessionHandle sessionHandle;
    private TProtocolVersion protocolVersion;
    private Map<String, String> returnedConf;
    private TOperationHandle launchEngineOperationHandle;

    private OpenedSessionInfo(
        TSessionHandle tSessionHandle,
        TProtocolVersion protocolVersion,
        Map<String, String> returnedConf) {
      this.sessionHandle = tSessionHandle;
      this.protocolVersion = protocolVersion;
      this.returnedConf = returnedConf;
      String launchEngineOpHandleGuid = returnedConf.get(Common.KyuubiGuidKey());
      String launchEngineOpHandleSecret = returnedConf.get(Common.KyuubiSecretKey());

      if (launchEngineOpHandleGuid != null && launchEngineOpHandleSecret != null) {
        byte[] guidBytes = Base64.getMimeDecoder().decode(launchEngineOpHandleGuid);
        byte[] secretBytes = Base64.getMimeDecoder().decode(launchEngineOpHandleSecret);
        THandleIdentifier handleIdentifier =
            new THandleIdentifier(ByteBuffer.wrap(guidBytes), ByteBuffer.wrap(secretBytes));
        launchEngineOperationHandle =
            new TOperationHandle(handleIdentifier, TOperationType.UNKNOWN, true);
      }
    }

    public TSessionHandle getSessionHandle() {
      return sessionHandle;
    }

    public TProtocolVersion getProtocolVersion() {
      return protocolVersion;
    }

    public Map<String, String> getReturnedConf() {
      return returnedConf;
    }

    public TOperationHandle getLaunchEngineOperationHandle() {
      return launchEngineOperationHandle;
    }
  }

  private OpenedSessionInfo openSession() throws TException, HiveSQLException {
    TOpenSessionReq openReq = new TOpenSessionReq();
    TOpenSessionResp openResp = cliClient.OpenSession(openReq);
    Utils.throwIfFail(openResp.getStatus());
    if (!supportedProtocols.contains(openResp.getServerProtocolVersion())) {
      throw new TException("Unsupported Hive2 protocol");
    }
    return new OpenedSessionInfo(
        openResp.getSessionHandle(),
        openResp.getServerProtocolVersion(),
        openResp.getConfiguration());
  }

  @Override
  public void close() throws SQLException {
    stopping = true;
    if (cliClient != null) {
      TCloseSessionReq closeReq = new TCloseSessionReq(sessionHandle);
      try {
        TCloseSessionResp resp = cliClient.CloseSession(closeReq);
        logger.info("disconnect to {} successfully {}", connectedHost, resp);
      } catch (TException e) {
        throw new SQLException("fail to cleanup server resources: " + e.getMessage(), e);
      } finally {
        Utils.cleanup(transport);
        sessionHandle = null;
        cliClient = null;
        transport = null;
        connectedHost = null;
      }
    }
  }

  @Override
  public boolean isClosed() throws SQLException {
    return cliClient == null;
  }

  @Override
  public DatabaseMetaData getMetaData() throws SQLException {
    return new KyuubiDatabaseMetaData(this, cliClient, sessionHandle);
  }

  @Override
  public void setReadOnly(boolean readOnly) throws SQLException {
    throw new SQLException("Enabling read-only mode not supported");
  }

  @Override
  public boolean isReadOnly() throws SQLException {
    return false;
  }

  @Override
  public void setCatalog(String catalog) throws SQLException {}

  @Override
  public String getCatalog() throws SQLException {
    return "";
  }

  @Override
  public void setTransactionIsolation(int level) throws SQLException {}

  @Override
  public int getTransactionIsolation() throws SQLException {
    return Connection.TRANSACTION_NONE;
  }

  @Override
  public SQLWarning getWarnings() throws SQLException {
    return warningChain;
  }

  @Override
  public void clearWarnings() throws SQLException {
    warningChain = null;
  }

  @Override
  public Statement createStatement(int resultSetType, int resultSetConcurrency)
      throws SQLException {
    if (resultSetConcurrency != ResultSet.CONCUR_READ_ONLY) {
      throw new SQLException(
          "Statement with resultset concurrency " + resultSetConcurrency + " is not supported",
          "HYC00"); // Optional feature not implemented
    }
    if (resultSetType == ResultSet.TYPE_SCROLL_SENSITIVE) {
      throw new SQLException(
          "Statement with resultset type " + resultSetType + " is not supported",
          "HYC00"); // Optional feature not implemented
    }
    return new KyuubiStatement(this, cliClient, sessionHandle, fetchSize);
  }

  @Override
  public PreparedStatement prepareStatement(String sql, int resultSetType, int resultSetConcurrency)
      throws SQLException {
    return new KyuubiPreparedStatement(this, cliClient, sessionHandle, sql);
  }

  @Override
  public CallableStatement prepareCall(String sql, int resultSetType, int resultSetConcurrency)
      throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  @Override
  public Map<String, Class<?>> getTypeMap() throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  @Override
  public void setTypeMap(Map<String, Class<?>> map) throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  @Override
  public void setHoldability(int holdability) throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  @Override
  public int getHoldability() throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  @Override
  public Savepoint setSavepoint() throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  @Override
  public Savepoint setSavepoint(String name) throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  @Override
  public void rollback(Savepoint savepoint) throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  @Override
  public void releaseSavepoint(Savepoint savepoint) throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  @Override
  public Statement createStatement(
      int resultSetType, int resultSetConcurrency, int resultSetHoldability) throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  @Override
  public PreparedStatement prepareStatement(
      String sql, int resultSetType, int resultSetConcurrency, int resultSetHoldability)
      throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  @Override
  public CallableStatement prepareCall(
      String sql, int resultSetType, int resultSetConcurrency, int resultSetHoldability)
      throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  @Override
  public PreparedStatement prepareStatement(String sql, int autoGeneratedKeys) throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  @Override
  public PreparedStatement prepareStatement(String sql, int[] columnIndexes) throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  @Override
  public PreparedStatement prepareStatement(String sql, String[] columnNames) throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  @Override
  public Clob createClob() throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  @Override
  public Blob createBlob() throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  @Override
  public NClob createNClob() throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  @Override
  public SQLXML createSQLXML() throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  @Override
  public boolean isValid(int timeout) throws SQLException {
    boolean rc = false;
    try {
      String productName =
          new KyuubiDatabaseMetaData(this, cliClient, sessionHandle).getDatabaseProductName();
      logger.debug("current connection is valid {}", productName);
      rc = true;
    } catch (SQLException ignore) {
    }
    return rc;
  }

  @Override
  public void setClientInfo(String name, String value) throws SQLClientInfoException {
    clientInfo.put(name, value);
    updateClientInfo();
  }

  private void updateClientInfo() throws SQLClientInfoException {
    try {
      TSetClientInfoReq req = new TSetClientInfoReq(sessionHandle);
      Map<String, String> clientInfoMap =
          clientInfo.entrySet().stream()
              .collect(Collectors.toMap(x -> x.getKey().toString(), x -> x.getValue().toString()));

      req.setConfiguration(clientInfoMap);
      TSetClientInfoResp openResp = cliClient.SetClientInfo(req);
      Utils.throwIfFail(openResp.getStatus());
    } catch (TException | SQLException e) {
      logger.error("fail to update client info", e);
      throw new SQLClientInfoException("Error setting client info", null, e);
    }
  }

  @Override
  public void setClientInfo(Properties properties) throws SQLClientInfoException {
    clientInfo.clear();
    clientInfo.putAll(properties);
    updateClientInfo();
  }

  @Override
  public String getClientInfo(String name) throws SQLException {
    return clientInfo.getProperty(name);
  }

  @Override
  public Properties getClientInfo() throws SQLException {
    return clientInfo;
  }

  @Override
  public Array createArrayOf(String typeName, Object[] elements) throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  @Override
  public Struct createStruct(String typeName, Object[] attributes) throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  @Override
  public void setSchema(String schema) throws SQLException {
    try (Statement stmt = createStatement()) {
      stmt.execute("use " + schema);
    }
  }

  @Override
  public String getSchema() throws SQLException {
    try (Statement stmt = createStatement();
        ResultSet res = stmt.executeQuery("SELECT current_database()")) {
      if (!res.next()) {
        throw new SQLException("Failed to get schema information");
      }
      return res.getString(1);
    }
  }

  @Override
  public void abort(Executor executor) throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  @Override
  public void setNetworkTimeout(Executor executor, int milliseconds) throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  @Override
  public int getNetworkTimeout() throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
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
