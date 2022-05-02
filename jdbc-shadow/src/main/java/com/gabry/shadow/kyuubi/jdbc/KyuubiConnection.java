package com.gabry.shadow.kyuubi.jdbc;

import com.gabry.shadow.kyuubi.common.Common;
import com.gabry.shadow.kyuubi.driver.ConnectionInfo;
import com.gabry.shadow.kyuubi.driver.HostInfo;
import com.gabry.shadow.kyuubi.utils.Utils;
import org.apache.hadoop.hive.common.auth.HiveAuthUtils;
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
import java.sql.*;
import java.util.*;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;

public class KyuubiConnection implements java.sql.Connection {
  private static final Logger logger = LoggerFactory.getLogger(KyuubiConnection.class);
  private static final List<TProtocolVersion> supportedProtocols = new ArrayList<>(10);

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
  }

  private final ConnectionInfo connectionInfo;
  private final int fetchSize;
  private int loginTimeout;
  private TTransport transport;
  private TCLIService.Iface cliClient;
  private TProtocolVersion serverProtocolVersion;
  private TSessionHandle sessionHandle;
  private HostInfo connectedHost;

  public KyuubiConnection(ConnectionInfo connectionInfo) {
    this.connectionInfo = connectionInfo;
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
    return null;
  }

  @Override
  public CallableStatement prepareCall(String sql) throws SQLException {
    return null;
  }

  @Override
  public String nativeSQL(String sql) throws SQLException {
    return null;
  }

  @Override
  public void setAutoCommit(boolean autoCommit) throws SQLException {}

  @Override
  public boolean getAutoCommit() throws SQLException {
    return false;
  }

  @Override
  public void commit() throws SQLException {}

  @Override
  public void rollback() throws SQLException {}

  private HostInfo getServerHost() {
    return connectionInfo.getHosts()[0];
  }

  private TTransport createUnderlyingTransport(HostInfo hostInfo) {
    return HiveAuthUtils.getSocketTransport(hostInfo.getHost(), hostInfo.getPort(), loginTimeout);
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
        Tupple2<TSessionHandle, TProtocolVersion> sessionResult = openSession();
        sessionHandle = sessionResult.first;
        serverProtocolVersion = sessionResult.second;
      } catch (TException e) {
        throw new SQLException("can't open session " + e.getMessage(), e);
      }
      logger.info("open session {} to {} successfully", sessionHandle, connectedHost);
    } catch (TTransportException e) {
      throw new SQLException(e);
    }
    return this;
  }
  public TProtocolVersion getProtocolVersion() {
    return serverProtocolVersion;
  }

  private Tupple2<TSessionHandle, TProtocolVersion> openSession()
      throws TException, HiveSQLException {
    TOpenSessionReq openReq = new TOpenSessionReq();
    TOpenSessionResp openResp = cliClient.OpenSession(openReq);
    Utils.throwIfFail(openResp.getStatus());
    if (!supportedProtocols.contains(openResp.getServerProtocolVersion())) {
      throw new TException("Unsupported Hive2 protocol");
    }
    TProtocolVersion serverProtocolVer = openResp.getServerProtocolVersion();
    TSessionHandle tSessionHandle = openResp.getSessionHandle();
    Map<String, String> openRespConf = openResp.getConfiguration();
    logger.debug("configuration from server {}", openRespConf);
    String launchEngineOpHandleGuid = openRespConf.get(Common.KyuubiGuidKey());
    String launchEngineOpHandleSecret = openRespConf.get(Common.KyuubiSecretKey());

    if (launchEngineOpHandleGuid != null && launchEngineOpHandleSecret != null) {
      try {
        byte[] guidBytes = Base64.getMimeDecoder().decode(launchEngineOpHandleGuid);
        byte[] secretBytes = Base64.getMimeDecoder().decode(launchEngineOpHandleSecret);
        logger.info("launch handle guid {}", guidBytes);
        logger.info("launch handle secret {}", secretBytes);
      } catch (Exception e) {
        logger.error("Failed to decode launch engine operation handle from open session resp", e);
      }
    }

    return new Tupple2<>(tSessionHandle, serverProtocolVer);
  }

  @Override
  public void close() throws SQLException {
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
    return null;
  }

  @Override
  public void setReadOnly(boolean readOnly) throws SQLException {}

  @Override
  public boolean isReadOnly() throws SQLException {
    return false;
  }

  @Override
  public void setCatalog(String catalog) throws SQLException {}

  @Override
  public String getCatalog() throws SQLException {
    return null;
  }

  @Override
  public void setTransactionIsolation(int level) throws SQLException {}

  @Override
  public int getTransactionIsolation() throws SQLException {
    return 0;
  }

  @Override
  public SQLWarning getWarnings() throws SQLException {
    return null;
  }

  @Override
  public void clearWarnings() throws SQLException {}

  @Override
  public Statement createStatement(int resultSetType, int resultSetConcurrency)
      throws SQLException {
    return null;
  }

  @Override
  public PreparedStatement prepareStatement(String sql, int resultSetType, int resultSetConcurrency)
      throws SQLException {
    return null;
  }

  @Override
  public CallableStatement prepareCall(String sql, int resultSetType, int resultSetConcurrency)
      throws SQLException {
    return null;
  }

  @Override
  public Map<String, Class<?>> getTypeMap() throws SQLException {
    return null;
  }

  @Override
  public void setTypeMap(Map<String, Class<?>> map) throws SQLException {}

  @Override
  public void setHoldability(int holdability) throws SQLException {}

  @Override
  public int getHoldability() throws SQLException {
    return 0;
  }

  @Override
  public Savepoint setSavepoint() throws SQLException {
    return null;
  }

  @Override
  public Savepoint setSavepoint(String name) throws SQLException {
    return null;
  }

  @Override
  public void rollback(Savepoint savepoint) throws SQLException {}

  @Override
  public void releaseSavepoint(Savepoint savepoint) throws SQLException {}

  @Override
  public Statement createStatement(
      int resultSetType, int resultSetConcurrency, int resultSetHoldability) throws SQLException {
    return null;
  }

  @Override
  public PreparedStatement prepareStatement(
      String sql, int resultSetType, int resultSetConcurrency, int resultSetHoldability)
      throws SQLException {
    return null;
  }

  @Override
  public CallableStatement prepareCall(
      String sql, int resultSetType, int resultSetConcurrency, int resultSetHoldability)
      throws SQLException {
    return null;
  }

  @Override
  public PreparedStatement prepareStatement(String sql, int autoGeneratedKeys) throws SQLException {
    return null;
  }

  @Override
  public PreparedStatement prepareStatement(String sql, int[] columnIndexes) throws SQLException {
    return null;
  }

  @Override
  public PreparedStatement prepareStatement(String sql, String[] columnNames) throws SQLException {
    return null;
  }

  @Override
  public Clob createClob() throws SQLException {
    return null;
  }

  @Override
  public Blob createBlob() throws SQLException {
    return null;
  }

  @Override
  public NClob createNClob() throws SQLException {
    return null;
  }

  @Override
  public SQLXML createSQLXML() throws SQLException {
    return null;
  }

  @Override
  public boolean isValid(int timeout) throws SQLException {
    return false;
  }

  @Override
  public void setClientInfo(String name, String value) throws SQLClientInfoException {}

  @Override
  public void setClientInfo(Properties properties) throws SQLClientInfoException {}

  @Override
  public String getClientInfo(String name) throws SQLException {
    return null;
  }

  @Override
  public Properties getClientInfo() throws SQLException {
    return null;
  }

  @Override
  public Array createArrayOf(String typeName, Object[] elements) throws SQLException {
    return null;
  }

  @Override
  public Struct createStruct(String typeName, Object[] attributes) throws SQLException {
    return null;
  }

  @Override
  public void setSchema(String schema) throws SQLException {}

  @Override
  public String getSchema() throws SQLException {
    return null;
  }

  @Override
  public void abort(Executor executor) throws SQLException {}

  @Override
  public void setNetworkTimeout(Executor executor, int milliseconds) throws SQLException {}

  @Override
  public int getNetworkTimeout() throws SQLException {
    return 0;
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
