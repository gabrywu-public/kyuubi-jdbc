package com.gabry.kyuubi.jdbc;

import com.gabry.kyuubi.driver.DriverUtils;
import com.gabry.kyuubi.driver.KyuubiDriver;
import com.gabry.kyuubi.utils.Utils;
import org.apache.hive.service.cli.GetInfoType;
import org.apache.hive.service.rpc.thrift.*;
import org.apache.thrift.TException;

import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Arrays;

public class KyuubiDatabaseMetaData extends AbstractKyuubiDatabaseMetaData {
  private final KyuubiConnection boundConnection;
  private final TCLIService.Iface boundClient;
  private final TSessionHandle boundSessionHandle;

  private String dbVersion;
  private String dbProductName;

  public KyuubiDatabaseMetaData(
      KyuubiConnection connection, TCLIService.Iface client, TSessionHandle sessHandle) {
    this.boundConnection = connection;
    this.boundClient = client;
    this.boundSessionHandle = sessHandle;
  }

  @Override
  public String getURL() throws SQLException {
    return boundConnection.getJdbcURL();
  }

  @Override
  public String getUserName() throws SQLException {
    return boundConnection.getUserName();
  }

  private TGetInfoResp getServerInfo(TGetInfoType type) throws SQLException {
    try {
      TGetInfoReq req = new TGetInfoReq(boundSessionHandle, type);
      TGetInfoResp resp = boundClient.GetInfo(req);
      Utils.throwIfFail(resp.getStatus());
      return resp;
    } catch (TException e) {
      throw new SQLException(e);
    }
  }

  @Override
  public String getDatabaseProductName() throws SQLException {
    if (dbProductName == null) {
      TGetInfoResp resp = getServerInfo(GetInfoType.CLI_DBMS_NAME.toTGetInfoType());
      dbProductName = resp.getInfoValue().getStringValue();
    }
    return dbProductName;
  }

  @Override
  public String getDatabaseProductVersion() throws SQLException {
    if (dbVersion == null) { // lazy-caching of the version.
      TGetInfoResp resp = getServerInfo(GetInfoType.CLI_DBMS_VER.toTGetInfoType());
      this.dbVersion = resp.getInfoValue().getStringValue();
    }
    return dbVersion;
  }

  @Override
  public String getDriverName() throws SQLException {
    return KyuubiDriver.getDriverName();
  }

  @Override
  public String getDriverVersion() throws SQLException {
    return KyuubiDriver.getDriverManifestVersionAttribute();
  }

  @Override
  public int getDriverMajorVersion() {
    return KyuubiDriver.getVersionAt(0);
  }

  @Override
  public int getDriverMinorVersion() {
    return KyuubiDriver.getVersionAt(1);
  }

  @Override
  public String getIdentifierQuoteString() throws SQLException {
    return "`";
  }

  @Override
  public String getSearchStringEscape() throws SQLException {
    return "\\";
  }

  @Override
  public String getExtraNameCharacters() throws SQLException {
    return "";
  }

  @Override
  public String getSchemaTerm() throws SQLException {
    return "database";
  }

  @Override
  public String getProcedureTerm() throws SQLException {
    return "UDF";
  }

  @Override
  public String getCatalogTerm() throws SQLException {
    return "instance";
  }

  @Override
  public String getCatalogSeparator() throws SQLException {
    return ".";
  }

  @Override
  public int getMaxColumnNameLength() throws SQLException {
    return 128;
  }

  @Override
  public ResultSet getTables(
      String catalog, String schemaPattern, String tableNamePattern, String[] types)
      throws SQLException {
    TGetTablesReq getTableReq = new TGetTablesReq(boundSessionHandle);
    getTableReq.setCatalogName(catalog);
    getTableReq.setSchemaName(schemaPattern == null ? "%" : schemaPattern);
    getTableReq.setTableName(tableNamePattern);
    if (types != null) {
      getTableReq.setTableTypes(Arrays.asList(types));
    }
    try {
      TGetTablesResp getTableResp = boundClient.GetTables(getTableReq);
      Utils.throwIfFail(getTableResp.getStatus());
      return KyuubiStatement.createStatementForOperation(
              boundConnection, boundClient, boundSessionHandle, getTableResp.getOperationHandle())
          .executeOperation();
    } catch (TException rethrow) {
      throw new SQLException(rethrow.getMessage(), "08S01", rethrow);
    }
  }

  @Override
  public ResultSet getSchemas() throws SQLException {
    return getSchemas(null, null);
  }

  @Override
  public ResultSet getCatalogs() throws SQLException {
    try {
      TGetCatalogsResp catalogResp =
          boundClient.GetCatalogs(new TGetCatalogsReq(boundSessionHandle));
      Utils.throwIfFail(catalogResp.getStatus());
      KyuubiStatement catalogStatement =
          KyuubiStatement.createStatementForOperation(
              boundConnection, boundClient, boundSessionHandle, catalogResp.getOperationHandle());
      return catalogStatement.executeOperation();
    } catch (TException e) {
      throw new SQLException(e.getMessage(), e);
    }
  }

  @Override
  public ResultSet getTableTypes() throws SQLException {
    try {
      TGetTableTypesResp catalogResp =
          boundClient.GetTableTypes(new TGetTableTypesReq(boundSessionHandle));
      Utils.throwIfFail(catalogResp.getStatus());
      KyuubiStatement catalogStatement =
          KyuubiStatement.createStatementForOperation(
              boundConnection, boundClient, boundSessionHandle, catalogResp.getOperationHandle());
      return catalogStatement.executeOperation();
    } catch (TException e) {
      throw new SQLException(e.getMessage(), e);
    }
  }

  @Override
  public ResultSet getColumns(
      String catalog, String schemaPattern, String tableNamePattern, String columnNamePattern)
      throws SQLException {
    try {
      TGetColumnsReq colReq = new TGetColumnsReq();
      colReq.setSessionHandle(boundSessionHandle);
      colReq.setCatalogName(catalog);
      colReq.setSchemaName(schemaPattern);
      colReq.setTableName(tableNamePattern);
      colReq.setColumnName(columnNamePattern);
      TGetColumnsResp catalogResp = boundClient.GetColumns(colReq);
      Utils.throwIfFail(catalogResp.getStatus());
      KyuubiStatement catalogStatement =
          KyuubiStatement.createStatementForOperation(
              boundConnection, boundClient, boundSessionHandle, catalogResp.getOperationHandle());
      return catalogStatement.executeOperation();
    } catch (TException e) {
      throw new SQLException(e.getMessage(), e);
    }
  }

  @Override
  public ResultSet getPrimaryKeys(String catalog, String schema, String table) throws SQLException {
    try {
      TGetPrimaryKeysReq getPKReq = new TGetPrimaryKeysReq(boundSessionHandle);
      getPKReq.setTableName(table);
      getPKReq.setSchemaName(schema);
      getPKReq.setCatalogName(catalog);
      TGetPrimaryKeysResp catalogResp = boundClient.GetPrimaryKeys(getPKReq);
      Utils.throwIfFail(catalogResp.getStatus());
      KyuubiStatement catalogStatement =
          KyuubiStatement.createStatementForOperation(
              boundConnection, boundClient, boundSessionHandle, catalogResp.getOperationHandle());
      return catalogStatement.executeOperation();
    } catch (TException e) {
      throw new SQLException(e.getMessage(), e);
    }
  }

  @Override
  public ResultSet getCrossReference(
      String parentCatalog,
      String parentSchema,
      String parentTable,
      String foreignCatalog,
      String foreignSchema,
      String foreignTable)
      throws SQLException {
    try {
      TGetCrossReferenceReq getFKReq = new TGetCrossReferenceReq(boundSessionHandle);
      getFKReq.setParentTableName(parentTable);
      getFKReq.setParentSchemaName(parentSchema);
      getFKReq.setParentCatalogName(parentCatalog);
      getFKReq.setForeignTableName(foreignTable);
      getFKReq.setForeignSchemaName(foreignSchema);
      getFKReq.setForeignCatalogName(foreignCatalog);
      TGetCrossReferenceResp getFKResp = boundClient.GetCrossReference(getFKReq);
      Utils.throwIfFail(getFKResp.getStatus());
      KyuubiStatement catalogStatement =
          KyuubiStatement.createStatementForOperation(
              boundConnection, boundClient, boundSessionHandle, getFKResp.getOperationHandle());
      return catalogStatement.executeOperation();
    } catch (TException e) {
      throw new SQLException(e.getMessage(), "08S01", e);
    }
  }

  @Override
  public ResultSet getTypeInfo() throws SQLException {
    try {
      TGetTypeInfoResp getFKResp = boundClient.GetTypeInfo(new TGetTypeInfoReq(boundSessionHandle));
      Utils.throwIfFail(getFKResp.getStatus());
      KyuubiStatement catalogStatement =
          KyuubiStatement.createStatementForOperation(
              boundConnection, boundClient, boundSessionHandle, getFKResp.getOperationHandle());
      return catalogStatement.executeOperation();
    } catch (TException e) {
      throw new SQLException(e.getMessage(), "08S01", e);
    }
  }

  @Override
  public int getDatabaseMajorVersion() throws SQLException {
    return DriverUtils.getDatabaseVersionAt(getDatabaseProductVersion(), 0);
  }

  @Override
  public int getDatabaseMinorVersion() throws SQLException {
    return DriverUtils.getDatabaseVersionAt(getDatabaseProductVersion(), 1);
  }

  @Override
  public int getJDBCMajorVersion() throws SQLException {
    return 3;
  }

  @Override
  public int getJDBCMinorVersion() throws SQLException {
    return 0;
  }

  @Override
  public int getSQLStateType() throws SQLException {
    return DatabaseMetaData.sqlStateSQL99;
  }

  @Override
  public ResultSet getSchemas(String catalog, String schemaPattern) throws SQLException {
    try {
      TGetSchemasReq schemaReq = new TGetSchemasReq(boundSessionHandle);
      if (catalog != null) {
        schemaReq.setCatalogName(catalog);
      }
      if (schemaPattern == null) {
        schemaPattern = "%";
      }
      schemaReq.setSchemaName(schemaPattern);
      TGetSchemasResp schemaResp = boundClient.GetSchemas(schemaReq);
      Utils.throwIfFail(schemaResp.getStatus());
      KyuubiStatement catalogStatement =
          KyuubiStatement.createStatementForOperation(
              boundConnection, boundClient, boundSessionHandle, schemaResp.getOperationHandle());
      return catalogStatement.executeOperation();
    } catch (TException e) {
      throw new SQLException(e.getMessage(), "08S01", e);
    }
  }

  @Override
  public ResultSet getFunctions(String catalog, String schemaPattern, String functionNamePattern)
      throws SQLException {
    try {
      TGetFunctionsReq getFunctionsReq =
          new TGetFunctionsReq(boundSessionHandle, functionNamePattern);
      getFunctionsReq.setCatalogName(catalog);
      getFunctionsReq.setSchemaName(schemaPattern);
      TGetFunctionsResp funcResp = boundClient.GetFunctions(getFunctionsReq);
      Utils.throwIfFail(funcResp.getStatus());
      KyuubiStatement catalogStatement =
          KyuubiStatement.createStatementForOperation(
              boundConnection, boundClient, boundSessionHandle, funcResp.getOperationHandle());
      return catalogStatement.executeOperation();
    } catch (TException e) {
      throw new SQLException(e.getMessage(), "08S01", e);
    }
  }
}
