package com.gabry.kyuubi.jdbc;

import com.gabry.kyuubi.utils.Utils;
import com.gabry.kyuubi.driver.DriverUtils;
import com.gabry.kyuubi.driver.KyuubiDriver;
import org.apache.hive.service.cli.GetInfoType;
import org.apache.hive.service.rpc.thrift.*;
import org.apache.thrift.TException;

import java.sql.*;
import java.util.Arrays;
import java.util.jar.Attributes;

public class KyuubiDatabaseMetaData implements DatabaseMetaData {
  private Attributes manifestAttributes = null;
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
  public boolean allProceduresAreCallable() throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  @Override
  public boolean allTablesAreSelectable() throws SQLException {
    return true;
  }

  @Override
  public String getURL() throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  @Override
  public String getUserName() throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  @Override
  public boolean isReadOnly() throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  @Override
  public boolean nullsAreSortedHigh() throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  @Override
  public boolean nullsAreSortedLow() throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  @Override
  public boolean nullsAreSortedAtStart() throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  @Override
  public boolean nullsAreSortedAtEnd() throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
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
    if (dbVersion != null) { // lazy-caching of the version.
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
  public boolean usesLocalFiles() throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  @Override
  public boolean usesLocalFilePerTable() throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  @Override
  public boolean supportsMixedCaseIdentifiers() throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  @Override
  public boolean storesUpperCaseIdentifiers() throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  @Override
  public boolean storesLowerCaseIdentifiers() throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  @Override
  public boolean storesMixedCaseIdentifiers() throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  @Override
  public boolean supportsMixedCaseQuotedIdentifiers() throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  @Override
  public boolean storesUpperCaseQuotedIdentifiers() throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  @Override
  public boolean storesLowerCaseQuotedIdentifiers() throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  @Override
  public boolean storesMixedCaseQuotedIdentifiers() throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  @Override
  public String getIdentifierQuoteString() throws SQLException {
    return " ";
  }

  @Override
  public String getSQLKeywords() throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  @Override
  public String getNumericFunctions() throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  @Override
  public String getStringFunctions() throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  @Override
  public String getSystemFunctions() throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  @Override
  public String getTimeDateFunctions() throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
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
  public boolean supportsAlterTableWithAddColumn() throws SQLException {
    return true;
  }

  @Override
  public boolean supportsAlterTableWithDropColumn() throws SQLException {
    return false;
  }

  @Override
  public boolean supportsColumnAliasing() throws SQLException {
    return true;
  }

  @Override
  public boolean nullPlusNonNullIsNull() throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  @Override
  public boolean supportsConvert() throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  @Override
  public boolean supportsConvert(int fromType, int toType) throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  @Override
  public boolean supportsTableCorrelationNames() throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  @Override
  public boolean supportsDifferentTableCorrelationNames() throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  @Override
  public boolean supportsExpressionsInOrderBy() throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  @Override
  public boolean supportsOrderByUnrelated() throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  @Override
  public boolean supportsGroupBy() throws SQLException {
    return true;
  }

  @Override
  public boolean supportsGroupByUnrelated() throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  @Override
  public boolean supportsGroupByBeyondSelect() throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  @Override
  public boolean supportsLikeEscapeClause() throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  @Override
  public boolean supportsMultipleResultSets() throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  @Override
  public boolean supportsMultipleTransactions() throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  @Override
  public boolean supportsNonNullableColumns() throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  @Override
  public boolean supportsMinimumSQLGrammar() throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  @Override
  public boolean supportsCoreSQLGrammar() throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  @Override
  public boolean supportsExtendedSQLGrammar() throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  @Override
  public boolean supportsANSI92EntryLevelSQL() throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  @Override
  public boolean supportsANSI92IntermediateSQL() throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  @Override
  public boolean supportsANSI92FullSQL() throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  @Override
  public boolean supportsIntegrityEnhancementFacility() throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  @Override
  public boolean supportsOuterJoins() throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  @Override
  public boolean supportsFullOuterJoins() throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  @Override
  public boolean supportsLimitedOuterJoins() throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
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
  public boolean isCatalogAtStart() throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  @Override
  public String getCatalogSeparator() throws SQLException {
    return ".";
  }

  @Override
  public boolean supportsSchemasInDataManipulation() throws SQLException {
    return true;
  }

  @Override
  public boolean supportsSchemasInProcedureCalls() throws SQLException {
    return false;
  }

  @Override
  public boolean supportsSchemasInTableDefinitions() throws SQLException {
    return false;
  }

  @Override
  public boolean supportsSchemasInIndexDefinitions() throws SQLException {
    return false;
  }

  @Override
  public boolean supportsSchemasInPrivilegeDefinitions() throws SQLException {
    return false;
  }

  @Override
  public boolean supportsCatalogsInDataManipulation() throws SQLException {
    return false;
  }

  @Override
  public boolean supportsCatalogsInProcedureCalls() throws SQLException {
    return false;
  }

  @Override
  public boolean supportsCatalogsInTableDefinitions() throws SQLException {
    return false;
  }

  @Override
  public boolean supportsCatalogsInIndexDefinitions() throws SQLException {
    return false;
  }

  @Override
  public boolean supportsCatalogsInPrivilegeDefinitions() throws SQLException {
    return false;
  }

  @Override
  public boolean supportsPositionedDelete() throws SQLException {
    return false;
  }

  @Override
  public boolean supportsPositionedUpdate() throws SQLException {
    return false;
  }

  @Override
  public boolean supportsSelectForUpdate() throws SQLException {
    return false;
  }

  @Override
  public boolean supportsStoredProcedures() throws SQLException {
    return false;
  }

  @Override
  public boolean supportsSubqueriesInComparisons() throws SQLException {
    return false;
  }

  @Override
  public boolean supportsSubqueriesInExists() throws SQLException {
    return false;
  }

  @Override
  public boolean supportsSubqueriesInIns() throws SQLException {
    return false;
  }

  @Override
  public boolean supportsSubqueriesInQuantifieds() throws SQLException {
    return false;
  }

  @Override
  public boolean supportsCorrelatedSubqueries() throws SQLException {
    return false;
  }

  @Override
  public boolean supportsUnion() throws SQLException {
    return false;
  }

  @Override
  public boolean supportsUnionAll() throws SQLException {
    return false;
  }

  @Override
  public boolean supportsOpenCursorsAcrossCommit() throws SQLException {
    return false;
  }

  @Override
  public boolean supportsOpenCursorsAcrossRollback() throws SQLException {
    return false;
  }

  @Override
  public boolean supportsOpenStatementsAcrossCommit() throws SQLException {
    return false;
  }

  @Override
  public boolean supportsOpenStatementsAcrossRollback() throws SQLException {
    return false;
  }

  @Override
  public int getMaxBinaryLiteralLength() throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  @Override
  public int getMaxCharLiteralLength() throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  @Override
  public int getMaxColumnNameLength() throws SQLException {
    return 128;
  }

  @Override
  public int getMaxColumnsInGroupBy() throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  @Override
  public int getMaxColumnsInIndex() throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  @Override
  public int getMaxColumnsInOrderBy() throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  @Override
  public int getMaxColumnsInSelect() throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  @Override
  public int getMaxColumnsInTable() throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  @Override
  public int getMaxConnections() throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  @Override
  public int getMaxCursorNameLength() throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  @Override
  public int getMaxIndexLength() throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  @Override
  public int getMaxSchemaNameLength() throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  @Override
  public int getMaxProcedureNameLength() throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  @Override
  public int getMaxCatalogNameLength() throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  @Override
  public int getMaxRowSize() throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  @Override
  public boolean doesMaxRowSizeIncludeBlobs() throws SQLException {
    return false;
  }

  @Override
  public int getMaxStatementLength() throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  @Override
  public int getMaxStatements() throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  @Override
  public int getMaxTableNameLength() throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  @Override
  public int getMaxTablesInSelect() throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  @Override
  public int getMaxUserNameLength() throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  @Override
  public int getDefaultTransactionIsolation() throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  @Override
  public boolean supportsTransactions() throws SQLException {
    return false;
  }

  @Override
  public boolean supportsTransactionIsolationLevel(int level) throws SQLException {
    return false;
  }

  @Override
  public boolean supportsDataDefinitionAndDataManipulationTransactions() throws SQLException {
    return false;
  }

  @Override
  public boolean supportsDataManipulationTransactionsOnly() throws SQLException {
    return false;
  }

  @Override
  public boolean dataDefinitionCausesTransactionCommit() throws SQLException {
    return false;
  }

  @Override
  public boolean dataDefinitionIgnoredInTransactions() throws SQLException {
    return false;
  }

  @Override
  public ResultSet getProcedures(String catalog, String schemaPattern, String procedureNamePattern)
      throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  @Override
  public ResultSet getProcedureColumns(
      String catalog, String schemaPattern, String procedureNamePattern, String columnNamePattern)
      throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
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
          .getResultSet();
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
      return catalogStatement.getResultSet();
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
      return catalogStatement.getResultSet();
    } catch (TException e) {
      throw new SQLException(e.getMessage(), e);
    }
  }

  @Override
  public ResultSet getColumnPrivileges(
      String catalog, String schema, String table, String columnNamePattern) throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  @Override
  public ResultSet getTablePrivileges(String catalog, String schemaPattern, String tableNamePattern)
      throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  @Override
  public ResultSet getBestRowIdentifier(
      String catalog, String schema, String table, int scope, boolean nullable)
      throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  @Override
  public ResultSet getVersionColumns(String catalog, String schema, String table)
      throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
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
      return catalogStatement.getResultSet();
    } catch (TException e) {
      throw new SQLException(e.getMessage(), e);
    }
  }

  @Override
  public ResultSet getImportedKeys(String catalog, String schema, String table)
      throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  @Override
  public ResultSet getExportedKeys(String catalog, String schema, String table)
      throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
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
      return catalogStatement.getResultSet();
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
      return catalogStatement.getResultSet();
    } catch (TException e) {
      throw new SQLException(e.getMessage(), "08S01", e);
    }
  }

  @Override
  public ResultSet getIndexInfo(
      String catalog, String schema, String table, boolean unique, boolean approximate)
      throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  @Override
  public boolean supportsResultSetType(int type) throws SQLException {
    return true;
  }

  @Override
  public boolean supportsResultSetConcurrency(int type, int concurrency) throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  @Override
  public boolean ownUpdatesAreVisible(int type) throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  @Override
  public boolean ownDeletesAreVisible(int type) throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  @Override
  public boolean ownInsertsAreVisible(int type) throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  @Override
  public boolean othersUpdatesAreVisible(int type) throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  @Override
  public boolean othersDeletesAreVisible(int type) throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  @Override
  public boolean othersInsertsAreVisible(int type) throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  @Override
  public boolean updatesAreDetected(int type) throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  @Override
  public boolean deletesAreDetected(int type) throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  @Override
  public boolean insertsAreDetected(int type) throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  @Override
  public boolean supportsBatchUpdates() throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  @Override
  public ResultSet getUDTs(
      String catalog, String schemaPattern, String typeNamePattern, int[] types)
      throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  @Override
  public Connection getConnection() throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  @Override
  public boolean supportsSavepoints() throws SQLException {
    return false;
  }

  @Override
  public boolean supportsNamedParameters() throws SQLException {
    return false;
  }

  @Override
  public boolean supportsMultipleOpenResults() throws SQLException {
    return false;
  }

  @Override
  public boolean supportsGetGeneratedKeys() throws SQLException {
    return false;
  }

  @Override
  public ResultSet getSuperTypes(String catalog, String schemaPattern, String typeNamePattern)
      throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  @Override
  public ResultSet getSuperTables(String catalog, String schemaPattern, String tableNamePattern)
      throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  @Override
  public ResultSet getAttributes(
      String catalog, String schemaPattern, String typeNamePattern, String attributeNamePattern)
      throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  @Override
  public boolean supportsResultSetHoldability(int holdability) throws SQLException {
    return false;
  }

  @Override
  public int getResultSetHoldability() throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
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
  public boolean locatorsUpdateCopy() throws SQLException {
    return false;
  }

  @Override
  public boolean supportsStatementPooling() throws SQLException {
    return false;
  }

  @Override
  public RowIdLifetime getRowIdLifetime() throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
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
      return catalogStatement.getResultSet();
    } catch (TException e) {
      throw new SQLException(e.getMessage(), "08S01", e);
    }
  }

  @Override
  public boolean supportsStoredFunctionsUsingCallSyntax() throws SQLException {
    return false;
  }

  @Override
  public boolean autoCommitFailureClosesAllResultSets() throws SQLException {
    return false;
  }

  @Override
  public ResultSet getClientInfoProperties() throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
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
      return catalogStatement.getResultSet();
    } catch (TException e) {
      throw new SQLException(e.getMessage(), "08S01", e);
    }
  }

  @Override
  public ResultSet getFunctionColumns(
      String catalog, String schemaPattern, String functionNamePattern, String columnNamePattern)
      throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  @Override
  public ResultSet getPseudoColumns(
      String catalog, String schemaPattern, String tableNamePattern, String columnNamePattern)
      throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  @Override
  public boolean generatedKeyAlwaysReturned() throws SQLException {
    return false;
  }

  @Override
  public <T> T unwrap(Class<T> iface) throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  @Override
  public boolean isWrapperFor(Class<?> iface) throws SQLException {
    return false;
  }
}
