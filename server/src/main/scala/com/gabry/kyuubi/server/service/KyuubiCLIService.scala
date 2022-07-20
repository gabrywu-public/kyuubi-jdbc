package com.gabry.kyuubi.server.service

import org.apache.hive.service.rpc.thrift._
import org.slf4j.LoggerFactory

class KyuubiCLIService extends TCLIService.Iface {
  override def OpenSession(req: TOpenSessionReq): TOpenSessionResp = {
    throw new UnsupportedOperationException(s"OpenSession $req")
  }

  override def CloseSession(req: TCloseSessionReq): TCloseSessionResp = {
    throw new UnsupportedOperationException(s"CloseSession $req")
  }

  override def GetInfo(req: TGetInfoReq): TGetInfoResp = {
    throw new UnsupportedOperationException(s"GetInfo $req")
  }

  override def ExecuteStatement(req: TExecuteStatementReq): TExecuteStatementResp = {
    throw new UnsupportedOperationException(s"ExecuteStatement $req")
  }

  override def GetTypeInfo(req: TGetTypeInfoReq): TGetTypeInfoResp = {
    throw new UnsupportedOperationException(s"GetTypeInfo $req")
  }

  override def GetCatalogs(req: TGetCatalogsReq): TGetCatalogsResp = {
    throw new UnsupportedOperationException(s"GetCatalogs $req")
  }

  override def GetSchemas(req: TGetSchemasReq): TGetSchemasResp = {
    throw new UnsupportedOperationException(s"GetSchemas $req")
  }

  override def GetTables(req: TGetTablesReq): TGetTablesResp = {
    throw new UnsupportedOperationException(s"GetTables $req")
  }

  override def GetTableTypes(req: TGetTableTypesReq): TGetTableTypesResp = {
    throw new UnsupportedOperationException(s"GetTableTypes $req")
  }

  override def GetColumns(req: TGetColumnsReq): TGetColumnsResp = {
    throw new UnsupportedOperationException(s"GetColumns $req")
  }

  override def GetFunctions(req: TGetFunctionsReq): TGetFunctionsResp = {
    throw new UnsupportedOperationException(s"GetFunctions $req")
  }

  override def GetPrimaryKeys(req: TGetPrimaryKeysReq): TGetPrimaryKeysResp = {
    throw new UnsupportedOperationException(s"GetPrimaryKeys $req")
  }

  override def GetCrossReference(req: TGetCrossReferenceReq): TGetCrossReferenceResp = {
    throw new UnsupportedOperationException(s"GetCrossReference $req")
  }

  override def GetOperationStatus(req: TGetOperationStatusReq): TGetOperationStatusResp = {
    throw new UnsupportedOperationException(s"GetOperationStatus $req")
  }

  override def CancelOperation(req: TCancelOperationReq): TCancelOperationResp = {
    throw new UnsupportedOperationException(s"CancelOperation $req")
  }

  override def CloseOperation(req: TCloseOperationReq): TCloseOperationResp = {
    throw new UnsupportedOperationException(s"CloseOperation $req")
  }

  override def GetResultSetMetadata(req: TGetResultSetMetadataReq): TGetResultSetMetadataResp = {
    throw new UnsupportedOperationException(s"GetResultSetMetadata $req")
  }

  override def FetchResults(req: TFetchResultsReq): TFetchResultsResp = {
    throw new UnsupportedOperationException(s"FetchResults $req")
  }

  override def GetDelegationToken(req: TGetDelegationTokenReq): TGetDelegationTokenResp = {
    throw new UnsupportedOperationException(s"GetDelegationToken $req")
  }

  override def CancelDelegationToken(req: TCancelDelegationTokenReq): TCancelDelegationTokenResp = {
    throw new UnsupportedOperationException(s"CancelDelegationToken $req")
  }

  override def RenewDelegationToken(req: TRenewDelegationTokenReq): TRenewDelegationTokenResp = {
    throw new UnsupportedOperationException(s"RenewDelegationToken $req")
  }

  override def GetQueryId(req: TGetQueryIdReq): TGetQueryIdResp = {
    throw new UnsupportedOperationException(s"GetQueryId $req")
  }

  override def SetClientInfo(req: TSetClientInfoReq): TSetClientInfoResp = {
    throw new UnsupportedOperationException(s"SetClientInfo $req")
  }
}

object KyuubiCLIService {
  val logger = LoggerFactory.getLogger(classOf[KyuubiCLIService])
}
