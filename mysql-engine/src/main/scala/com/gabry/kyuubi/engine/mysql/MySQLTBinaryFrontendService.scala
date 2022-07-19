package com.gabry.kyuubi.engine.mysql

import org.apache.kyuubi.service.{Serverable, Service, TBinaryFrontendService}

class MySQLTBinaryFrontendService (override val serverable: Serverable)
  extends TBinaryFrontendService("MySQLTBinaryFrontend") {
  override val discoveryService: Option[Service] = None
}
