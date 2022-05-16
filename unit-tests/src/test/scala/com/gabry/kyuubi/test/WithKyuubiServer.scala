package com.gabry.kyuubi.test

import org.apache.kyuubi.Utils
import org.apache.kyuubi.config.KyuubiConf
import org.apache.kyuubi.config.KyuubiConf._
import org.apache.kyuubi.engine.spark.SparkSQLEngine
import org.apache.kyuubi.ha.HighAvailabilityConf.{HA_ZK_AUTH_TYPE, HA_ZK_QUORUM}
import org.apache.kyuubi.ha.client.AuthTypes
import org.apache.kyuubi.metrics.MetricsConf.METRICS_ENABLED
import org.apache.kyuubi.server.KyuubiServer
import org.apache.kyuubi.zookeeper.{EmbeddedZookeeper, ZookeeperConf}
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach}

import java.util.Properties

trait WithKyuubiServer extends AnyFunSuite with BeforeAndAfterAll
  with BeforeAndAfterEach {
  protected val conf: KyuubiConf
  protected var server: KyuubiServer = _
  private var zkServer: EmbeddedZookeeper = _

  override def beforeAll(): Unit = {
    val rootDir = classOf[WithKyuubiServer].getResource("/").toURI.getPath
    val globalConf = new Properties()
    globalConf.load(classOf[WithKyuubiServer].getResourceAsStream("/global.properties"))
    val engineJarPath = Utils.getCodeSourceLocation(classOf[SparkSQLEngine])
    conf.set("kyuubi.session.engine.spark.main.resource", engineJarPath)
    conf.set(KYUUBI_ENGINE_ENV_PREFIX + ".SPARK_HOME", rootDir + "../" + globalConf.getProperty("spark.home"))
    conf.set(KYUUBI_ENGINE_ENV_PREFIX + ".KYUUBI_WORK_DIR_ROOT", rootDir + "../logs")

    conf.set(FRONTEND_PROTOCOLS, Seq("THRIFT_BINARY"))
    conf.set(SERVER_OPERATION_LOG_DIR_ROOT, rootDir + "server-operation-logs")
    conf.set(METRICS_ENABLED, false)
    conf.set(AUTHENTICATION_METHOD, Seq("NOSASL"))
    conf.set(FRONTEND_BIND_HOST, "localhost")

    zkServer = new EmbeddedZookeeper()
    conf.set(ZookeeperConf.ZK_CLIENT_PORT, 0)
    conf.set(ZookeeperConf.ZK_CLIENT_PORT_ADDRESS, "localhost")
    val zkData = Utils.createTempDir()
    conf.set(ZookeeperConf.ZK_DATA_DIR, zkData.toString)
    zkServer.initialize(conf)
    zkServer.start()
    conf.set(HA_ZK_QUORUM, zkServer.getConnectString)
    conf.set(HA_ZK_AUTH_TYPE, AuthTypes.NONE.toString)

    conf.set("spark.ui.enabled", "false")
    conf.setIfMissing("spark.sql.catalogImplementation", "in-memory")
    conf.setIfMissing(ENGINE_CHECK_INTERVAL, 1000L)
    conf.setIfMissing(ENGINE_IDLE_TIMEOUT, 5000L)
    server = KyuubiServer.startServer(conf)
  }

  override def afterAll(): Unit = {

    if (server != null) {
      server.stop()
      server = null
    }

    if (zkServer != null) {
      zkServer.stop()
      zkServer = null
    }
    super.afterAll()
  }

  protected def getConnectionUrl: String = server.frontendServices.head.connectionUrl

}
