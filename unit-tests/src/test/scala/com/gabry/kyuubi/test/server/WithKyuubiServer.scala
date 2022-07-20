package com.gabry.kyuubi.test.server

import com.gabry.kyuubi.server.KyuubiServer
import org.apache.kyuubi.Logging
import org.apache.thrift.server.TServer
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach}

trait WithKyuubiServer extends AnyFunSuite with BeforeAndAfterAll
  with BeforeAndAfterEach with Logging {
  protected var server: TServer = _
  private val port = 10009
  private val host = "localhost"
  private var serverThread:Thread = _

  override def beforeAll(): Unit = {
    server = KyuubiServer.startSimpleServer(host,port).get
    serverThread = new Thread(() => {
      server.serve()
    })
    serverThread.start()
    logger.info(s"Kyuubi server $server is running at 10009 port")
  }

  override def afterAll(): Unit = {
    server.stop()
    serverThread.interrupt()
    logger.info("Kyuubi server stopped")
  }

  protected def getConnectionUrl: String = s"$host:${port}"

}
