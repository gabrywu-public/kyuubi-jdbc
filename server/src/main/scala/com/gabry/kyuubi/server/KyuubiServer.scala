package com.gabry.kyuubi.server

import com.gabry.kyuubi.server.service.KyuubiCLIService
import org.apache.hive.service.rpc.thrift.TCLIService
import org.apache.thrift.protocol.{TBinaryProtocol, TCompactProtocol}
import org.apache.thrift.server.{TNonblockingServer, TServer, TSimpleServer}
import org.apache.thrift.transport.{TNonblockingServerSocket, TServerSocket}

import java.net.{InetAddress, ServerSocket}
import scala.util.Try

object KyuubiServer {
  def startNonblockingServer(port: Int): Try[TNonblockingServer] = Try {
    val socket = new TNonblockingServerSocket(port)
    val options = new TNonblockingServer.Args(socket)
    val processor = new TCLIService.Processor[TCLIService.Iface](new KyuubiCLIService)
    options.processor(processor)
    options.protocolFactory(new TBinaryProtocol.Factory)
    new TNonblockingServer(options)
  }

  def startSimpleServer(host:String, port: Int): Try[TSimpleServer] = Try {
    val socket = new TServerSocket(new ServerSocket(port, -1, InetAddress.getByName(host)))
    val processor = new TCLIService.Processor[TCLIService.Iface](new KyuubiCLIService)
    val options = new TServer.Args(socket)
      .processor(processor)
      .protocolFactory(new TBinaryProtocol.Factory)
    new TSimpleServer(options)
  }

  def main(args: Array[String]): Unit = {
    KyuubiServer.startNonblockingServer(9099).foreach { server =>
      println(s"KyuubiServer[$server] is running at 9090 port")
      server.serve()
    }
  }
}
