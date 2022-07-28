package com.gabry.kyuubi.thrift.server.mid;

import com.gabry.kyuubi.thrift.service.DemoService;
import com.gabry.kyuubi.thrift.service.impl.DemoServiceImpl;
import org.apache.thrift.TProcessorFactory;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.server.TServer;
import org.apache.thrift.server.TThreadPoolServer;
import org.apache.thrift.transport.TServerSocket;
import org.apache.thrift.transport.TTransportFactory;
import org.apache.thrift.transport.layered.TFramedTransport;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MiddleServer {
  private static final Logger logger = LoggerFactory.getLogger(MiddleServer.class);
  public static void main(String[] args) {
    TServerSocket serverSocket;
    try {
      int localPort = 9090;
      String remoteHost = "127.0.0.1";
      int remotePort = 19090;
      serverSocket =  new TServerSocket(localPort);
      TThreadPoolServer.Args sargs = new TThreadPoolServer.Args(serverSocket)
          .transportFactory(new TTransportFactory())
          .processorFactory(new TProcessorFactory(new TProcessorProxy(
              new DemoService.Processor<DemoService.Iface>(
                  new DemoServiceImpl("middle")),
              remoteHost,remotePort)))
          .inputProtocolFactory(new TBinaryProtocolProxy.Factory(new TBinaryProtocol.Factory()))
          .outputProtocolFactory(new TBinaryProtocolProxy.Factory(new TBinaryProtocol.Factory(),true));

      TServer server = new TThreadPoolServer(sargs);
      logger.info("middle server is running at {}, remote port is {}:{}",localPort,remoteHost, remotePort);
      server.serve();
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }
}
