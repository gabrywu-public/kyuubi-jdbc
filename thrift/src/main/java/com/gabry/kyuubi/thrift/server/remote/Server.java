package com.gabry.kyuubi.thrift.server.remote;

import com.gabry.kyuubi.thrift.server.mid.MiddleServer;
import com.gabry.kyuubi.thrift.service.DemoService;
import com.gabry.kyuubi.thrift.service.impl.DemoServiceImpl;
import org.apache.thrift.TProcessor;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.server.TNonblockingServer;
import org.apache.thrift.server.TServer;
import org.apache.thrift.transport.TNonblockingServerSocket;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Server {
  private static final Logger logger = LoggerFactory.getLogger(Server.class);
  public static void main(String[] args){
    TNonblockingServerSocket socket;
    try {
      int localPort = 19090;
      socket = new TNonblockingServerSocket(localPort);
      TNonblockingServer.Args options = new TNonblockingServer.Args(socket);
      TProcessor processor = new DemoService.Processor<DemoService.Iface>(new DemoServiceImpl("remote"));
      options.processor(processor);
      options.protocolFactory(new TBinaryProtocol.Factory());
      TServer server = new TNonblockingServer(options);
      logger.info("Remote Server is running at {} port", localPort);
      server.serve();
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }
}
