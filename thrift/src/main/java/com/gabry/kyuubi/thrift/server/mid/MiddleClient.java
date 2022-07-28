package com.gabry.kyuubi.thrift.server.mid;

import com.gabry.kyuubi.thrift.service.DemoService;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.layered.TFramedTransport;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MiddleClient {
  private static final Logger logger = LoggerFactory.getLogger(MiddleClient.class);

  public static void main(String[] args) throws Exception {
    String remoteHost = "127.0.0.1";
    int remotePort = 9090;
    TSocket socket = new TSocket("127.0.0.1", 9090);
    socket.setTimeout(10*60*1000);
    TTransport transport = new TFramedTransport(socket);
    TProtocol protocol = new TBinaryProtocol(transport);
    transport.open();
    logger.info("connected to remote server at {}:{}", remoteHost, remotePort);

    DemoService.Client client = new DemoService.Client.Factory().getClient(protocol);
    String result = client.sayHi("ITer_ZC");
    logger.info("say hi result {}", result);
  }
}
