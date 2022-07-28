package com.gabry.kyuubi.thrift.server.mid;

import java.nio.ByteBuffer;
import org.apache.thrift.ProcessFunction;
import org.apache.thrift.TApplicationException;
import org.apache.thrift.TException;
import org.apache.thrift.TProcessor;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TMessage;
import org.apache.thrift.protocol.TMessageType;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.protocol.TProtocolUtil;
import org.apache.thrift.protocol.TType;
import org.apache.thrift.transport.TByteBuffer;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransportException;
import org.apache.thrift.transport.layered.TFramedTransport;

public class TProcessorProxy implements TProcessor {
  private final TProcessor underlyingProcessor;
  private final String remoteHost;
  private final int remotePort;
  private TFramedTransport middleTransport;
  private TBinaryProtocol middleProtocol;
  public TProcessorProxy(TProcessor underlyingProcessor, String remoteHost,int remotePort) {
    this.underlyingProcessor = underlyingProcessor;
    this.remoteHost = remoteHost;
    this.remotePort = remotePort;
    init(remoteHost, remotePort);

  }
  private void init(String remoteHost,int remotePort){
    try {
    TSocket socket = new TSocket(remoteHost, remotePort);
    socket.setTimeout(10*60*1000);
      middleTransport = new TFramedTransport(socket);
      middleProtocol = new TBinaryProtocol(middleTransport);
      middleTransport.open();
    } catch (TTransportException e) {
      throw new RuntimeException(e);
    }
  }

  private boolean forward(TMessage tMessage) {
    return true;
  }

  private void forward(byte[] data) throws TTransportException {
    // 1. 把data发送给remote
    // 2. 接收remote的消息，并处理，得知道远程返回了多少消息
    middleTransport.write(data,0,data.length);
    middleTransport.flush();
  }
  static class ForwardProcessor extends
  @Override
  public void process(TProtocol in, TProtocol out) throws TException {
    if (in instanceof TBinaryProtocolProxy) {
      TBinaryProtocolProxy protocolProxy = (TBinaryProtocolProxy) in;
      TMessage tMessage = protocolProxy.readMessageBegin();
      System.out.println("receive message " + tMessage + ", forward as needed");
      byte[] data = protocolProxy.clone(tMessage);
      if (forward(tMessage)) {
        forward(data);
        // 覆盖这个实现，不应该用service的函数来处理
        underlyingProcessor.process(middleProtocol, out);
        ProcessFunction
//        TMessage msg = in.readMessageBegin();
//        ProcessFunction fn = processMap.get(msg.name);
//        if (fn == null) {
//          TProtocolUtil.skip(in, TType.STRUCT);
//          in.readMessageEnd();
//          TApplicationException x = new TApplicationException(TApplicationException.UNKNOWN_METHOD, "Invalid method name: '"+msg.name+"'");
//          out.writeMessageBegin(new TMessage(msg.name, TMessageType.EXCEPTION, msg.seqid));
//          x.write(out);
//          out.writeMessageEnd();
//          out.getTransport().flush();
//        } else {
//          fn.process(msg.seqid, in, out, iface);
//        }
      } else {
        underlyingProcessor.process(
            new TBinaryProtocol(new TByteBuffer(ByteBuffer.wrap(data))), out);
      }
    }else{
      underlyingProcessor.process(in, out);
    }
  }
}
