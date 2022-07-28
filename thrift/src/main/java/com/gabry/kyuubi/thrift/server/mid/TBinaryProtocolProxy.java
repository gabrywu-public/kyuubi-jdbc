package com.gabry.kyuubi.thrift.server.mid;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TMessage;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.protocol.TProtocolDecorator;
import org.apache.thrift.protocol.TProtocolFactory;
import org.apache.thrift.transport.TByteBuffer;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;
import org.apache.thrift.transport.layered.TFramedTransport;

public class TBinaryProtocolProxy extends TProtocolDecorator {
  private int bodyLen = -1;
  private int messageLen = -1;
  private final TBinaryProtocolWrapper protocol;
  /**
   * Encloses the specified protocol.
   *
   * @param protocol All operations will be forward to this protocol. Must be non-null.
   */
  public TBinaryProtocolProxy(TBinaryProtocolWrapper protocol) {
    super(protocol);
    this.protocol = protocol;
    bodyLen = -1;
  }

  private int getTMessageSize(TMessage tMessage) {
    return tMessage.name.getBytes(StandardCharsets.UTF_8).length
        + 4
        + (protocol.getStrictWrite() ? 8 : 5);
  }

  @Override
  public TMessage readMessageBegin() throws TException {
    this.bodyLen = super.readI32();
    TMessage tMessage = super.readMessageBegin();
    this.messageLen = getTMessageSize(tMessage);
    return tMessage;
  }

  public int getBodyLen() {
    return bodyLen;
  }

  public int getMessageLen() {
    return messageLen;
  }

  public byte[] clone(TMessage message) throws TException {
    if (bodyLen < 1) {
      throw new TException("you should all of data before you call this function");
    }
    byte[] buf = new byte[bodyLen];
    TProtocol tProtocol = new TBinaryProtocol(new TByteBuffer(ByteBuffer.wrap(buf)));
    tProtocol.writeMessageBegin(message);
    readRemainBytes(buf, messageLen);
    tProtocol.writeMessageEnd();
    return buf;
  }

  private void readRemainBytes(byte[] buf, int start) throws TTransportException {
    int remainSize = bodyLen - messageLen;
    trans_.checkReadBytesAvailable(remainSize);

    if (trans_.getBytesRemainingInBuffer() >= remainSize) {
      System.arraycopy(trans_.getBuffer(), trans_.getBufferPosition(), buf, start, remainSize);
      trans_.consumeBuffer(remainSize);
    }

    trans_.readAll(buf, start, remainSize);
  }

  public static class Factory implements TProtocolFactory {
    private final TProtocolFactory underlyingProtocolFactory;
    private final boolean framed;

    public Factory(TProtocolFactory underlyingProtocolFactory, boolean framed) {
      this.underlyingProtocolFactory = underlyingProtocolFactory;
      this.framed = framed;
    }
    public Factory(TProtocolFactory underlyingProtocolFactory) {
      this(underlyingProtocolFactory,false);
    }
    @Override
    public TProtocol getProtocol(TTransport trans) {
      try {
        return new TBinaryProtocolProxy(
            new TBinaryProtocolWrapper(framed?
        new TFramedTransport(underlyingProtocolFactory.getProtocol(trans).getTransport())
                :underlyingProtocolFactory.getProtocol(trans).getTransport())
        );
      } catch (TTransportException e) {
        throw new RuntimeException(e);
      }
    }
  }
}
