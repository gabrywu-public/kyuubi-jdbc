package com.gabry.kyuubi.thrift.server.mid;

import com.gabry.kyuubi.thrift.UnsafeUtils;
import org.apache.thrift.TByteArrayOutputStream;
import org.apache.thrift.TConfiguration;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;
import org.apache.thrift.transport.TTransportFactory;
import org.apache.thrift.transport.layered.TFramedTransport;

public class TExtendedFramedTransport extends TFramedTransport {
  private final TFramedTransport framedTransport;
  private final TByteArrayOutputStream writeBufferProxy;
  private static final byte[] sizeFiller = new byte[] {0x00, 0x00, 0x00, 0x00};

  public TExtendedFramedTransport(TFramedTransport framedTransport)
      throws TTransportException {
    super(framedTransport.getInnerTransport());
    this.framedTransport = framedTransport;
    long writeBufferOffset = 0;
    try {
      writeBufferOffset = UnsafeUtils.getFieldOffset(TFramedTransport.class, "writeBuffer_");
    } catch (NoSuchFieldException e) {
      throw new TTransportException(e);
    }
    this.writeBufferProxy =
        (TByteArrayOutputStream) UnsafeUtils.getFieldValue(this, writeBufferOffset);
  }

  private int readMessageLen(byte[] buf) {
    return ((buf[4] & 0xff) << 24)
        | ((buf[4 + 1] & 0xff) << 16)
        | ((buf[4 + 2] & 0xff) << 8)
        | ((buf[4 + 3] & 0xff));
  }

  public void writeArgsLen(byte[] buf, int i32) {
    buf[4] = (byte) (0xff & (i32 >> 24));
    buf[4 + 1] = (byte) (0xff & (i32 >> 16));
    buf[4 + 2] = (byte) (0xff & (i32 >> 8));
    buf[4 + 3] = (byte) (0xff & (i32));
  }

  @Override
  public void flush() throws TTransportException {
    byte[] buf = writeBufferProxy.get();
    int frameLen = writeBufferProxy.len() - 4; // account for the prepended frame size
    int messageLen = readMessageLen(buf);
    int argsLen = frameLen - messageLen;
    writeArgsLen(buf, argsLen);
    writeBufferProxy.reset();
    writeBufferProxy.write(sizeFiller, 0, 4); // make room for the next frame's size data

    encodeFrameSize(frameLen, buf); // this is the frame length without the filler
    getInnerTransport()
        .write(buf, 0, frameLen + 4); // we have to write the frame size and frame data
    getInnerTransport().flush();
  }
  public static class Factory extends TTransportFactory {

    @Override
    public TTransport getTransport(TTransport base) throws TTransportException {
      return new TExtendedFramedTransport(new TFramedTransport(base));
    }
  }
}
