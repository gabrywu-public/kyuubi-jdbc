package com.gabry.kyuubi.thrift.server.mid;

import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.transport.TTransport;

public class TBinaryProtocolWrapper extends TBinaryProtocol {

  public TBinaryProtocolWrapper(TTransport trans) {
    super(trans);
  }

  public TBinaryProtocolWrapper(TTransport trans, boolean strictRead, boolean strictWrite) {
    super(trans, strictRead, strictWrite);
  }

  public TBinaryProtocolWrapper(
      TTransport trans, long stringLengthLimit, long containerLengthLimit) {
    super(trans, stringLengthLimit, containerLengthLimit);
  }

  public TBinaryProtocolWrapper(
      TTransport trans,
      long stringLengthLimit,
      long containerLengthLimit,
      boolean strictRead,
      boolean strictWrite) {
    super(trans, stringLengthLimit, containerLengthLimit, strictRead, strictWrite);
  }

  public boolean getStrictWrite() {
    return strictWrite_;
  }
}
