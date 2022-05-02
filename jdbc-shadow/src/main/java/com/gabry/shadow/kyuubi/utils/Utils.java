package com.gabry.shadow.kyuubi.utils;

import org.apache.hive.service.cli.HiveSQLException;
import org.apache.hive.service.rpc.thrift.TStatus;
import org.apache.hive.service.rpc.thrift.TStatusCode;

public class Utils {
  private Utils() {
    // do nothing
  }

  public static void cleanup(AutoCloseable closeable) {
    try {
      closeable.close();
    } catch (Exception ignore) {
    }
  }

  public static void throwIfFail(TStatus status) throws HiveSQLException {
    if (status.getStatusCode() != TStatusCode.SUCCESS_STATUS) {
      throw new HiveSQLException(status);
    }
  }
}
