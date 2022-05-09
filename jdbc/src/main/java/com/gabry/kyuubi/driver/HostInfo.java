package com.gabry.kyuubi.driver;

public class HostInfo {
  public static final HostInfo[] empty = new HostInfo[0];
  private final String host;
  private final Integer port;

  public HostInfo(String host, Integer port) {
    this.host = host;
    this.port = port;
  }

  public HostInfo(String host) {
    this(host, null);
  }

  public String getHost() {
    return host;
  }

  public Integer getPort() {
    return port;
  }

  @Override
  public String toString() {
    return "Host{" + "host='" + host + '\'' + ", port=" + port + '}';
  }
}
