package com.gabry.kyuubi.driver;

import java.net.InetAddress;
import java.net.UnknownHostException;

public class HostInfo {
  public static final HostInfo[] empty = new HostInfo[0];
  private final String canonicalHost;
  private final String host;
  private final Integer port;

  public HostInfo(String host, Integer port) {
    this.host = host;
    this.port = port;
    this.canonicalHost = getCanonicalHostName(host);
  }

  public HostInfo(String host) {
    this(host, null);
  }

  public String getHost() {
    return host;
  }

  public String getCanonicalHost() {
    return canonicalHost;
  }

  public static String getCanonicalHostName(String hostName) {
    try {
      return InetAddress.getByName(hostName).getCanonicalHostName();
    } catch (UnknownHostException exception) {
      return hostName;
    }
  }

  public Integer getPort() {
    return port;
  }

  @Override
  public String toString() {
    return "HostInfo{"
        + "canonicalHost='"
        + canonicalHost
        + '\''
        + ", host='"
        + host
        + '\''
        + ", port="
        + port
        + '}';
  }
}
