package com.gabry.kyuubi.jdbc;

import java.util.Arrays;

public enum KyuubiAuthEnum {
  AUTH_TOKEN("delegationToken"),
  AUTH_QOP("saslQop"),
  AUTH_SIMPLE("noSasl"),
  UNKNOWN("unknown");
  private final String authName;

  KyuubiAuthEnum(String authName) {
    this.authName = authName;
  }

  public String getAuthName() {
    return authName;
  }

  public static KyuubiAuthEnum from(String authName) {
    return Arrays.stream(KyuubiAuthEnum.values())
        .filter(e -> e.getAuthName().equalsIgnoreCase(authName))
        .findFirst()
        .orElse(KyuubiAuthEnum.UNKNOWN);
  }
}
