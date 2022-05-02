package com.gabry.shadow.kyuubi.driver.exception;

import java.sql.SQLException;

public class JdbcUrlParseException extends SQLException {
  public JdbcUrlParseException(String message) {
    super(message);
  }
}
