package com.gabry.shadow.jdbc.driver.exception;

import java.sql.SQLException;

public class JdbcUrlParseException extends SQLException {
  public JdbcUrlParseException(String message) {
    super(message);
  }
}
