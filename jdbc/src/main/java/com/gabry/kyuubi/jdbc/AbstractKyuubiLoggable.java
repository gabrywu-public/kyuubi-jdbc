package com.gabry.kyuubi.jdbc;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

public abstract class AbstractKyuubiLoggable {
  private static final Logger logger = LoggerFactory.getLogger(AbstractKyuubiLoggable.class);
  private KyuubiResultSet logsResultSet = null;

  protected void setLogsResultSet(KyuubiResultSet resultSet) {
    this.logsResultSet = resultSet;
  }

  protected boolean hasMoreLogs() throws SQLException {
    return logsResultSet != null && logsResultSet.next();
  }

  protected List<String> getExecLog() throws SQLException {
    List<String> logs = new ArrayList<>(logsResultSet.getFetchSize());
    boolean hasLogs = true;
    for (int i = 0; i < logsResultSet.getFetchSize() && hasLogs; i++) {
      logs.add(logsResultSet.getString(1));
      hasLogs = hasMoreLogs();
    }
    return logs;
  }

  protected void closeLog() throws SQLException {
    if (logsResultSet != null) {
      logsResultSet.close();
    }
  }
}
