package com.gabry.kyuubi.jdbc;

import java.sql.SQLException;
import java.util.List;

public interface IKyuubiLoggable {
    boolean hasMoreLogs();
    List<String> getExecLog() throws SQLException, ClosedOrCancelledException;
}
