package com.gabry.kyuubi.jdbc;

import java.sql.SQLException;

public class ClosedOrCancelledException extends SQLException {

    private static final long serialVersionUID = 0;

    public ClosedOrCancelledException(String msg) {
        super(msg);
    }
}
