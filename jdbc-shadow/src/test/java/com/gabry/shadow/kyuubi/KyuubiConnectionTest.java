package com.gabry.shadow.kyuubi;

import com.gabry.shadow.kyuubi.driver.KyuubiDriver;
import com.gabry.shadow.kyuubi.jdbc.KyuubiConnection;
import org.junit.Assert;
import org.junit.Test;

import java.sql.SQLException;

public class KyuubiConnectionTest {
    @Test
    public void testConnect() throws SQLException {
        KyuubiDriver kyuubiDriver = new KyuubiDriver();
        try(KyuubiConnection connection = (KyuubiConnection) kyuubiDriver.connect("jdbc:kyuubi://192.168.3.3:10009/default",null)){
            Assert.assertFalse(connection.isClosed());
        }
    }
}
