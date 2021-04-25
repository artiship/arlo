package io.github.artiship.arlo.dbconn;

import java.sql.Connection;

public interface DbConnInterface {
    Connection conn(DataSourceEnum dataSourceEnum, String ip, String port, String database, String username, String password);
}
