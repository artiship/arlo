package io.github.artiship.arlo.dbconn;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.sql.Connection;
import java.sql.DriverManager;


public abstract class DbConnAbstractor implements DbConnInterface {
    private static Log LOG = LogFactory.getLog(DbConnAbstractor.class);

    @Override
    public Connection conn(DataSourceEnum dataSourceEnum, String ip, String port, String database, String username, String password) {
        String url = String.format(dataSourceEnum.getUrl(), ip, port, database);
        try (Connection conn = DriverManager.getConnection(url, username, password)) {
            Class.forName(dataSourceEnum.getDriver());

            return conn;
        } catch (Exception e) {
            LOG.error(String.format("数据库Url：%s， %s !!!!!", url, e.getMessage()));
            return null;
        }
    }
}
