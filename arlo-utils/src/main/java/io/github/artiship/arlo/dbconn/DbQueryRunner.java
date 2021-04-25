package io.github.artiship.arlo.dbconn;

import org.apache.commons.dbutils.QueryRunner;
import org.apache.commons.dbutils.handlers.MapListHandler;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.sql.Connection;
import java.sql.DriverManager;
import java.util.List;
import java.util.Map;

public class DbQueryRunner extends DbConnAbstractor {
    private static Log LOG = LogFactory.getLog(DbQueryRunner.class);

    private DataSourceEnum dataSourceEnum;
    private String ip;
    private String port;
    private String database;
    private String username;
    private String password;

    public DbQueryRunner(DataSourceEnum dataSourceEnum, String ip, String port, String database, String username, String password) {
        this.dataSourceEnum = dataSourceEnum;
        this.ip = ip;
        this.port = port;
        this.database = database;
        this.username = username;
        this.password = password;
    }

    public Connection testConn() throws Exception {
        LOG.error(String.format("testConn config: %s %s", username, ip));
        Class.forName(dataSourceEnum.getDriver());
        String url = String.format(dataSourceEnum.getUrl(), ip, port, database);
        try (Connection conn = DriverManager.getConnection(url, username, password)) {
            return conn;
        } catch (Exception e) {
            e.printStackTrace();
            LOG.error(String.format("testConn: %s ", e.getMessage()));
            throw new Exception(e.getMessage());
        }
    }

    public List<Map<String, Object>> queryListScan(String sql) throws Exception {
        QueryRunner queryRunner = new QueryRunner();
        String url = String.format(dataSourceEnum.getUrl(), ip, port, database);
        Class.forName(dataSourceEnum.getDriver());
        try (Connection conn = DriverManager.getConnection(url, username, password)) {
            List<Map<String, Object>> result = queryRunner.query(conn, sql, new MapListHandler());

            return result;
        } catch (Exception e) {
            LOG.error(String.format("数据库Url：%s; DB Query Get Empty Set !!!!!", e.getMessage()));
            throw new Exception(e.getMessage());
        }
    }
}
