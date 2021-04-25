package io.github.artiship.arlo.dbconn;

import lombok.Getter;

@Getter
public enum DataSourceEnum {

    MySQL(
            "mysql",
            "jdbc:mysql://%s:%s/%s?useUnicode=true&characterEncoding=UTF-8&serverTimezone=UTC",
            "com.mysql.jdbc.Driver"
    ),

    MySQLJDBC(
            "mysql",
            "jdbc:mysql://%s:%s/%s?useUnicode=true&characterEncoding=UTF-8",
            "com.mysql.cj.jdbc.Driver"
    ),

//    HiveMetadata(
//            "hiveMetadata",
//            "jdbc:mysql://%s:%s/%s?useUnicode=true&characterEncoding=UTF-8",
//            "com.mysql.jdbc.Driver"
//    ),

    Postgres("postgres",
            "jdbc:postgresql://%s:%s/%s",
            "org.postgresql.Driver"
    );

    private String name;
    private String url;
    private String driver;

    DataSourceEnum(String name, String url, String driver) {
        this.name = name;
        this.url = url;
        this.driver = driver;
    }
}
