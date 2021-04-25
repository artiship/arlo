package io.github.artiship.arlo.dbconn;

public class SchemaInfo {

    public static final int MySQL = 1;
    public static final int Hive = 2;

    public static final String SHOW_DATABASES = "show databases;";

    public static final String FieldDelimiter = ",";

    public static String MySQLTable = "SELECT " +
            "1 as type, " +
            "table_schema as dbName, " +
            "table_name as tblName, " +
            "table_comment as tblComment " +
            "FROM INFORMATION_SCHEMA.TABLES " +
            "WHERE table_schema ='%s' " +
            "; ";

    public static String MySQLMultipleTable = "SELECT " +
            "1 as type, " +
            "table_schema as dbName, " +
            "table_name as tblName, " +
            "table_comment as tblComment " +
            "FROM INFORMATION_SCHEMA.TABLES " +
            "WHERE table_schema ='%s' " +
            "and table_name REGEXP '%s' " +
            "; ";

    public static String MySQLTableAndStatistics = "SELECT " +
            "1 as type, " +
            "TABLE_SCHEMA as dbName, " +
            "TABLE_NAME as tblName, " +
            "table_comment as tblComment, " +
            "ifnull(UPDATE_TIME , CREATE_TIME) as transientLastDdlTime, " +
            "TABLE_ROWS as numRows, " +
            "null as numFiles, " +
            "data_length as rawDataSize, " +
            "(data_length + index_length) as totalSize " +
            "FROM INFORMATION_SCHEMA.TABLES " +
            "WHERE table_schema = '%s' " +
            "; ";

    public static String SINEGLMySQLTableAndStatistics = "SELECT " +
            "1 as type, " +
            "TABLE_SCHEMA as dbName, " +
            "TABLE_NAME as tblName, " +
            "table_comment as tblComment, " +
            "ifnull(UPDATE_TIME , CREATE_TIME) as transientLastDdlTime, " +
            "TABLE_ROWS as numRows, " +
            "null as numFiles, " +
            "data_length as rawDataSize, " +
            "(data_length + index_length) as totalSize " +
            "FROM INFORMATION_SCHEMA.TABLES " +
            "WHERE table_schema = '%s' AND table_name = '%s' " +
            "; ";

    public static String MySQLTableField = "SELECT " +
            "column_name as columnName, " +
            "column_type as columnType, " +
            "column_comment as columnComment," +
            "if(column_key='PRI', 1, 0) as isPri " +
            "FROM INFORMATION_SCHEMA.COLUMNS " +
            "WHERE table_schema = '%s' " +
            "AND table_name = '%s' " +
            "; ";
}
