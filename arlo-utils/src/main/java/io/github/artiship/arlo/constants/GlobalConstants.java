package io.github.artiship.arlo.constants;

public class GlobalConstants {
    //zk
    public static final String WORKER_GROUP = "/arlo/workers";
    public static final String MASTER_GROUP = "/arlo/masters";
    public static final String DAG_GROUP = "/arlo/dags";
    public static final String ALERT_GROUP = "/arlo/alert";
    public static final String DEAD_WORKER_GROUP = "/arlo/dead/workers";
    public static final String LOST_TASK_GROUP = "/arlo/lost/tasks";
    public static final String ZK_FILES_GROUP = "/arlo/files";
    public static final String ZK_LOCK_GROUP = "/arlo/lock";

    public static final String ARLO = "arlo";
    public static final String CONFIG_FILE = "config.properties";
    public static final String APPLICATION_REGEX = "application_\\d+_\\d+";
    public static final String WORK_START_ALL_JOB_SHELL = "start-all";
    public static final String WORK_K8S_START_JOB_SHELL = "k8s_start";
    public static final String WORK_K8S_JOB_CONFIG_SHELL = "pod_config";

    //oss
    public static final String OSS_PATH_PREFIX = "arlo_";
    public static final String OSS_PATH_PUBLIC = ARLO + "/public/sourceGroup/";
    public static final String OSS_PATH_COMMON = ARLO + "/common";

    public static final String ERROR = "error";
    //sls
    public static final String ACCESS_ID = "";
    public static final String ACCESS_KEY = "";
    public static final String HOST = "";
    public static final String PROJECT = "datacenter-platform-arlo";
    public static final String LOG_STORE = "arlo";
    public static final String TOPIC = "arlo";
    public static final String LEFTBRACKETS = "(";
    public static final String RIGHTBRACKETS = ")";
    //bigint
    public static final String TINYINT = "tinyint";
    public static final String SMALLINT = "smallint";
    public static final String MEDIUMINT = "mediumint";
    public static final String INT = "int";
    public static final String BIGINT = "bigint";
    //double
    public static final String DECIMAL = "decimal";
    public static final String DOUBLE = "double";
    public static final String FLOAT = "float";
    //String
    public static final String DATE = "date";
    public static final String TIME = "time";
    public static final String YEAR = "year";
    public static final String DATETIME = "datetime";
    public static final String TIMESTAMP = "timestamp";
    public static final String CHAR = "char";
    public static final String VARCHAR = "varchar";
    public static final String TINYBLOB = "tinyblob";
    public static final String TINYTEXT = "tinytext";
    public static final String BLOB = "blob";
    public static final String TEXT = "text";
    public static final String MEDIUMBLOB = "mediumblob";
    public static final String MEDIUMTEXT = "mediumtext";
    public static final String LONGBLOB = "longblob";
    public static final String LONGTEXT = "longtext";

    public static final String STRING = "string";

    public static final String MASTER = "master";

    public static final Long DEFAULT_SIZE = 256 * 1024 * 1024 * 100L;

}
