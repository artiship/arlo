package io.github.artiship.arlo.scheduler.worker.common;

public class Constants {
    public static final String GET_CHILD_PID = "ps -ef|awk '{print $2\" \"$3}'|grep \" %s\"|awk '{print $1}'";
    public static final String ZK_PATH_SEPARATOR = "/";
    public static final String LOG_FILE_PATH = "log";
    public static final String NORMAL_LOG_FILE_NAME = "normalLog.log";
    public static final String START_SHELL_FILE_NAME = "start-all.sh";
    public static final Long CHECK_MASTER_CMD_INTERVAL = 10000L;
    public static final long ZK_WATCHER_COMPENSATION_INTERVAL = 3000L;
    public static final String FILE_SYNC_HANDLER_SOURCE_WATCHER = "watcher";
    public static final String FILE_SYNC_HANDLER_SOURCE_COMPENSATION = "compensation";
    public static final String KILL_PID_COMMAND = "kill -9 %s";
    public static final String MD5_COMMAND = "md5sum %s";
    public static final String WATERDROP_AND_SAPRK_APPLICATIONID_TAG = "Application Id:";
    public static final String HIVE_APPLICATIONID_TAG = "Tracking URL =";
    public static final String APPLICATIONID_TAG = "application_";
    public static final String LOCAL_COMMON_SHELL_BASE_PATH = "/data1/common";
    public static final String SYNC_TASK = "/arlo/syncLock";
    public static final String SYNC_TASK_WORKER = "/arlo/syncWorker";
    public static final String LOCAL_SYNC_TMP = "/data/arlo/worker/sync";

}
