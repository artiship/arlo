package io.github.artiship.arlo.scheduler.worker.util;

import com.google.common.collect.Lists;
import io.github.artiship.arlo.constants.GlobalConstants;
import io.github.artiship.arlo.model.enums.TaskState;
import io.github.artiship.arlo.scheduler.core.model.SchedulerTaskBo;
import io.github.artiship.arlo.scheduler.worker.common.CommonCache;
import io.github.artiship.arlo.scheduler.worker.common.Constants;
import io.github.artiship.arlo.scheduler.worker.model.WorkerNode;
import io.github.artiship.arlo.utils.MetricsUtils;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.List;


@Slf4j
public class CommonUtils {


    public static String getCurrentDate() {
        return LocalDateTime.now()
                            .format(DateTimeFormatter.ofPattern("yyyy-MM-dd"));
    }


    public static String buildRunShellCommand(String packageLocalPath, String bizDay, String bizHour, String bizMM, Long taskId) {
        //启动脚本
        StringBuilder shellCommandBuilder = new StringBuilder();
        //cd ${packageLocalPath}
        shellCommandBuilder.append("cd ")
                           .append(packageLocalPath)
                           .append(";");
        //dos2unix *sh
        shellCommandBuilder.append("dos2unix ")
                           .append("*.sh;");
        //chmod 777 *sh
        shellCommandBuilder.append("chmod 777 ")
                           .append("*.sh;");
        //sh start-all.sh {bizday} {bizhour} ${bizMM}
        return shellCommandBuilder.append("sh ")
                                  .append(Constants.START_SHELL_FILE_NAME)
                                  .append(" ")
                                  .append(bizDay)
                                  .append(" ")
                                  .append(bizHour)
                                  .append(" ")
                                  .append(bizMM)
                                  .append(" ")
                                  .append(taskId)
                                  .toString();
    }


    public static String getNodePath(int workerPort) {
        WorkerNode node = new WorkerNode(MetricsUtils.getHostIpAddress(), workerPort);
        StringBuilder basePathBuilder = new StringBuilder();
        basePathBuilder.append(GlobalConstants.WORKER_GROUP)
                       .append(Constants.ZK_PATH_SEPARATOR)
                       .append(node.toString());

        return basePathBuilder.toString();
    }


    public static String getSyncTaskRegistPath(int workerPort) {
        WorkerNode node = new WorkerNode(MetricsUtils.getHostIpAddress(), workerPort);
        StringBuilder basePathBuilder = new StringBuilder();
        basePathBuilder.append(Constants.SYNC_TASK_WORKER)
                       .append(Constants.ZK_PATH_SEPARATOR)
                       .append(node.toString());

        return basePathBuilder.toString();
    }


    public static void killAllRunningTasks(ZkClientHolder zkClientHolder, RpcClientHolder rpcClientHolder, TaskState taskState) {
        List<SchedulerTaskBo> runningTasks = CommonCache.cloneRunningTask();
        if (runningTasks.size() != 0) {
            runningTasks.forEach((task) -> {
                log.info("Need to kill task [{}]", task);
                try {
                    CommonUtils.killTask(zkClientHolder, rpcClientHolder, task, taskState);
                } catch (Exception e) {
                    log.error(String.format("Task_%s_%s, kill task failed.", task.getJobId(), task.getId()), e);
                }
            });
        }
    }


    public static void killTask(ZkClientHolder zkClientHolder, RpcClientHolder rpcClientHolder, SchedulerTaskBo task, TaskState returnTaskState) {
        try {
            //记录KILL的taskId, 以免在KILL过程中返回SUCCESSFAIL状态给master
            Long taskId = task.getId();
            CommonCache.addKillTask(taskId);
            log.info("Task_{}_{}, start handle kill task request.", task.getJobId(), task.getId());
            //杀死yarn的applicationId
            LinuxProcessUtils.killTaskOnYarn(task);
            //杀死运行在机器上的任务
            LinuxProcessUtils.killTaskOnLinux(task);
            log.info("Task_{}_{}, kill task success.", task.getJobId(), task.getId());
            //KILL成功
            RpcUtils.updateTask(zkClientHolder, rpcClientHolder, task, returnTaskState, null, LocalDateTime.now());
            log.info("Task_{}_{}, push state [{}] to master success.", task.getJobId(), task.getId(), returnTaskState.getCode());
        } catch (Exception e) {
            TaskState pushMasterState = returnTaskState == TaskState.FAILOVER ? TaskState.FAILOVER : TaskState.KILL_FAIL;
            //KILL失败
            RpcUtils.updateTask(zkClientHolder, rpcClientHolder, task, pushMasterState, null, LocalDateTime.now());
            log.error(String.format("Task_%s_%s, task kill failed, push state [%s]", task.getJobId(), task.getId(), pushMasterState.getCode()), e);
        }
    }


    public static String addDateBeforeLog(String outputLog) {
        StringBuilder builder = new StringBuilder(outputLog.length() + 30);
        builder.append("[")
               .append(LocalDateTime.now()
                                    .format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS")))
               .append("] ")
               .append(outputLog);
        return builder.toString();
    }


    public static void extractApplicationId(String runningLog, long jobId, long taskId, SchedulerTaskBo task, RpcClientHolder rpcClientHolder) {
        if (null != runningLog) {
            String applicationId = null;
            if (runningLog.contains(Constants.WATERDROP_AND_SAPRK_APPLICATIONID_TAG)) {
                //该行日志包含Spark的TAG
                applicationId = getApplicationIdBySparkLog(runningLog, Constants.WATERDROP_AND_SAPRK_APPLICATIONID_TAG);
            } else if (runningLog.contains(Constants.HIVE_APPLICATIONID_TAG)) {
                //该行日志包含Hive的TAG
                applicationId = getApplicationIdByHiveLog(runningLog, Constants.APPLICATIONID_TAG);
            }

            if (StringUtils.isBlank(applicationId)) {
                return;
            }

            log.info("Task_{}_{}, extracted application id [{}].", jobId, taskId, applicationId);
            List<String> applicationIds = task.getApplicationId();
            if (null == applicationIds) {
                applicationIds = Lists.newArrayList();
            }

            applicationIds.add(applicationId);
            task.setApplicationId(applicationIds);

            //缓存最新的ApplicationId
            CommonCache.cacheApplicationId(task, applicationId);

            //把applicationId推送到master
            try {
                RpcUtils.send2Master(rpcClientHolder, task.toRpcTask());
            } catch (Exception e) {
                //这个applicationId推送失败可以忽略, 影响不是很严重
                log.error(String.format("update application id to master failed. task [%s]", task), e);
            }
        }
    }


    private static String getApplicationIdByHiveLog(String runningLog, String applicationIdTag) {
        int startIndex = runningLog.indexOf(applicationIdTag);
        int endIndex = runningLog.length() - 1;

        if (startIndex != -1 && startIndex <= endIndex) {
            return runningLog.substring(startIndex, endIndex);
        }

        return "";
    }


    private static String getApplicationIdBySparkLog(String runningLog, String applicationIdTag) {
        int startIndex = runningLog.indexOf(applicationIdTag);
        if (-1 != startIndex) {
            return runningLog.substring(startIndex + applicationIdTag.length());
        }

        return "";
    }
}