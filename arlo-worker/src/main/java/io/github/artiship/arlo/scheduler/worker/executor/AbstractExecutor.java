package io.github.artiship.arlo.scheduler.worker.executor;

import io.github.artiship.arlo.model.enums.TaskState;
import io.github.artiship.arlo.scheduler.core.model.SchedulerTaskBo;
import io.github.artiship.arlo.scheduler.worker.common.CommonCache;
import io.github.artiship.arlo.scheduler.worker.common.Constants;
import io.github.artiship.arlo.scheduler.worker.updownload.impl.OssUpDownload;
import io.github.artiship.arlo.scheduler.worker.updownload.intf.IUpDownload;
import io.github.artiship.arlo.scheduler.worker.util.CommonUtils;
import io.github.artiship.arlo.scheduler.worker.util.LinuxProcessUtils;
import io.github.artiship.arlo.scheduler.worker.util.OSSClientHolder;
import io.github.artiship.arlo.scheduler.worker.util.RpcClientHolder;
import io.github.artiship.arlo.scheduler.worker.util.RpcUtils;
import io.github.artiship.arlo.scheduler.worker.util.ZkClientHolder;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;
import org.zeroturnaround.exec.StartedProcess;

import java.io.File;
import java.io.IOException;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

import static io.github.artiship.arlo.scheduler.worker.util.CommonUtils.buildRunShellCommand;


@Slf4j
public abstract class AbstractExecutor implements IExecutor {

    private SchedulerTaskBo task;

    private RpcClientHolder rpcClientHolder;

    private ZkClientHolder zkClientHolder;

    private OSSClientHolder ossClientHolder;

    private IUpDownload upDownload;

    public AbstractExecutor(SchedulerTaskBo task, RpcClientHolder rpcClientHolder, ZkClientHolder zkClientHolder, OSSClientHolder ossClientHolder) {
        this.task = task;
        this.rpcClientHolder = rpcClientHolder;
        this.zkClientHolder = zkClientHolder;
        this.ossClientHolder = ossClientHolder;
    }

    @Override
    public void execute() {
        try {
            log.info("Task_{}_{}, start handle task execute request, taskid [{}].", task.getJobId(), task.getId(), task.getId());
            task.setStartTime(LocalDateTime.now());
            String packageLocalPath = getWorkPath(getBaswWorkPathWithoutPlaceholder(), task.getId());
            upDownload = preExecute(ossClientHolder, packageLocalPath);
            log.info("Task_{}_{}, do preprocess success.", task.getJobId(), task.getId());

            String[] bizDayBizHourAndMM = getBizDayBizHourBizMM();
            String runCommand = buildRunShellCommand(
                    packageLocalPath,
                    bizDayBizHourAndMM[0],
                    bizDayBizHourAndMM[1],
                    bizDayBizHourAndMM[2],
                    task.getId()
            );

            log.info("Task_{}_{}, run shell command [{}].", task.getJobId(), task.getId(), runCommand);

            if (!check(packageLocalPath)) {
                log.error("Task_{}_{}, workpath not exist or 'start-all.sh' not found.", task.getJobId(), task.getId());
                RpcUtils.updateTask(zkClientHolder, rpcClientHolder, task, TaskState.FAIL, null, LocalDateTime.now());
                return;
            }

            StartedProcess startedProcess = doExecute(runCommand, upDownload);

            Long pid = null;
            try {
                pid = LinuxProcessUtils.getLinuxProcessPid(startedProcess);
            } catch (Exception e) {
                log.error(String.format("task excute failed ,set -1. task [%s]", task), e);
            }
            log.info("Task_{}_{}, process pid [{}].", task.getJobId(), task.getId(), pid);

            CommonCache.cachePid(task, pid);

            RpcUtils.updateTask(zkClientHolder, rpcClientHolder, task, TaskState.RUNNING, pid, null);
            log.info("Task_{}_{}, task push running state to master success.", task.getJobId(), task.getId());

            int exitCode = LinuxProcessUtils.getShellExitCode(startedProcess);

            log.info("Task_{}_{}, task shell exit code [{}].", task.getJobId(), task.getId(), exitCode);

            if (0 == exitCode) {
                RpcUtils.updateTask(zkClientHolder, rpcClientHolder, task, TaskState.SUCCESS, null, LocalDateTime.now());
                log.info("Task_{}_{}, task push success state to master success.", task.getJobId(), task.getId());
            } else {
                RpcUtils.updateTask(zkClientHolder, rpcClientHolder, task, TaskState.FAIL, null, LocalDateTime.now());
                log.info("Task_{}_{}, task push fail state to master success.", task.getJobId(), task.getId());
            }
        } catch (Throwable e) {
            log.error(String.format("Task_%s_%s task excute failed.", task.getJobId(), task.getId()), e);
            RpcUtils.updateTask(zkClientHolder, rpcClientHolder, task, TaskState.FAIL, null, LocalDateTime.now());
        } finally {
            CommonCache.removeApplicationId(task);
            CommonCache.removePid(task);
            if (null != upDownload) {
                upDownload.close();
            }
        }
    }

    @Override
    public void kill() {
        CommonUtils.killTask(zkClientHolder, rpcClientHolder, task, TaskState.KILLED);
    }


    public abstract String getBaswWorkPath();


    protected StartedProcess doExecute(String runCommand, IUpDownload upDownload) throws IOException {
        return LinuxProcessUtils.executeLinuxShell(runCommand, upDownload, task, rpcClientHolder);
    }


    public IUpDownload preExecute(OSSClientHolder ossClientHolder, String workPath) throws Exception {
        File targetPath = new File(workPath);
        if (targetPath.exists()) {
            FileUtils.deleteDirectory(targetPath);
        }

        FileUtils.forceMkdir(targetPath);

        String packagePath = task.getOssPath();
        if (!StringUtils.isNotBlank(packagePath) || -1 == packagePath.indexOf("/")) {
            throw new RuntimeException(String.format("Bad input oss path : %s", packagePath));
        }

        String basePackagePath = packagePath.substring(0, packagePath.lastIndexOf("/"));

        StringBuilder runLogFilePathBuilder = new StringBuilder();
        runLogFilePathBuilder.append(basePackagePath)
                             .append(File.separator)
                             .append(Constants.LOG_FILE_PATH)
                             .append(File.separator)
                             .append(task.getId())
                             .append(File.separator)
                             .append(Constants.NORMAL_LOG_FILE_NAME);

        IUpDownload upDownload = new OssUpDownload(runLogFilePathBuilder.toString(), ossClientHolder);
        upDownload.preprocess();

        downloadTaskRunPackage(upDownload, task.getOssPath(), workPath);

        if (StringUtils.isNotBlank(ossClientHolder.getCommonObjects())) {
            downloadCommonPackage(upDownload, ossClientHolder.getCommonObjects(), workPath);
        }

        return upDownload;
    }


    protected String getWorkPath(String workBasePath, Long taskId) {
        String version = getVersion(task.getOssPath());
        StringBuilder workPathBuilder = new StringBuilder(workBasePath);
        workPathBuilder.append(File.separator)
                       .append(version)
                       .append(File.separator)
                       .append(taskId);
        return workPathBuilder.toString();
    }


    private String getVersion(String path) {
        return path.substring(path.lastIndexOf("/") + 1);
    }


    protected void downloadTaskRunPackage(IUpDownload upDownload, String sourcePath, String targetPath) throws Exception {
        upDownload.download(sourcePath, targetPath);
    }


    protected void downloadCommonPackage(IUpDownload upDownload, String sourcePath, String targetPath) throws Exception {
        upDownload.download(sourcePath, targetPath);
    }

    protected String[] getBizDayBizHourBizMM() {
        LocalDateTime scheduleTime = task.getCalculationTime();
        String day = scheduleTime.format(DateTimeFormatter.ofPattern("yyyy-MM-dd"));
        String hour = scheduleTime.format(DateTimeFormatter.ofPattern("HH"));
        String min = scheduleTime.format(DateTimeFormatter.ofPattern("mm"));
        return new String[]{day, hour, min};
    }

    protected String getBaswWorkPathWithoutPlaceholder() {
        return String.format(getBaswWorkPath(), CommonUtils.getCurrentDate());
    }

    public boolean check(String packageLocalPath) {
        File runPath = new File(packageLocalPath);
        if (runPath.exists()) {
            StringBuilder startAllShellBuilder = new StringBuilder(packageLocalPath);
            startAllShellBuilder.append(File.separator)
                                .append(Constants.START_SHELL_FILE_NAME);
            File startAllShellFile = new File(startAllShellBuilder.toString());
            if (startAllShellFile.exists()) {
                return true;
            }
        }

        return false;
    }
}
