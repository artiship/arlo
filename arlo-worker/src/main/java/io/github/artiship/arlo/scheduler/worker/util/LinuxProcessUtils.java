package io.github.artiship.arlo.scheduler.worker.util;

import com.google.common.collect.Lists;
import com.google.common.collect.Ordering;
import io.github.artiship.arlo.scheduler.core.model.SchedulerTaskBo;
import io.github.artiship.arlo.scheduler.worker.common.CommonCache;
import io.github.artiship.arlo.scheduler.worker.common.Constants;
import io.github.artiship.arlo.scheduler.worker.updownload.intf.IUpDownload;
import io.github.artiship.arlo.utils.TaskProcessUtils;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.zeroturnaround.exec.ProcessExecutor;
import org.zeroturnaround.exec.ProcessResult;
import org.zeroturnaround.exec.StartedProcess;
import org.zeroturnaround.exec.stream.LogOutputStream;
import org.zeroturnaround.process.PidProcess;
import org.zeroturnaround.process.ProcessUtil;
import org.zeroturnaround.process.Processes;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeoutException;

import static java.util.concurrent.TimeUnit.SECONDS;


@Slf4j
public class LinuxProcessUtils {
    private static final String DOS_TO_UNIX_KEYWORD = "dos2unix";
    private static final String ON_K8S_SPARK_APP_ID_PREFIX = "spark-application-";


    public static Long getLinuxProcessPid(StartedProcess startedProcess) throws Exception {
        Process process = startedProcess.getProcess();
        return TaskProcessUtils.getLinuxPid(process);
    }


    public static int getShellExitCode(StartedProcess startedProcess) throws ExecutionException, InterruptedException {
        Future future = startedProcess.getFuture();
        //等待任务执行完成
        ProcessResult processResult = (ProcessResult) future.get();
        return processResult.getExitValue();
    }


    public static StartedProcess executeLinuxShell(String runCommand, IUpDownload upDownload, SchedulerTaskBo task,
                                                   RpcClientHolder rpcClientHolder) throws IOException {
        //执行任务
        ProcessExecutor processExecutor = new ProcessExecutor();
        return processExecutor.command("/bin/bash", "-c", runCommand)
                              .redirectError(new LogOutputStream() {
                                  @Override
                                  protected void processLine(String errorMsg) {
                                      try {
                                          if (isNonsense(errorMsg)) return;

                                          //异常日志上传
                                          upDownload.uploadLog(CommonUtils.addDateBeforeLog(errorMsg));
                                          //采集Yarn的ApplicationId
                                          CommonUtils.extractApplicationId(errorMsg, task.getJobId(), task.getId(), task, rpcClientHolder);
                                      } catch (Exception e) {
                                          //获取写日志异常, 日志写入异常不因影响任务的正常秩序
                                          log.warn("Task_{}_{}, upload log failed, [{}]", task.getJobId(), task.getId(), errorMsg);
                                      }
                                  }
                              })
                              .redirectOutput(new LogOutputStream() {
                                  @Override
                                  protected void processLine(String normalMsg) {
                                      try {
                                          if (isNonsense(normalMsg)) return;

                                          //采集Yarn的ApplicationId
                                          CommonUtils.extractApplicationId(normalMsg, task.getJobId(), task.getId(), task, rpcClientHolder);
                                          //正常日志上传
                                          upDownload.uploadLog(CommonUtils.addDateBeforeLog(normalMsg));
                                      } catch (Exception e) {
                                          log.warn("Task_{}_{}, upload log failed, [{}]", task.getJobId(), task.getId(), normalMsg);
                                      }
                                  }


                              })
                              .destroyOnExit()
                              .start();
    }

    private static boolean isNonsense(String log) {
        if (log == null) return true;
        if (log.contains(DOS_TO_UNIX_KEYWORD)) return true;
        return false;
    }


    public static void killTaskOnYarn(SchedulerTaskBo task) throws Exception {
        String applicationId;
        String tmpApplicationId = CommonCache.getApplicationId(task);
        log.info("Kill applicationId, [{}], [{}]", task.getId(), tmpApplicationId);

        if (null != tmpApplicationId) {
            if (tmpApplicationId.trim()
                                .startsWith(ON_K8S_SPARK_APP_ID_PREFIX)) {
                log.info("Task_{}_{}, Ignore on k8s application id [{}]", task.getJobId(), task.getId(), tmpApplicationId);
                return;
            }
            do {
                log.info("Task_{}_{}, kill task applicationId [{}].", task.getJobId(), task.getId(), tmpApplicationId);
                applicationId = tmpApplicationId;
                LinuxProcessUtils.killYarnApplicationById(applicationId, task.getJobId(), task.getId());
                //KILL完成后再取一次当前的applicationId
                //避免多个applicationId正在更替, 导致任务KILL失败
                tmpApplicationId = CommonCache.getApplicationId(task);
            } while (!applicationId.equals(tmpApplicationId));
        }
    }


    private static void killYarnApplicationById(String applicationId, long jobId, long taskId) throws Exception {
        StringBuilder commandBuilder = new StringBuilder("source /etc/profile;export HADOOP_USER_NAME=hadoop;yarn application -kill ");
        commandBuilder.append(applicationId);

        ProcessExecutor processExecutor = new ProcessExecutor();
        StartedProcess startedProcess = processExecutor.command("/bin/bash", "-c", commandBuilder.toString())
                                                       .redirectError(new LogOutputStream() {
                                                           @Override
                                                           protected void processLine(String errorMsg) {
                                                               log.info("Task_{}_{}, kill yarn applicationId log [{}].", jobId, taskId, errorMsg);
                                                           }
                                                       })
                                                       .redirectOutput(new LogOutputStream() {
                                                           @Override
                                                           protected void processLine(String normalMsg) {
                                                               log.info("Task_{}_{}, kill yarn applicationId log [{}].", jobId, taskId, normalMsg);
                                                           }
                                                       })
                                                       .destroyOnExit()
                                                       .start();

        int exitCode = startedProcess.getFuture()
                                     .get()
                                     .getExitValue();
        if (0 != exitCode) {
            //exit code不为0, 即执行失败
            throw new RuntimeException(String.format("kill yarn application failed. yarn application id [%s], exit code [%s]", applicationId, exitCode));
        }
    }


    public static void killTaskOnLinux(SchedulerTaskBo task) throws Exception {
        Long pid = CommonCache.getPid(task);
        //KILL进程
        if (null != pid) {
            log.info("Task_{}_{}, kill task pid [{}].", task.getJobId(), task.getId(), pid);
            killLinuxProcessByPid(pid.intValue());
        }
    }


    private static void killLinuxProcessByPid(Integer pid) throws Exception {
        if (null != pid && -1 != pid) {
            List<String> pidTree = getPidTree(pid);
            Collections.sort(pidTree, Ordering.natural()
                                              .nullsLast()
                                              .reverse());

            log.info("Pid tree of {} is {}", pid, pidTree);
            for (String pidString : pidTree) {
                PidProcess process = Processes.newPidProcess(Integer.parseInt(pidString));
                if (process.isAlive()) {
                    destroyGracefullyOrForcefullyAndWait(pidString);

                    log.info("Kill Pid {}, ParentId {}", pidString, pid);
                }
            }
        }
    }

    public static void destroyGracefullyOrForcefullyAndWait(String pidString) throws InterruptedException, TimeoutException, IOException {
        int pid = Integer.parseInt(pidString);
        PidProcess process = Processes.newPidProcess(pid);
        ProcessUtil.destroyGracefullyOrForcefullyAndWait(process, 10, SECONDS, 5, SECONDS);
    }


    private static void killProcessByShell(String pidString, Integer parentPid) throws IOException, ExecutionException, InterruptedException {
        //执行任务
        ProcessExecutor processExecutor = new ProcessExecutor();
        int exitCode = processExecutor.command("/bin/bash", "-c", String.format(Constants.KILL_PID_COMMAND, pidString))
                                      .redirectError(new LogOutputStream() {
                                          @Override
                                          protected void processLine(String errorMsg) {
                                              log.info("Kill Pid {}, ParentId {}, Message [{}]", pidString, parentPid, errorMsg);
                                          }
                                      })
                                      .redirectOutput(new LogOutputStream() {
                                          @Override
                                          protected void processLine(String normalMsg) {
                                              log.info("Kill Pid {}, ParentId {}, Message [{}]", pidString, parentPid, normalMsg);
                                          }
                                      })
                                      .destroyOnExit()
                                      .start()
                                      .getFuture()
                                      .get()
                                      .getExitValue();

        log.info("Kill Pid {}, ParentId {}, Exit code [{}]", pidString, parentPid, exitCode);
    }


    private static List<String> getPidTree(Integer pid) throws InterruptedException, TimeoutException, IOException {
        List<String> needGetChildPids = Lists.newArrayList();
        needGetChildPids.add(pid.toString());

        //需要删除已经获取过子Pid集合的Pid
        List<String> removeHasGetChildPidsPid = Lists.newArrayList();
        //新增的需要子Pid集合的Pid
        List<String> newNeedGetChildPidsPid = Lists.newArrayList();
        //存储Pid树中所有的子Pid
        List<String> allPids = Lists.newArrayList();

        while (needGetChildPids.size() != 0) {
            for (String needGetChildPid : needGetChildPids) {
                String getChildPidCommand = String.format(Constants.GET_CHILD_PID, needGetChildPid);
                ProcessExecutor processExecutor = new ProcessExecutor();
                processExecutor.command("/bin/bash", "-c", getChildPidCommand)
                               .redirectOutput(new LogOutputStream() {
                                   @Override
                                   protected void processLine(String childPid) {
                                       if (StringUtils.isNotBlank(childPid) && StringUtils.isNumeric(childPid)) {
                                           allPids.add(childPid);
                                           //获取到的子Pid需要继续获取该子Pid的孩子Pid
                                           newNeedGetChildPidsPid.add(childPid);
                                       }
                                   }
                               })
                               .destroyOnExit()
                               .execute()
                               .getExitValue();

                //该Pid已经获取过了子Pid, 需要从"获取子Pid的Pid列表"中移除
                removeHasGetChildPidsPid.add(needGetChildPid);
            }

            //列表中移除
            needGetChildPids.removeAll(removeHasGetChildPidsPid);
            //新增需要获取子Pids的Pid
            needGetChildPids.addAll(newNeedGetChildPidsPid);
            //清空
            removeHasGetChildPidsPid.clear();
            newNeedGetChildPidsPid.clear();

        }
        allPids.add(pid.toString());
        return allPids;
    }

    public static void main(String[] args) {
        List<String> pids = new ArrayList<>();
        pids.add("4112");
        pids.add("4139");
        pids.add("4104");

        Collections.sort(pids, Ordering.natural()
                                       .nullsLast()
                                       .reverse());
        System.out.println(pids);
    }


    public static String md5(String file) {
        //执行任务
        ProcessExecutor processExecutor = new ProcessExecutor();
        final String[] md5 = new String[]{""};
        int exitCode = -1;
        try {
            exitCode = processExecutor.command("/bin/bash", "-c", String.format(Constants.MD5_COMMAND, file))
                                      .redirectError(new LogOutputStream() {
                                          @Override
                                          protected void processLine(String errorMsg) {
                                              md5[0] = errorMsg;
                                          }
                                      })
                                      .redirectOutput(new LogOutputStream() {
                                          @Override
                                          protected void processLine(String normalMsg) {
                                              md5[0] = normalMsg;
                                          }
                                      })
                                      .destroyOnExit()
                                      .start()
                                      .getFuture()
                                      .get()
                                      .getExitValue();
        } catch (Exception e) {
            log.error("Get file md5 failed.", e);
        }

        if (exitCode != 0) {
            log.error("Get file md5 failed, [{}]", file);
        }

        return md5[0];
    }
}
