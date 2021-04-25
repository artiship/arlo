package io.github.artiship.arlo.scheduler.worker;

import io.github.artiship.arlo.constants.GlobalConstants;
import io.github.artiship.arlo.model.enums.FileType;
import io.github.artiship.arlo.model.enums.TaskState;
import io.github.artiship.arlo.scheduler.worker.common.Constants;
import io.github.artiship.arlo.scheduler.worker.rpc.WorkerRpcServer;
import io.github.artiship.arlo.scheduler.worker.thread.HandleMasterCommandThread;
import io.github.artiship.arlo.scheduler.worker.thread.HeartbeatThread;
import io.github.artiship.arlo.scheduler.worker.thread.ZkWatcherCompensation;
import io.github.artiship.arlo.scheduler.worker.util.CommonUtils;
import io.github.artiship.arlo.scheduler.worker.util.OSSClientHolder;
import io.github.artiship.arlo.scheduler.worker.util.RpcClientHolder;
import io.github.artiship.arlo.scheduler.worker.util.SyncCommonShellUtil;
import io.github.artiship.arlo.scheduler.worker.util.ZkClientHolder;
import io.github.artiship.arlo.scheduler.worker.util.ZkUtils;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.io.FileUtils;
import org.apache.zookeeper.CreateMode;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.springframework.context.annotation.ImportResource;
import org.springframework.stereotype.Component;

import java.io.File;
import java.io.IOException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;


@Slf4j
@ImportResource("file:///data1/etc/project/arlo-worker/applicationContext.xml")
@Component
public class WorkerLauncher {

    @Value("${arlo.zookeeper.quorum}")
    private String zookeeperAddress;

    @Value("${arlo.zookeeper.sessionTimeout}")
    private int zookeeperSessionTimeout;

    @Value("${arlo.zookeeper.connectionTimeout}")
    private int zookeeperConnectTimeout;

    @Value("${arlo.worker.heartbeat.interval}")
    private long heartbeatInterval;

    @Value("${arlo.worker.server.port}")
    private int workerPort;

    @Value("${arlo.worker.group}")
    private String group;

    @Value("${arlo.worker.max.task.num}")
    private int maxTaskNums;

    @Autowired
    private WorkerRpcServer rpcServer;
    @Autowired
    private HeartbeatThread heartbeatThread;
    @Autowired
    private ZkClientHolder zkClientHolder;
    @Autowired
    private HandleMasterCommandThread handleMasterCommandThread;
    @Autowired
    private RpcClientHolder rpcClientHolder;
    @Autowired
    private OSSClientHolder ossClientHolder;
    private ScheduledExecutorService heartbeatSes;
    private ScheduledExecutorService handleMasterCmdSes;
    private ScheduledExecutorService syncFilesSes;


    public static void main(String[] args) {
        AnnotationConfigApplicationContext context = new AnnotationConfigApplicationContext(WorkerLauncher.class);
        WorkerLauncher launcher = context.getBean(WorkerLauncher.class);
        launcher.printConfigInfo();
        launcher.registHook(context);
        try {
            launcher.beforeStartUp();
            launcher.start();
        } catch (Exception e) {
            log.error("worker server start failed.", e);
            System.exit(-1);
        }
    }


    public void beforeStartUp() {
        heartbeatSes = Executors.newSingleThreadScheduledExecutor();
        handleMasterCmdSes = Executors.newSingleThreadScheduledExecutor();
        syncFilesSes = Executors.newSingleThreadScheduledExecutor();
        ZkUtils.createPathRecursive(zkClientHolder.getZkClient(), GlobalConstants.DEAD_WORKER_GROUP, CreateMode.PERSISTENT);
    }


    private void registHook(AnnotationConfigApplicationContext context) {
        Runtime.getRuntime()
               .addShutdownHook(new ShutDownHookThread(context));
    }


    private void start() throws IOException, InterruptedException {
        StringBuilder localPathBuilder = new StringBuilder();
        for (FileType fileType : FileType.values()) {
            localPathBuilder.setLength(0);
            localPathBuilder.append(Constants.LOCAL_COMMON_SHELL_BASE_PATH)
                            .append(File.separator)
                            .append(fileType.getDir());
            File localPath = new File(localPathBuilder.toString());
            if (!localPath.exists()) {
                FileUtils.forceMkdir(localPath);
            }

            localPathBuilder.setLength(0);
            localPathBuilder.append(Constants.LOCAL_SYNC_TMP)
                            .append(File.separator)
                            .append(fileType.getDir());
            localPath = new File(localPathBuilder.toString());
            if (!localPath.exists()) {
                FileUtils.forceMkdir(localPath);
            }
        }


        if (!ZkUtils.exists(zkClientHolder.getZkClient(), Constants.SYNC_TASK)) {
            ZkUtils.createNode(zkClientHolder.getZkClient(), Constants.SYNC_TASK, "", CreateMode.PERSISTENT);
        }

        ZkUtils.regist(workerPort, zkClientHolder.getZkClient(), ossClientHolder, false);
        ZkUtils.watchCommonShellChange(zkClientHolder.getZkClient());
        SyncCommonShellUtil.syncAllCommonShell(ossClientHolder);

        heartbeatSes.scheduleWithFixedDelay(heartbeatThread, 0L, heartbeatInterval, TimeUnit.MILLISECONDS);
        handleMasterCmdSes.scheduleWithFixedDelay(handleMasterCommandThread, 0L, Constants.CHECK_MASTER_CMD_INTERVAL, TimeUnit.MILLISECONDS);
        ZkWatcherCompensation zkWatcherCompensation = new ZkWatcherCompensation(zkClientHolder);
        syncFilesSes.scheduleWithFixedDelay(zkWatcherCompensation, 0L, Constants.ZK_WATCHER_COMPENSATION_INTERVAL, TimeUnit.MILLISECONDS);

        rpcServer.startServer();
        rpcServer.blockUntilShutdown();
    }

    private void printConfigInfo() {
        log.info("----------------------------------------------------");
        log.info("*******zookeeperAddress : {}", zookeeperAddress);
        log.info("zookeeperSessionTimeout : {}", zookeeperSessionTimeout);
        log.info("zookeeperConnectTimeout : {}", zookeeperConnectTimeout);
        log.info("******heartbeatInterval : {}", heartbeatInterval);
        log.info("*************workerPort : {}", workerPort);
        log.info("******************group : {}", group);
        log.info("************maxTaskNums : {}", maxTaskNums);
        log.info("----------------------------------------------------");
    }

    private class ShutDownHookThread extends Thread {

        private AnnotationConfigApplicationContext context;

        public ShutDownHookThread(AnnotationConfigApplicationContext context) {
            this.context = context;
        }

        @Override
        public void run() {
            log.info("shut down hook running.");
            log.info("stop heart beat.");

            if (null != heartbeatSes) {
                heartbeatSes.shutdownNow();
            }

            if (null != handleMasterCmdSes) {
                handleMasterCmdSes.shutdownNow();
            }

            log.info("unregist self from zookeeper.");

            ZkUtils.unRegist(workerPort, zkClientHolder.getZkClient());

            log.info("kill all running task.");

            CommonUtils.killAllRunningTasks(zkClientHolder, rpcClientHolder, TaskState.FAILOVER);

            log.info("shut down grpc server.");
            if (rpcServer != null) {
                rpcServer.shutdownNow();
            }

            log.info("close spring context.");
            context.close();

            log.warn("shut down hook run success.");
        }
    }
}