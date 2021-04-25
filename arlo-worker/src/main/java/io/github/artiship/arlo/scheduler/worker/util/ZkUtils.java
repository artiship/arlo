package io.github.artiship.arlo.scheduler.worker.util;

import io.github.artiship.arlo.constants.GlobalConstants;
import io.github.artiship.arlo.scheduler.worker.common.Constants;
import io.github.artiship.arlo.scheduler.worker.model.WorkerNode;
import io.github.artiship.arlo.scheduler.worker.zookeeper.listener.CommonShellChangeHandlerListener;
import io.github.artiship.arlo.scheduler.worker.zookeeper.listener.CommonShellChangeSenderListener;
import lombok.extern.slf4j.Slf4j;
import org.I0Itec.zkclient.IZkChildListener;
import org.I0Itec.zkclient.ZkClient;
import org.I0Itec.zkclient.exception.ZkNodeExistsException;
import org.apache.zookeeper.CreateMode;

import java.util.List;


@Slf4j
public class ZkUtils {


    public static void regist(int workerPort, ZkClient zkClient, OSSClientHolder ossClientHolder, boolean fromHeartbeat) {
        String nodePath = CommonUtils.getNodePath(workerPort);
        String basePath = nodePath.substring(0, nodePath.lastIndexOf(Constants.ZK_PATH_SEPARATOR));
        if (!ZkUtils.exists(zkClient, basePath)) {
            createPathRecursive(zkClient, basePath, CreateMode.PERSISTENT);
        }

        String syncRegistPath = CommonUtils.getSyncTaskRegistPath(workerPort);
        String syncRegistBasePath = syncRegistPath.substring(0, syncRegistPath.lastIndexOf(Constants.ZK_PATH_SEPARATOR));
        if (!ZkUtils.exists(zkClient, syncRegistBasePath)) {
            createPathRecursive(zkClient, syncRegistBasePath, CreateMode.PERSISTENT);
        }

        createNode(zkClient, nodePath, null, CreateMode.EPHEMERAL);

        if (!ZkUtils.exists(zkClient, syncRegistPath)) {
            createNode(zkClient, syncRegistPath, null, CreateMode.PERSISTENT);
        }

        if (!fromHeartbeat) {
            addWatcher(zkClient, syncRegistPath, new CommonShellChangeHandlerListener(zkClient, ossClientHolder));
        }

    }


    public static void watchCommonShellChange(ZkClient zkClient) {
        if (!exists(zkClient, GlobalConstants.ZK_FILES_GROUP)) {
            createPathRecursive(zkClient, GlobalConstants.ZK_FILES_GROUP, CreateMode.PERSISTENT);
        }

        addWatcher(zkClient, GlobalConstants.ZK_FILES_GROUP, new CommonShellChangeSenderListener(zkClient));
    }


    public static void addWatcher(ZkClient zkClient, String watchPath, IZkChildListener watcher) {
        zkClient.subscribeChildChanges(watchPath, watcher);
    }


    public static void unRegist(int workerPort, ZkClient zkClient) {
        String nodePath = CommonUtils.getNodePath(workerPort);
        delNode(zkClient, nodePath);

        String syncWorkerPath = CommonUtils.getSyncTaskRegistPath(workerPort);
        delNode(zkClient, syncWorkerPath);
    }


    public static WorkerNode activeMaster(ZkClient zkClient, String parentPath) {
        String data = ZkUtils.getNodeData(zkClient, parentPath);
        String[] ipPort;
        if (null != data && (ipPort = data.split(":")).length == 3) {
            return new WorkerNode(ipPort[0], Integer.parseInt(ipPort[1]));
        } else {
            throw new RuntimeException("can not get active master.");
        }
    }


    public static String getNodeData(ZkClient zkClient, String path) {
        if (zkClient.exists(path)) {
            return zkClient.readData(path);
        }

        return null;
    }


    public static void createPathRecursive(ZkClient zkClient, String path, CreateMode createMode) {
        if (path.endsWith(Constants.ZK_PATH_SEPARATOR)) {
            path = path.substring(0, path.length() - 1);
        }

        int index = path.lastIndexOf(Constants.ZK_PATH_SEPARATOR);
        if (0 != index) {
            String parentPath = path.substring(0, index);
            if (!zkClient.exists(parentPath)) {
                createPathRecursive(zkClient, parentPath, createMode);
            }
            createNode(zkClient, path, "", createMode);
        } else {
            createNode(zkClient, path, "", createMode);
        }
    }


    public static void createNode(ZkClient zkClient, String path, String data, CreateMode createMode) {
        try {
            zkClient.create(path, data, createMode);
        } catch (Exception e) {
            if (!(e instanceof ZkNodeExistsException)) {
                throw e;
            }
        }
    }


    public static boolean createNodeIfNotExist(ZkClient zkClient, String path, String data, CreateMode createMode) {
        try {
            zkClient.create(path, data, createMode);
        } catch (Exception e) {
            log.error("create node failed.", e);
            return false;
        }

        return true;
    }


    public static boolean exists(ZkClient zkClient, String path) {
        return zkClient.exists(path);
    }


    public static boolean delNode(ZkClient zkClient, String path) {
        return zkClient.delete(path);
    }


    public static List<String> getWorkers(ZkClient zkClient) {
        return zkClient.getChildren(Constants.SYNC_TASK_WORKER);
    }
}
