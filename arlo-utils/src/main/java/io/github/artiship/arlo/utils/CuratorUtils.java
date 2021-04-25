package io.github.artiship.arlo.utils;

import com.google.common.base.Throwables;
import io.github.artiship.arlo.model.ZkMaster;
import lombok.extern.slf4j.Slf4j;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;

import static io.github.artiship.arlo.constants.GlobalConstants.MASTER_GROUP;
import static io.github.artiship.arlo.model.ZkMaster.from;
import static java.nio.charset.StandardCharsets.UTF_8;

@Slf4j
public class CuratorUtils {
    private static final int ZK_CONNECTION_TIMEOUT_MILLIS = 15000;
    private static final int ZK_SESSION_TIMEOUT_MILLIS = 60000;
    private static final int RETRY_WAIT_MILLIS = 5000;
    private static final int MAX_RECONNECT_ATTEMPTS = 3;

    public static CuratorFramework newClient(String zkUrl) {
        final CuratorFramework client = CuratorFrameworkFactory.newClient(zkUrl,
                ZK_SESSION_TIMEOUT_MILLIS, ZK_CONNECTION_TIMEOUT_MILLIS,
                new ExponentialBackoffRetry(RETRY_WAIT_MILLIS, MAX_RECONNECT_ATTEMPTS));
        client.start();
        return client;
    }

    public static ZkMaster activeMaster(CuratorFramework zk) throws Exception {
        return from(new String(zk.getData()
                                 .forPath(MASTER_GROUP), UTF_8));
    }

    public static String createPath(CuratorFramework zkClient, String path) throws Exception {
        if (zkClient.checkExists()
                    .forPath(path) == null) {
            try {
                zkClient.create()
                        .creatingParentsIfNeeded()
                        .forPath(path);
            } catch (Exception e) {
                log.error("Zk path {} exists", path, e);
                Throwables.throwIfUnchecked(e);
            }
        }
        return path;
    }

    public static String createPath(CuratorFramework zkClient, String path, byte[] data) throws Exception {
        if (zkClient.checkExists()
                    .forPath(path) == null) {
            try {
                zkClient.create()
                        .creatingParentsIfNeeded()
                        .forPath(path, data);
            } catch (Exception e) {
                log.error("Zk path {} exists", path, e);
                Throwables.throwIfUnchecked(e);
            }
        }
        return path;
    }
}
