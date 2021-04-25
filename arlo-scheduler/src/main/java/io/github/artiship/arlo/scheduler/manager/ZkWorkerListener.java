package io.github.artiship.arlo.scheduler.manager;

import io.github.artiship.arlo.model.ZkWorker;
import io.github.artiship.arlo.scheduler.core.Service;
import io.github.artiship.arlo.scheduler.rest.service.SchedulerService;
import lombok.extern.slf4j.Slf4j;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.cache.PathChildrenCache;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheEvent;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheListener;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import javax.annotation.Resource;
import java.util.concurrent.TimeUnit;

import static com.google.common.base.Splitter.on;
import static com.google.common.collect.Iterables.getLast;
import static io.github.artiship.arlo.constants.GlobalConstants.DEAD_WORKER_GROUP;
import static io.github.artiship.arlo.constants.GlobalConstants.WORKER_GROUP;
import static io.github.artiship.arlo.utils.CuratorUtils.createPath;

@Slf4j
@Component
public class ZkWorkerListener implements PathChildrenCacheListener, Service {

    @Autowired
    private ResourceManager resourceManager;
    @Autowired
    private SchedulerService schedulerService;
    @Autowired
    private AlertService alertService;
    @Resource
    private CuratorFramework zkClient;
    private PathChildrenCache workers;

    @Override
    public void start() throws Exception {
        workers = new PathChildrenCache(zkClient, createPath(zkClient, WORKER_GROUP), true);
        workers.start();
        workers.getListenable()
               .addListener(this);
    }

    @Override
    public void stop() throws Exception {
        workers.close();
    }

    @Override
    public void childEvent(CuratorFramework client, PathChildrenCacheEvent event) {
        String path = event.getData()
                           .getPath();
        try {
            String ipAndPort = getLast(on("/").split(path));
            ZkWorker zkWorker = ZkWorker.from(ipAndPort);

            switch (event.getType()) {
                case CHILD_ADDED:
                    log.info("Worker added : {}", path);
                    resourceManager.activeWorker(zkWorker);
                    break;
                case CHILD_REMOVED:
                    log.info("Worker removed : {}", path);

                    try {
                        TimeUnit.SECONDS.sleep(10);
                    } catch (InterruptedException e) {
                        log.warn("Worker remove sleep was interrupted", e);
                    }

                    if (client.checkExists()
                              .forPath(path) == null) {
                        resourceManager.removeWorker(zkWorker);
                        schedulerService.failoverTasks(zkWorker.getIp());
                        createPath(zkClient, DEAD_WORKER_GROUP + "/" + zkWorker.toString());
                    }
                    alertService.workerDown(zkWorker.getIp());
                    break;
                case CHILD_UPDATED:
                default:
                    break;
            }
        } catch (Exception e) {
            log.error("Zk worker listener for {} causes exception", path, e);
        }
    }
}
