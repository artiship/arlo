package io.github.artiship.arlo.scheduler.manager;

import io.github.artiship.arlo.scheduler.core.Service;
import io.github.artiship.arlo.scheduler.core.exception.TaskNotFoundException;
import io.github.artiship.arlo.scheduler.rest.service.SchedulerService;
import lombok.extern.slf4j.Slf4j;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.cache.PathChildrenCache;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheEvent;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheListener;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import javax.annotation.Resource;

import static io.github.artiship.arlo.constants.GlobalConstants.LOST_TASK_GROUP;
import static io.github.artiship.arlo.model.ZkLostTask.from;
import static io.github.artiship.arlo.utils.CuratorUtils.createPath;
import static java.util.concurrent.CompletableFuture.runAsync;

@Slf4j
@Component
public class ZkLostTaskListener implements PathChildrenCacheListener, Service {

    @Autowired
    private SchedulerService schedulerService;
    @Resource
    private CuratorFramework zkClient;
    private PathChildrenCache lostTasks;

    @Override
    public void start() throws Exception {
        lostTasks = new PathChildrenCache(zkClient, createPath(zkClient, LOST_TASK_GROUP), true);
        lostTasks.start();
        lostTasks.getListenable()
                 .addListener(this);
    }

    @Override
    public void stop() throws Exception {
        lostTasks.close();
    }

    @Override
    public void childEvent(CuratorFramework client, PathChildrenCacheEvent event) throws Exception {
        String path = event.getData()
                           .getPath();

        switch (event.getType()) {
            case CHILD_ADDED:
                log.info("Lost task added : {}", path);
                this.handleLostTask(path);
                break;
            case CHILD_REMOVED:
                log.info("Lost task removed : {}", path);
                break;
            case CHILD_UPDATED:
            default:
                break;
        }
    }

    public void handleLostTask(String path) {
        runAsync(() -> {
            try {
                schedulerService.updateLostTask(from(zkClient.getData()
                                                             .forPath(path)));
                zkClient.delete()
                        .forPath(path);
            } catch (TaskNotFoundException e) {
                try {
                    zkClient.delete()
                            .forPath(path);
                } catch (Exception ex) {
                    log.error("Remove lost task from zk {} fail", path, e);
                }
            } catch (Exception e) {
                log.error("Remove lost task zk node {} fail", path, e);
            }
        });
    }
}
