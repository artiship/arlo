package io.github.artiship.arlo.scheduler.worker.common;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.Lists;
import io.github.artiship.arlo.scheduler.core.model.SchedulerTaskBo;

import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

public class CommonCache {
    private static AtomicInteger CURRENT_TASK_CNT = new AtomicInteger(0);
    private static ConcurrentHashMap<Long, Long> CACHE_PID = new ConcurrentHashMap<>();
    private static ConcurrentHashMap<Long, String> CACHE_APPLICATIONID = new ConcurrentHashMap<>();
    private static ConcurrentHashMap<Long, SchedulerTaskBo> CACHE_TASK = new ConcurrentHashMap<>();
    private static LoadingCache<Long, Integer> KILLING_TASK_IDS =
            CacheBuilder.newBuilder()
                        .expireAfterWrite(60, TimeUnit.SECONDS)
                        .build(new CacheLoader<Long, Integer>() {
                            @Override
                            public Integer load(Long key) {
                                return 0;
                            }
                        });

    public static boolean isKillTask(Long taskId) {
        try {
            return KILLING_TASK_IDS.get(taskId) != 0;
        } catch (Exception e) {
            return false;
        }
    }

    public static void addKillTask(Long taskId) {
        KILLING_TASK_IDS.put(taskId, 1);
    }

    public static void cachePid(SchedulerTaskBo task, Long pid) {
        synchronized (CACHE_PID) {
            CACHE_PID.put(task.getId(), pid);
            CACHE_TASK.put(task.getId(), task);
        }
    }

    public static Long getPid(SchedulerTaskBo task) {
        synchronized (CACHE_PID) {
            return CACHE_PID.get(task.getId());
        }
    }

    public static void removePid(SchedulerTaskBo taskBo) {
        synchronized (CACHE_PID) {
            CACHE_PID.remove(taskBo.getId());
            CACHE_TASK.remove(taskBo.getId());
        }

    }

    public static void cacheApplicationId(SchedulerTaskBo task, String applicationId) {
        synchronized (CACHE_APPLICATIONID) {
            CACHE_APPLICATIONID.put(task.getId(), applicationId);
        }
    }

    public static String getApplicationId(SchedulerTaskBo task) {
        synchronized (CACHE_APPLICATIONID) {
            return CACHE_APPLICATIONID.get(task.getId());
        }

    }

    public static void removeApplicationId(SchedulerTaskBo task) {
        synchronized (CACHE_APPLICATIONID) {
            CACHE_APPLICATIONID.remove(task.getId());
        }

    }

    public static void addOneTask() {
        CURRENT_TASK_CNT.incrementAndGet();
    }

    public static int currTaskCnt() {
        return CURRENT_TASK_CNT.get();
    }

    public static void delOneTask() {
        CURRENT_TASK_CNT.decrementAndGet();
    }

    public static List<SchedulerTaskBo> cloneRunningTask() {
        synchronized (CACHE_PID) {
            List<SchedulerTaskBo> tasks = Lists.newArrayListWithExpectedSize(CACHE_PID.size());
            CACHE_TASK.entrySet()
                      .forEach((entry) -> tasks.add(entry.getValue()));
            return tasks;
        }
    }
}
