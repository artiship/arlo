package io.github.artiship.arlo.scheduler.worker.thread;

import com.google.common.util.concurrent.ThreadFactoryBuilder;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.RejectedExecutionHandler;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;


public class ThreadPoolFactory {


    public static ThreadPoolExecutor create(String threadPoolName, int maxCorePoolSize) {
        return create(threadPoolName, maxCorePoolSize, maxCorePoolSize, 0L,
                TimeUnit.MILLISECONDS, new SynchronousQueue<>(), new ThreadPoolExecutor.AbortPolicy());
    }


    private static ThreadPoolExecutor create(String threadPoolName, int corePoolSize, int maxPoolSize, long keepAliveTime,
                                             TimeUnit unit, BlockingQueue<Runnable> queue, RejectedExecutionHandler rejectedExecutionHandler) {
        StringBuilder nameFormatBuilder = new StringBuilder(threadPoolName);
        nameFormatBuilder.append("-%d");
        ThreadFactory threadFactory = new ThreadFactoryBuilder().setNameFormat(nameFormatBuilder.toString())
                                                                .build();
        return new ThreadPoolExecutor(corePoolSize, maxPoolSize, keepAliveTime, unit,
                queue, threadFactory, rejectedExecutionHandler);
    }
}
