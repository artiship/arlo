package io.github.artiship.arlo.scheduler.config;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import static org.apache.curator.framework.CuratorFrameworkFactory.newClient;

@Configuration
public class AppConfig {
    @Value("${zookeeper.quorum}")
    private String zkUrl;
    @Value("${zookeeper.sessionTimeoutMs:15000}")
    private int sessionTimeoutMs;
    @Value("${zookeeper.connectionTimoutMs:60000}")
    private int connectionTimeoutMs;
    @Value("${zookeeper.retryWait:5000}")
    private int retryWait;
    @Value("${zookeeper.retryAttempts:3}")
    private int reconnectAttempts;

    @Bean
    public CuratorFramework zkClient() {
        final CuratorFramework client = newClient(
                zkUrl,
                sessionTimeoutMs,
                connectionTimeoutMs,
                new ExponentialBackoffRetry(retryWait, reconnectAttempts));
        client.start();
        return client;
    }
}
