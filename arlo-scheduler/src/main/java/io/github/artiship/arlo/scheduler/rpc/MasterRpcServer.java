package io.github.artiship.arlo.scheduler.rpc;

import io.github.artiship.arlo.scheduler.core.Service;
import io.grpc.Server;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

import static io.grpc.ServerBuilder.forPort;

@Slf4j
@Component
public class MasterRpcServer implements Service {
    @Value("${rpc.port:9090}")
    private int DEFAULT_PORT = 9090;

    @Autowired
    private MasterRpcService masterRpcService;
    private Server server;

    @Override
    public void start() {
        try {
            server = forPort(DEFAULT_PORT).addService(masterRpcService)
                                          .build()
                                          .start();
        } catch (IOException e) {
            log.info("Rpc server start failed", e);
        }
    }

    @Override
    public void stop() {
        if (server != null) {
            try {
                server.shutdown()
                      .awaitTermination(5, TimeUnit.SECONDS);
            } catch (Exception e) {
                log.info("Rpc server stop failed", e);
            }
        }
    }
}