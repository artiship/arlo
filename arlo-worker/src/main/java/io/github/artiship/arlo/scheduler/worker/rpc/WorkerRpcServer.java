package io.github.artiship.arlo.scheduler.worker.rpc;

import io.grpc.Server;
import io.grpc.ServerBuilder;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.io.IOException;

@Component
@Slf4j
public class WorkerRpcServer {
    private Server server;

    @Value("${arlo.worker.server.port}")
    private int serverPort;

    @Autowired
    private WorkerRpcServiceImpl workerRpcService;

    public void startServer() throws IOException {
        server = ServerBuilder.forPort(serverPort)
                              .addService(workerRpcService)
                              .build()
                              .start();
        log.info("worker service started.");
    }

    public void blockUntilShutdown() throws InterruptedException {
        if (server != null) {
            server.awaitTermination();
        }
    }

    public void shutdownNow() {
        if (server != null) {
            server.shutdownNow();
        }
    }
}
