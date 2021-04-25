package io.github.artiship.arlo.scheduler.executor.zk;

import io.grpc.Server;
import org.junit.Test;

import java.io.IOException;

import static io.grpc.ServerBuilder.forPort;

public class ZkClientTest {
    @Test
    public void test() throws IOException {
        Server server = forPort(9090).addService(new GrpcService())
                                     .build();

        server.start();
        System.out.println("test server started.");
        try {
            server.awaitTermination();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
}
