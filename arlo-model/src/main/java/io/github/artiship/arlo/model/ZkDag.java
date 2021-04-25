package io.github.artiship.arlo.model;

import lombok.Data;

import java.util.List;

import static com.google.common.base.Splitter.on;
import static java.lang.Integer.valueOf;
import static java.util.Objects.requireNonNull;

@Data
public class ZkDag {
    private final String ip;
    private final Integer port;

    public ZkDag(String ip, Integer port) {
        requireNonNull(ip, "Worker ip is null");
        requireNonNull(port, "Worker ip is null");

        this.ip = ip;
        this.port = port;
    }

    public static ZkDag from(String nodeInfoStr) {
        List<String> list = on(",").splitToList(nodeInfoStr);
        return new ZkDag(list.get(0), valueOf(list.get(1)));
    }

    @Override
    public String toString() {
        return ip + ":" + port;
    }
}
