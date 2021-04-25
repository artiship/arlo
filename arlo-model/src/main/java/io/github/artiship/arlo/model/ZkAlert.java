package io.github.artiship.arlo.model;

import lombok.Data;
import lombok.extern.slf4j.Slf4j;

import java.util.List;

import static com.google.common.base.Splitter.on;
import static java.util.Objects.requireNonNull;


@Data
@Slf4j
public class ZkAlert {

    private final String ip;
    private final Integer port;

    public ZkAlert(String ip, Integer port) {
        requireNonNull(ip, "alert ip is null");
        requireNonNull(port, "alert port is null");

        this.ip = ip;
        this.port = port;
    }

    public static ZkAlert from(String nodeInfoStr) {
        log.info("nodeInfoStr :{}", nodeInfoStr);
        List<String> list = on(",").splitToList(nodeInfoStr);
        return new ZkAlert(list.get(0), 8890);
    }

    @Override
    public String toString() {
        return ip + ":" + port;
    }
}
