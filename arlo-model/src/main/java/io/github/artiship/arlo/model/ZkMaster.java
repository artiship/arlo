package io.github.artiship.arlo.model;

import lombok.AllArgsConstructor;
import lombok.Data;

import java.util.List;

import static com.google.common.base.Splitter.on;
import static java.lang.Integer.valueOf;

@Data
@AllArgsConstructor
public class ZkMaster {
    private String ip;
    private Integer rcpPort;
    private Integer httpPort;

    public static ZkMaster from(String zkNodeInfo) {
        List<String> list = on(":").splitToList(zkNodeInfo);
        return new ZkMaster(list.get(0), valueOf(list.get(1)), valueOf(list.get(2)));
    }

    @Override
    public String toString() {
        return ip + ":" + rcpPort + ":" + httpPort;
    }
}
