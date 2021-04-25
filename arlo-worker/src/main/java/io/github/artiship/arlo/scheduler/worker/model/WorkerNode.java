package io.github.artiship.arlo.scheduler.worker.model;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;


@Data
@EqualsAndHashCode
@AllArgsConstructor
@NoArgsConstructor
public class WorkerNode {

    private String ip;

    private int port;


    public String toString() {
        StringBuilder stringBuilder = new StringBuilder();
        stringBuilder.append(ip)
                     .append(":")
                     .append(port);
        return stringBuilder.toString();
    }
}
