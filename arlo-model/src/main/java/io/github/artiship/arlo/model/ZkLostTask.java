package io.github.artiship.arlo.model;

import com.google.gson.Gson;
import io.github.artiship.arlo.model.enums.TaskState;
import lombok.Data;
import lombok.experimental.Accessors;

import java.time.LocalDateTime;

import static com.google.common.base.Charsets.UTF_8;
import static java.util.Objects.requireNonNull;

@Data
@Accessors(chain = true)
public class ZkLostTask {
    private static Gson gson = new Gson();
    private Long id;
    private TaskState state;
    private LocalDateTime endTime;

    public ZkLostTask(Long id, TaskState state, LocalDateTime endTime) {
        requireNonNull(id, "Task id is null");
        requireNonNull(state, "Task state is null");
        requireNonNull(endTime, "Task end time is null");

        this.id = id;
        this.state = state;
        this.endTime = endTime;
    }

    public static ZkLostTask of(Long id, TaskState state, LocalDateTime endTime) {
        return new ZkLostTask(id, state, endTime);
    }

    public static ZkLostTask from(String jsonStr) {
        return gson.fromJson(jsonStr, ZkLostTask.class);
    }

    public static ZkLostTask from(byte[] bytes) {
        return from(new String(bytes, UTF_8));
    }

    @Override
    public String toString() {
        return gson.toJson(this);
    }

    public byte[] toJsonBytes() {
        return this.toString()
                   .getBytes(UTF_8);
    }
}
