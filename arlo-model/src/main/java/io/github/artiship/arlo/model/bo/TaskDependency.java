package io.github.artiship.arlo.model.bo;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.reflect.TypeToken;
import lombok.Data;
import lombok.experimental.Accessors;

import java.util.Objects;
import java.util.Set;

import static java.util.Collections.emptySet;

@Data
@Accessors(chain = true)
public class TaskDependency {
    private final static Gson gson = new GsonBuilder().serializeNulls()
                                                      .create();
    private Long jobId;
    private Long taskId;
    private String scheduleTime;
    private String calTimeRange;
    private String taskCron;
    private boolean isReady = false;

    public TaskDependency() {
    }

    public TaskDependency(Long jobId, String scheduleTime, String calTimeRange) {
        this.jobId = jobId;
        this.scheduleTime = scheduleTime;
        this.calTimeRange = calTimeRange;
    }

    public static Set<TaskDependency> parseTaskDependenciesJson(String taskDecenciesJson) {
        if (taskDecenciesJson == null || taskDecenciesJson.length() == 0)
            return emptySet();

        Set<TaskDependency> taskDependencies = gson.fromJson(taskDecenciesJson,
                new TypeToken<Set<TaskDependency>>() {
                }.getType());

        if (taskDependencies != null) return taskDependencies;

        return emptySet();
    }

    public static String toTaskDependenciesJson(Set<TaskDependency> taskDependencies) {
        if (taskDependencies == null)
            return "";

        return gson.toJson(taskDependencies);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        TaskDependency that = (TaskDependency) o;
        return jobId.equals(that.jobId) &&
                calTimeRange.equals(that.calTimeRange);
    }

    @Override
    public int hashCode() {
        return Objects.hash(jobId, calTimeRange);
    }

    @Override
    public String toString() {
        return "TaskDependency{" +
                "jobId=" + jobId +
                ", taskId=" + taskId +
                ", scheduleTime='" + scheduleTime + '\'' +
                ", calTimeRange='" + calTimeRange + '\'' +
                ", taskCron='" + taskCron + '\'' +
                '}';
    }
}
