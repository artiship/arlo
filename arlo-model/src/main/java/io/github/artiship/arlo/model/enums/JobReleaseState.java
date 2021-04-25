package io.github.artiship.arlo.model.enums;

import java.util.Arrays;
import java.util.List;


public enum JobReleaseState {

    DRAFT(-1), OFFLINE(0), ONLINE(1), DELETED(-2);

    private int code;

    JobReleaseState(int code) {
        this.code = code;
    }

    public static JobReleaseState of(int code) {
        for (JobReleaseState state : JobReleaseState.values()) {
            if (state.code == code) {
                return state;
            }
        }
        throw new IllegalArgumentException("unsupported job state " + code);
    }

    public static List<Integer> getOfflineAndOnline() {
        return Arrays.asList(JobReleaseState.OFFLINE.getCode(), JobReleaseState.ONLINE.getCode());
    }

    public static List<Integer> getNotOnline() {
        return Arrays.asList(JobReleaseState.OFFLINE.getCode(), JobReleaseState.DRAFT.getCode(), JobReleaseState.DELETED.getCode());
    }

    public int getCode() {
        return this.code;
    }
}
