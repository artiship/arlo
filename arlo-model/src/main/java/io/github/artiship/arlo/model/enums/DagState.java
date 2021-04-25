package io.github.artiship.arlo.model.enums;

import java.util.List;

import static java.util.Arrays.asList;
import static java.util.stream.Collectors.toList;

public enum DagState {

    SUBMITTED(0),
    PENDING(1),
    LIMITED(11),
    RUNNING(2),
    COMPLETED(3),
    STOPPED(4),
    FAIL(5);

    private int code;

    DagState(int code) {
        this.code = code;
    }

    public static DagState of(int code) {
        for (DagState batchState : DagState.values()) {
            if (batchState.code == code) {
                return batchState;
            }
        }
        throw new IllegalArgumentException("unsupported batch state type " + code);
    }

    public static List<DagState> finishedStates() {
        return asList(COMPLETED, STOPPED, FAIL);
    }

    public static List<Integer> finishedStateCodes() {
        return finishedStates().stream()
                               .map(s -> s.getCode())
                               .collect(toList());
    }

    public int getCode() {
        return this.code;
    }
}
