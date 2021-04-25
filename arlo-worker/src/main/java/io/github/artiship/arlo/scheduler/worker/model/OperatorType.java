package io.github.artiship.arlo.scheduler.worker.model;

import lombok.Getter;


public enum OperatorType {

    SUBMIT(1), KILL(2);

    @Getter
    private int code;

    OperatorType(int code) {
        this.code = code;
    }

}
