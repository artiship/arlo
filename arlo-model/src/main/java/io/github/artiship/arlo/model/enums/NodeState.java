package io.github.artiship.arlo.model.enums;

public enum NodeState {

    STANDBY(0), ACTIVE(1), DEAD(2), SHUTDOWN(3), UN_HEALTHY(4);

    private int code;

    NodeState(int code) {
        this.code = code;
    }

    public static NodeState of(int code) {
        for (NodeState nodeState : NodeState.values()) {
            if (nodeState.code == code) {
                return nodeState;
            }
        }
        throw new IllegalArgumentException("unsupported node status " + code);
    }

    public int getCode() {
        return this.code;
    }
}
