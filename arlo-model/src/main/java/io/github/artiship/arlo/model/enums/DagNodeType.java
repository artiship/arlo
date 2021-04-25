package io.github.artiship.arlo.model.enums;

public enum DagNodeType {

    JOB(1), TASK(2);

    private int code;

    DagNodeType(int code) {
        this.code = code;
    }

    public static DagNodeType of(int code) {
        for (DagNodeType dagNodeType : DagNodeType.values()) {
            if (dagNodeType.code == code) {
                return dagNodeType;
            }
        }
        throw new IllegalArgumentException("unsupported dag node type " + code);
    }

    public int getCode() {
        return this.code;
    }
}
