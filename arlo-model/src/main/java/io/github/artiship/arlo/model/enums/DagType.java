package io.github.artiship.arlo.model.enums;

public enum DagType {

    STATIC(1, "DEPRECATE: User should plan a static dag"),
    DYNAMIC(2, "DEPRECATE: Dag would be dynamically generated by job dependencies"),

    SINGLE(3, "补单个作业"),
    DOWN_STREAM(4, "补下游");


    private int code;
    private String desc;

    DagType(int code, String desc) {
        this.code = code;
        this.desc = desc;
    }

    public static DagType of(int code) {
        for (DagType dagType : DagType.values()) {
            if (dagType.code == code) {
                return dagType;
            }
        }
        throw new IllegalArgumentException("unsupported dag type " + code);
    }

    public int getCode() {
        return this.code;
    }
}
