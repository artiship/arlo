package io.github.artiship.arlo.scheduler.model.dependency;

public enum DependencyType {
    DEFAULT(1),
    CUSTOMIZE(2);

    private int code;

    DependencyType(int code) {
        this.code = code;
    }

    public static DependencyType of(int code) {
        for (DependencyType dependencyType : values()) {
            if (dependencyType.code == code) {
                return dependencyType;
            }
        }
        throw new IllegalArgumentException("Unsupported dependency type " + code);
    }

    public int getCode() {
        return this.code;
    }
}
