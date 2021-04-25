package io.github.artiship.arlo.model.enums;

public enum NodeType {

    MASTER(1), WORKER(2);

    private int code;

    NodeType(int code) {
        this.code = code;
    }

    public static NodeType of(int code) {
        for (NodeType nodeType : NodeType.values()) {
            if (nodeType.code == code) {
                return nodeType;
            }
        }
        throw new IllegalArgumentException("unsupported node type " + code);
    }

    public int getCode() {
        return this.code;
    }
}
