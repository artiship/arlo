package io.github.artiship.arlo.model.enums;

import lombok.Getter;

@Getter
public enum FileActionType {

    SHELL(1, "shell", "${shell_path}", "sh", ".sh"), PYTHON(2, "py", "${py_path}", "py", ".py"), SQL(3, "hql", "${hql_path}", "sql", ".sql"), TEXT(4, "text", "${text_path}", "txt", ".txt"), LOG(5, "log", "${log_path}", "log", ".log"), UPLOAD(6, "upload", "${upload_path}", "", ""), JAR(7, "jar", "${jar_path}", "jar", ""), OTHER(8, "other", "${other_path}", "", ""), YAML(9, "yaml", "${other_path}", "", ".yaml");

    private int code;
    private String dir;
    private String baseVariable;
    private String extension;
    private String desc;

    FileActionType(int code, String dir, String baseVariable, String extension, String desc) {
        this.dir = dir;
        this.baseVariable = baseVariable;
        this.code = code;
        this.extension = extension;
        this.desc = desc;
    }

    public static FileActionType of(int code) {
        for (FileActionType fleType : FileActionType.values()) {
            if (fleType.code == code) {
                return fleType;
            }
        }
        return OTHER;
    }

    public static FileActionType fromExtension(String extension) {
        for (FileActionType fleType : FileActionType.values()) {
            if (fleType.extension.equals(extension)) {
                return fleType;
            }
        }
        return OTHER;
    }

    public static FileActionType getTypeByJobType(JobType type) {
        if (type == JobType.SHELL || type == JobType.CALLABLE || type == JobType.FILE_CHECK || type == JobType.HIVE2MYSQL || type == JobType.MYSQL2HIVE || type == JobType.TABLEAU || type == JobType.SPARK_SUBMIT || type == JobType.YARN_MONITOR) {
            return FileActionType.SHELL;
        }
        if (type == JobType.PYTHON || type == JobType.DQC) {
            return FileActionType.PYTHON;
        }
        if (type == JobType.HQL) {
            return FileActionType.SQL;
        }
        return OTHER;
    }

    public String getDesc() {
        return this.desc;
    }

    public int getCode() {
        return this.code;
    }

    public String getDir() {
        return this.dir;
    }
}
