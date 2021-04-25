package io.github.artiship.arlo.enums;

public enum OSInfo {

    Linux("linux"),

    Windows("windows");

    private static String OS = System.getProperty("os.name")
                                     .toLowerCase();
    private String description;

    private OSInfo(String desc) {
        this.description = desc;
    }

    public static OSInfo getOsInfo() {
        for (OSInfo osinfo : OSInfo.values()) {
            if (OS.indexOf(osinfo.getDescription()) >= 0) {
                return osinfo;
            }
        }
        return OSInfo.Linux;
    }

    public String getDescription() {
        return description;
    }
}