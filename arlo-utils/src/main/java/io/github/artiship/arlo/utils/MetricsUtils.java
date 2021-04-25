package io.github.artiship.arlo.utils;

import oshi.SystemInfo;
import oshi.hardware.CentralProcessor;
import oshi.hardware.GlobalMemory;
import oshi.hardware.HardwareAbstractionLayer;

import java.math.BigDecimal;
import java.net.InetAddress;

public class MetricsUtils {

    private static SystemInfo systemInfo = new SystemInfo();


    public static GlobalMemory getMemory() {
        HardwareAbstractionLayer hal = systemInfo.getHardware();
        return hal.getMemory();
    }


    public static CentralProcessor getProcessor() {
        HardwareAbstractionLayer hal = systemInfo.getHardware();
        return hal.getProcessor();
    }


    public static double getMemoryTotal(GlobalMemory memory) {
        return memory.getTotal();
    }


    public static double getMemoryAvailable(GlobalMemory memory) {
        return memory.getAvailable();
    }


    public static double getMemoryUsage(GlobalMemory memory) {
        double available = getMemoryAvailable(memory);
        double total = getMemoryTotal(memory);
        BigDecimal bg = new BigDecimal(available / total * 100);
        return 100 - bg.setScale(2, BigDecimal.ROUND_HALF_UP)
                       .doubleValue();
    }


    public static double getMemoryUsage() {
        return getMemoryUsage(getMemory());
    }


    public static double getCpuUsage() {
        CentralProcessor processor = getProcessor();
        double useRate = processor.getSystemCpuLoadBetweenTicks();
        BigDecimal bg = new BigDecimal(useRate * 100);
        return bg.setScale(2, BigDecimal.ROUND_HALF_UP)
                 .doubleValue();
    }


    public static String getHostName() {
        InetAddress addr;
        String hostName;
        try {
            addr = InetAddress.getLocalHost();
            hostName = addr.getHostName();
        } catch (Exception e) {
            throw new RuntimeException("can not find host name");
        }
        return hostName;
    }


    public static String getHostIpAddress() {
        InetAddress addr;
        String ip;
        try {
            addr = InetAddress.getLocalHost();
            ip = addr.getHostAddress();
        } catch (Exception e) {
            throw new RuntimeException("can not find ip address");
        }
        return ip;
    }
}