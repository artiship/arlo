package io.github.artiship.arlo.utils;

import java.util.HashMap;
import java.util.Map;

public class StringUtils {

    private StringUtils() {

    }


    public static Map<String, String> coverString2Map(String map) {
        Map<String, String> result = new HashMap<>();
        if (map.startsWith("{")) {
            map = map.substring(1);
        }
        if (map.endsWith("}")) {
            map = map.substring(0, map.length() - 1);
        }
        String[] out = map.replaceAll(" ", "")
                          .split(",");
        for (String anOut : out) {
            String[] inn = anOut.split("=");
            result.put(inn[0], inn[1]);
        }
        return result;
    }

}