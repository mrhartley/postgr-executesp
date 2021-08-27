package com.appiancorp.ps;

public class ESPUtils {

    private static Integer DEFAULT_TIMEOUT_SECS = 14400; //4 hours

    private static boolean isNumeric(String str){
        return str != null && str.matches("[0-9.]+");
    }

    public static Integer getTimeoutSecs(String timeout){
        if(isNumeric(timeout) && !timeout.isEmpty()) {
            return Integer.valueOf(timeout);
        } else {
            return DEFAULT_TIMEOUT_SECS;
        }
    }
}
