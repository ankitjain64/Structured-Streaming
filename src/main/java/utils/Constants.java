package utils;

import java.util.Date;

public class Constants {
    public static final String USER_A = "userA";
    public static final String USER_B = "userB";
    public static final String INTERACTION = "interaction";
    public static final String TIMESTAMP = "ts";

    public static long getCurrentTime() {
        return new Date().getTime();
    }
}
