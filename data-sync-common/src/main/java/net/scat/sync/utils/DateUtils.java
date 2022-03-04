package net.scat.sync.utils;

import org.apache.commons.lang3.time.DateFormatUtils;

import java.util.Date;

public class DateUtils {
    private final static String default_pattern = "yyyy-MM-dd HH:mm:ss";
    public static String format(Date date) {
        return DateFormatUtils.format(date, default_pattern);
    }

}
