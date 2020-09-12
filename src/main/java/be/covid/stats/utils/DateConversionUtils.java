package be.covid.stats.utils;

import java.time.LocalDate;
import java.time.format.DateTimeFormatter;

public class DateConversionUtils {
    public static DateTimeFormatter DEFAULT_DATE_FORMAT = DateTimeFormatter.ofPattern("yyyyMMdd");
    public static DateTimeFormatter JSON_DATE_FORMAT = DateTimeFormatter.ofPattern("yyyy-MM-dd");

    public static String convert(LocalDate date, DateTimeFormatter dateFormat) {
        return date.format(dateFormat);
    }
}
