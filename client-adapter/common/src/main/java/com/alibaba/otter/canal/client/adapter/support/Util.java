package com.alibaba.otter.canal.client.adapter.support;

import java.io.File;
import java.net.URL;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.*;
import java.time.format.DateTimeFormatter;
import java.util.Date;
import java.util.TimeZone;
import java.util.function.Consumer;
import java.util.function.Function;

import javax.sql.DataSource;

import org.apache.commons.lang.StringUtils;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;

public class Util {

    private static final Logger logger = LoggerFactory.getLogger(Util.class);

    /**
     * 通过DS执行sql
     */
    public static Object sqlRS(DataSource ds, String sql, Function<ResultSet, Object> fun) throws SQLException {
        Connection conn = null;
        Statement stmt = null;
        ResultSet rs = null;
        try {
            conn = ds.getConnection();
            stmt = conn.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
            stmt.setFetchSize(Integer.MIN_VALUE);
            rs = stmt.executeQuery(sql);
            return fun.apply(rs);
        } finally {
            if (rs != null) {
                try {
                    rs.close();
                } catch (SQLException e) {
                    logger.error(e.getMessage(), e);
                }
            }
            if (stmt != null) {
                try {
                    stmt.close();
                } catch (SQLException e) {
                    logger.error(e.getMessage(), e);
                }
            }
            if (conn != null) {
                try {
                    conn.close();
                } catch (SQLException e) {
                    logger.error(e.getMessage(), e);
                }
            }
        }
    }

    /**
     * sql执行获取resultSet
     *
     * @param conn sql connection
     * @param sql sql
     * @param consumer 回调方法
     */
    public static void sqlRS(Connection conn, String sql, Consumer<ResultSet> consumer) {
        Statement stmt = null;
        ResultSet rs = null;
        try {
            stmt = conn.createStatement();
            rs = stmt.executeQuery(sql);
            consumer.accept(rs);
        } catch (SQLException e) {
            logger.error(e.getMessage(), e);
        } finally {
            if (rs != null) {
                try {
                    rs.close();
                } catch (SQLException e) {
                    logger.error(e.getMessage(), e);
                }
            }
            if (stmt != null) {
                try {
                    stmt.close();
                } catch (SQLException e) {
                    logger.error(e.getMessage(), e);
                }
            }
        }
    }

    public static File getConfDirPath() {
        return getConfDirPath("");
    }

    public static File getConfDirPath(String subConf) {
        URL url = Util.class.getClassLoader().getResource("");
        String path;
        if (url != null) {
            path = url.getPath();
        } else {
            path = new File("").getAbsolutePath();
        }
        File file = null;
        if (path != null) {
            file = new File(
                path + ".." + File.separator + Constant.CONF_DIR + File.separator + StringUtils.trimToEmpty(subConf));
            if (!file.exists()) {
                file = new File(path + StringUtils.trimToEmpty(subConf));
            }
        }
        if (file == null || !file.exists()) {
            throw new RuntimeException("Config dir not found.");
        }

        return file;
    }

    public static String cleanColumn(String column) {
        if (column == null) {
            return null;
        }
        if (column.contains("`")) {
            column = column.replaceAll("`", "");
        }

        if (column.contains("'")) {
            column = column.replaceAll("'", "");
        }

        if (column.contains("\"")) {
            column = column.replaceAll("\"", "");
        }

        return column;
    }

    private static String UTC; // 当前时区

    static {
        TimeZone timeZone = TimeZone.getTimeZone(ZoneId.systemDefault().getId());
        int currentTimeZone = timeZone.getOffset(System.currentTimeMillis()) * 10 / (1000 * 3600);
        int tmp = currentTimeZone % 10;
        int m = 60 * tmp / 10;
        UTC = "+" + String.format("%02d", currentTimeZone / 10) + ":" + String.format("%02d", m);
        TimeZone.setDefault(TimeZone.getTimeZone("GMT" + UTC));
    }

    /**
     * 通用日期时间字符解析
     *
     * @param datetimeStr 日期时间字符串
     * @return Date
     */
    public static Date parseDate(String datetimeStr) {
        if (StringUtils.isEmpty(datetimeStr)) {
            return null;
        }
        datetimeStr = datetimeStr.trim();
        if (datetimeStr.contains("-")) {
            if (datetimeStr.contains(":")) {
                datetimeStr = datetimeStr.replace(" ", "T");
            }
        } else if (datetimeStr.contains(":")) {
            datetimeStr = "T" + datetimeStr;
        }

        DateTime dateTime = new DateTime(datetimeStr, DateTimeZone.forID(UTC));

        return dateTime.toDate();
    }

    public static LoadingCache<String, DateTimeFormatter> dateFormatterCache = CacheBuilder.newBuilder()
        .build(new CacheLoader<String, DateTimeFormatter>() {

            @Override
            public DateTimeFormatter load(String key) {
                return DateTimeFormatter.ofPattern(key);
            }
        });

    public static Date parseDate2(String datetimeStr) {
        if (StringUtils.isEmpty(datetimeStr)) {
            return null;
        }
        try {
            datetimeStr = datetimeStr.trim();
            int len = datetimeStr.length();
            if (datetimeStr.contains("-") && datetimeStr.contains(":") && datetimeStr.contains(".")) {
                // 包含日期+时间+毫秒
                // 取毫秒位数
                int msLen = len - datetimeStr.indexOf(".") - 1;
                StringBuilder ms = new StringBuilder();
                for (int i = 0; i < msLen; i++) {
                    ms.append("S");
                }
                String formatter = "yyyy-MM-dd HH:mm:ss." + ms;

                DateTimeFormatter dateTimeFormatter = dateFormatterCache.get(formatter);
                LocalDateTime dateTime = LocalDateTime.parse(datetimeStr, dateTimeFormatter);
                return Date.from(dateTime.atZone(ZoneId.systemDefault()).toInstant());
            } else if (datetimeStr.contains("-") && datetimeStr.contains(":")) {
                // 包含日期+时间
                // 判断包含时间位数
                int i = datetimeStr.indexOf(":");
                i = datetimeStr.indexOf(":", i + 1);
                String formatter;
                if (i > -1) {
                    formatter = "yyyy-MM-dd HH:mm:ss";
                } else {
                    formatter = "yyyy-MM-dd HH:mm";
                }

                DateTimeFormatter dateTimeFormatter = dateFormatterCache.get(formatter);
                LocalDateTime dateTime = LocalDateTime.parse(datetimeStr, dateTimeFormatter);
                return Date.from(dateTime.atZone(ZoneId.systemDefault()).toInstant());
            } else if (datetimeStr.contains("-")) {
                // 只包含日期
                String formatter = "yyyy-MM-dd";
                DateTimeFormatter dateTimeFormatter = dateFormatterCache.get(formatter);
                LocalDate localDate = LocalDate.parse(datetimeStr, dateTimeFormatter);
                return Date.from(localDate.atStartOfDay().atZone(ZoneId.systemDefault()).toInstant());
            } else if (datetimeStr.contains(":")) {
                // 只包含时间
                String formatter;
                if (datetimeStr.contains(".")) {
                    // 包含毫秒
                    int msLen = len - datetimeStr.indexOf(".") - 1;
                    StringBuilder ms = new StringBuilder();
                    for (int i = 0; i < msLen; i++) {
                        ms.append("S");
                    }
                    formatter = "HH:mm:ss." + ms;
                } else {
                    // 判断包含时间位数
                    int i = datetimeStr.indexOf(":");
                    i = datetimeStr.indexOf(":", i + 1);
                    if (i > -1) {
                        formatter = "HH:mm:ss";
                    } else {
                        formatter = "HH:mm";
                    }
                }
                DateTimeFormatter dateTimeFormatter = dateFormatterCache.get(formatter);
                LocalTime localTime = LocalTime.parse(datetimeStr, dateTimeFormatter);
                LocalDate localDate = LocalDate.of(1970, Month.JANUARY, 1);
                LocalDateTime localDateTime = LocalDateTime.of(localDate, localTime);
                return Date.from(localDateTime.atZone(ZoneId.systemDefault()).toInstant());
            }
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
        }

        return null;
    }
}
