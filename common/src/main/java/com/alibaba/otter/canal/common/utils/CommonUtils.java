package com.alibaba.otter.canal.common.utils;

import org.apache.commons.lang.ArrayUtils;

import java.io.File;

/**
 * 通用工具类
 *
 * @author rewerma 2019-01-25 下午05:20:16
 * @version 1.0.0
 */
public class CommonUtils {

    /**
     * 获取conf文件夹所在路径
     *
     * @return 路径地址
     */
    public static String getConfPath() {
        String classpath = CommonUtils.class.getResource("/").getPath();
        String confPath = classpath + "../conf/";
        if (new File(confPath).exists()) {
            return confPath;
        } else {
            return classpath;
        }
    }

    /**
     * 删除文件夹
     *
     * @param dirFile 文件夹对象
     * @return 是否删除成功
     */
    public static boolean deleteDir(File dirFile) {
        if (!dirFile.exists()) {
            return false;
        }

        if (dirFile.isFile()) {
            return dirFile.delete();
        } else {
            File[] files = dirFile.listFiles();
            if (files == null || files.length == 0) {
                return dirFile.delete();
            }
            for (File file : files) {
                deleteDir(file);
            }
        }

        return dirFile.delete();
    }


    /**
     * 拆分IP地址和端口号
     *
     * @param text ip地址和端口号，ip和端口号以英文冒号(:)分隔;
     *             ipv4 127.0.0.1:3306
     *             ipv6 [::1]:3306
     * @return
     */
    public static String[] splitIPAndPort(String text) {
        text = text.replace("[", "").replace("]", "");
        int idx = text.lastIndexOf(':');
        if (idx > 0) {
            String ip = text.substring(0, idx);
            String port = text.substring(idx + 1);
            return new String[]{ip, port};
        }
        return ArrayUtils.EMPTY_STRING_ARRAY;
    }
}
