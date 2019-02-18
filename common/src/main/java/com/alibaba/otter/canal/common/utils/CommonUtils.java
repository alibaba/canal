package com.alibaba.otter.canal.common.utils;

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
}
