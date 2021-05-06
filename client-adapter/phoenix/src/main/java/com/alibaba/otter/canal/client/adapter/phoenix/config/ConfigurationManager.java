package com.alibaba.otter.canal.client.adapter.phoenix.config;

import java.io.InputStream;
import java.util.Properties;

/**
 * 配置管理组件
 * @author Administrator
 *
 */
public class ConfigurationManager {

	private static Properties prop = new Properties();

	static {
		try {
			InputStream in = ConfigurationManager.class
					.getClassLoader().getResourceAsStream("phoenix/phoenix_common.properties");
			prop.load(in);  
		} catch (Exception e) {
			e.printStackTrace();  
		}
	}
	
	/**
	 * 获取指定key对应的value
	 *
	 * @param key 
	 * @return 返回value是字符串
	 */
	public static String getProperty(String key) {
		return prop.getProperty(key);
	}
	
	/**
	 * 获取整数类型的配置项
	 * @param key StringKye
	 * @return value
	 */
	public static Integer getInteger(String key) {
		String value = getProperty(key);
		try {
			return Integer.valueOf(value);
		} catch (Exception e) {
			e.printStackTrace();
		}
		return 0;
	}
}
