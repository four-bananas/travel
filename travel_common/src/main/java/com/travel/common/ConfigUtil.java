package com.travel.common;

import java.io.IOException;
import java.io.InputStream;
import java.util.Objects;
import java.util.Properties;

/**
 * 配置文件加载类
 */
public class ConfigUtil {
    static Properties properties = new Properties();

    static {
        try {
            // 使用ClassLoader加载properties配置文件生成对应的输入流
            InputStream in = ConfigUtil.class.getClassLoader()
                    .getResourceAsStream("config.properties");
            // 使用properties对象加载输入流
            properties.load(in);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }


    /**
     * 重置配置文件，传入相对路径resource下的文件
     * @param fileName
     */
    public static void resetProperties(String fileName){
        try {
            properties.clear();
            // 使用ClassLoader加载properties配置文件生成对应的输入流
            InputStream in = ConfigUtil.class.getClassLoader()
                    .getResourceAsStream(fileName);
            // 使用properties对象加载输入流
            properties.load(in);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * 重置配置文件，传入相对路径resource下的文件
     * @param properties
     */
    public static void resetProperties(Properties properties){
        ConfigUtil.properties = properties;
    }

    /**
     * 获取key对应的value值
     *
     * @param key
     * @return
     */
    public static String getConfig(String key) {
        return (String) properties.get(key);
    }
    public static String getStringConfig(String key) {
        return properties.getProperty(key);
    }
    public static String getStringConfig(String key, String defaultValue) {
        return properties.getProperty(key, defaultValue);
    }

    public static Integer getIntegerConfig(String key) {
        return Integer.parseInt(properties.getProperty(key));
    }
    public static Integer getIntegerConfig(String key, String defaultValue) {
        return Integer.parseInt(properties.getProperty(key, defaultValue));
    }

    public static Long getLongConfig(String key) {
        return Long.parseLong(properties.getProperty(key));
    }

    public static void main(String[] args) {
        String config = ConfigUtil.getConfig("KAFKA_BOOTSTRAP_SERVERS");
        System.out.println("config:" + config);
    }

}
