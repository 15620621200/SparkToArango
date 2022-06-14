package com.cmbc.util;/*
 * @Package com.cmbc.util
 * @author wang shuangli
 * @date 2022-05-13 17:28
 * @version V1.0
 * @Copyright © 2015-2021
 */

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.util.Properties;

public class PropertiesUtil {


    public static Properties prop = new Properties();

    private static PropertiesUtil instance = new PropertiesUtil();
    private PropertiesUtil(){}
    public static PropertiesUtil getInstance(){
        return instance;
    }

    /**
     * 加载配置文件sateudp.properties
     */
    public static void initProperties(String filePath, String hdfsUser) {
//        String sateudp = System.getProperty("user.dir") + File.separator +"src\\main\\resources"+File.separator +"sateudp.properties";
//        String sateudp = "hdfs://192.168.138.131:9000/test/sateudp.properties";
        try {
//            "hdfs://192.168.138.131:9000/"
            int index = filePath.lastIndexOf(":") + 6;
            String uri = filePath.substring(0, index);
            FileSystem fileSystem = HdfsUtils.getHadoopFileSystem(uri, hdfsUser);
            prop.load(new InputStreamReader(fileSystem.open(new Path(filePath)), StandardCharsets.UTF_8));
        } catch (IOException | InterruptedException | URISyntaxException e) {
            e.printStackTrace();
        }
    }

    public static void initProperties(String filePath) {
//        String sateudp = System.getProperty("user.dir") + File.separator +"src\\main\\resources"+File.separator +"sateudp.properties";
//        String sateudp = "hdfs://192.168.138.131:9000/test/sateudp.properties";
        try {
          prop.load(new FileInputStream(new File(filePath)));
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static Object getProperty(String name) {
        return prop.get(name);
    }

    public static String getString(String name) {
        Object value = prop.get(name);
        System.out.println(name + ":" + value);
        if (value != null) {
            return value.toString();
        }
        return "";
    }

    public static String getString(String name,String defaultValue) {
        Object value = prop.get(name);
        System.out.println(name + ":" + value);
        if (value != null) {
            return value.toString();
        }
        return defaultValue;
    }

    public static long getLong(String name) {
        Object value = prop.get(name);
        System.out.println(name + ":" + value);
        if (value != null) {
            return Integer.parseInt(value.toString());
        }
        return 0;
    }
    public static long getLong(String name,Long defaultValue) {
        Object value = prop.get(name);
        System.out.println(name + ":" + value);
        if (value != null) {
            return Integer.parseInt(value.toString());
        }
        return defaultValue;
    }

    public static int getInt(String name) {
        Object value = prop.get(name);
        System.out.println(name + ":" + value);
        if (value != null) {
            return Integer.parseInt(value.toString());
        }
        return 0;
    }
    public static int getInt(String name,int defaultValue) {
        Object value = prop.get(name);
        System.out.println(name + ":" + value);
        if (value != null) {
            return Integer.parseInt(value.toString());
        }
        return defaultValue;
    }

    public static double getDouble(String name) {
        Object value = prop.get(name);
        System.out.println(name + ":" + value);
        if (value != null) {
            return Double.parseDouble(value.toString());
        }
        return 0;
    }
    public static double getDouble(String name,double defaultValue) {
        Object value = prop.get(name);
        System.out.println(name + ":" + value);
        if (value != null) {
            return Double.parseDouble(value.toString());
        }
        return defaultValue;
    }

    public static boolean getBoolean(String name) {
        Object value = prop.get(name);
        System.out.println(name + ":" + value);
        if (value != null) {
            return "true".equals(value.toString());
        }
        return false;
    }
    public static boolean getBoolean(String name,Boolean defaultValue) {
        Object value = prop.get(name);
        System.out.println(name + ":" + value);
        if (value != null) {
            return "true".equals(value.toString());
        }
        return defaultValue;
    }
}

