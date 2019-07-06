package org.kin.framework.utils;

import java.io.*;
import java.net.URL;
import java.util.Properties;

/**
 * @author huangjianqin
 * @date 2019/7/6
 */
public class PropertiesUtils {

    public static Properties loadPropertie(String propertyFileName) {
        // disk path
        if (propertyFileName.startsWith("file:")) {
            propertyFileName = propertyFileName.substring("file:".length());
            return loadFileProperties(propertyFileName);
        } else {
            return loadClassPathProperties(propertyFileName);
        }
    }

    public static Properties loadClassPathProperties(String propertyFileName) {
        InputStream in = null;
        try {
            in = Thread.currentThread().getContextClassLoader().getResourceAsStream(propertyFileName);
            if (in == null) {
                return null;
            }

            Properties prop = new Properties();
            prop.load(new InputStreamReader(in, "utf-8"));
            return prop;
        } catch (IOException e) {
            ExceptionUtils.log(e);
        } finally {
            if (in != null) {
                try {
                    in.close();
                } catch (IOException e) {
                    ExceptionUtils.log(e);
                }
            }
        }
        return null;
    }


    public static Properties loadFileProperties(String propertyFileName) {
        InputStream in = null;
        try {
            // load file location, disk
            File file = new File(propertyFileName);
            if (!file.exists()) {
                return null;
            }

            URL url = new File(propertyFileName).toURI().toURL();
            in = new FileInputStream(url.getPath());
            if (in == null) {
                return null;
            }

            Properties prop = new Properties();
            prop.load(new InputStreamReader(in, "utf-8"));
            return prop;
        } catch (IOException e) {
            ExceptionUtils.log(e);
        } finally {
            if (in != null) {
                try {
                    in.close();
                } catch (IOException e) {
                    ExceptionUtils.log(e);
                }
            }
        }
        return null;
    }

    public static boolean writeFileProperties(Properties properties, String filePathName){
        FileOutputStream out = null;
        try {

            // mk file
            File file = new File(filePathName);
            if (!file.exists()) {
                file.getParentFile().mkdirs();
            }

            // write data
            out = new FileOutputStream(file, false);
            properties.store(new OutputStreamWriter(out, "utf-8"), null);
            return true;
        } catch (IOException e) {
            ExceptionUtils.log(e);
            return false;
        } finally {
            if (out != null) {
                try {
                    out.close();
                } catch (IOException e) {
                    ExceptionUtils.log(e);
                }
            }
        }
    }
}
