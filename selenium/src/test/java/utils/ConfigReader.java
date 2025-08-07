package utils;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.Properties;

public class ConfigReader {
    private static final Properties properties = new Properties();

    static {
        try {
            FileInputStream fis = new FileInputStream("src/test/resources/application.properties");
            properties.load(fis);
        } catch (IOException e) {
            throw new RuntimeException("Failed to load application.properties file", e);
        }
    }

    public static String get(String key) {
        return properties.getProperty(key);
    }
}