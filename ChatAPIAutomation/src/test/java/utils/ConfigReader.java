package utils;

public class ConfigReader {

    public static String get(String key) {
        String value = System.getenv(key);
        if (value == null) {
            throw new RuntimeException("Environment variable '" + key + "' not found");
        }
        return value;
    }
}