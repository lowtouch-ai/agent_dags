package utils;

public class ConfigReader {

    public static String get(String key) {
        // First Check environment variable
        String envValue = System.getenv(key);
        if (envValue != null && !envValue.isEmpty()) {
            return envValue;
        }

        // Fallback to system property
        String sysValue = System.getProperty(key);
        if (sysValue != null && !sysValue.isEmpty()) {
            return sysValue;
        }

        // Fallback to application.properties if present
        try (java.io.InputStream input = ConfigReader.class.getClassLoader().getResourceAsStream("application.properties")) {
            if (input != null) {
                java.util.Properties props = new java.util.Properties();
                props.load(input);
                return props.getProperty(key);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }

        throw new RuntimeException("Config key not found: " + key);
    }
}
