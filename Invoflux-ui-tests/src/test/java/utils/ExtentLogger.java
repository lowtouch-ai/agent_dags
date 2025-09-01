package utils;

import com.aventstack.extentreports.ExtentTest;
import org.testng.ITestResult;
import org.testng.Reporter;

public class ExtentLogger {
    public static void log(String message) {
        ITestResult result = Reporter.getCurrentTestResult();
        ExtentTest test = (ExtentTest) result.getAttribute("extentTest");
        if (test != null) {
            test.info(message);
        }
    }
}
