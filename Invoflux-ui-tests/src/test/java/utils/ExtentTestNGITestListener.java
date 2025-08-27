package utils;

import org.testng.ITestContext;
import org.testng.ITestListener;
import org.testng.ITestResult;

import com.aventstack.extentreports.ExtentTest;

public class ExtentTestNGITestListener implements ITestListener {
    @Override
    public void onTestStart(ITestResult result) {
        ExtentTest test = ExtentReportManager.getInstance()
                .createTest(result.getMethod().getMethodName());
        result.setAttribute("extentTest", test);
    }

    @Override
    public void onTestSuccess(ITestResult result) {
        ExtentTest test = (ExtentTest) result.getAttribute("extentTest");
        if (test != null) {
            test.pass("Test passed");
        }
    }

    @Override
    public void onTestFailure(ITestResult result) {
        ExtentTest test = (ExtentTest) result.getAttribute("extentTest");
        if (test != null) {
            test.fail(result.getThrowable());
        }
    }

    @Override
    public void onTestSkipped(ITestResult result) {
        ExtentTest test = (ExtentTest) result.getAttribute("extentTest");
        if (test != null) {
            test.skip("Test skipped");
        }
    }

    @Override
    public void onFinish(ITestContext context) {
        ExtentReportManager.flush();
    }
}
