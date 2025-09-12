package utils;

import org.testng.ITestContext;
import org.testng.ITestListener;
import org.testng.ITestResult;

import java.util.ArrayList;
import java.util.List;

public class ExtentTestNGITestListener implements ITestListener {

    private static List<String> passedTests = new ArrayList<>();
    private static List<String> failedTests = new ArrayList<>();

    @Override
    public void onTestSuccess(ITestResult result) {
        passedTests.add(result.getMethod().getMethodName());
    }

    @Override
    public void onTestFailure(ITestResult result) {
        failedTests.add(result.getMethod().getMethodName());
    }

    @Override
    public void onFinish(ITestContext context) {
        System.out.println("========= TEST EXECUTION SUMMARY =========");
        System.out.println("PASSED TESTS:");
        passedTests.forEach(t -> System.out.println(" # " + t));

        System.out.println("FAILED TESTS:");
        if (failedTests.isEmpty()) {
            System.out.println(" None ");
        } else {
            failedTests.forEach(t -> System.out.println(" X " + t));
        }
        System.out.println("==========================================");
    }
}
