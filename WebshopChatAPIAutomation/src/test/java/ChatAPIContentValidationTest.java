import com.aventstack.extentreports.ExtentReports;
import com.aventstack.extentreports.ExtentTest;
import com.aventstack.extentreports.reporter.ExtentSparkReporter;
import com.google.gson.*;

import org.testng.Assert;
import org.testng.annotations.*;

import utils.ConfigReader;

import java.io.*;
import java.text.SimpleDateFormat;
import java.util.*;

public class ChatAPIContentValidationTest {

    //private static final String API_URL = ConfigReader.get("api.url");
    //private static final String BEARER_TOKEN = ConfigReader.get("api.token");
	private static final String API_URL = System.getenv("API_URL");
    private static final String BEARER_TOKEN = System.getenv("API_TOKEN");
    private static ExtentReports extent;
    private static ThreadLocal<ExtentTest> testReport = new ThreadLocal<>();

    // Collect failed tests
    private static List<String> failedTests = new ArrayList<>();

    @BeforeSuite
    public void setupReport() {
        ExtentSparkReporter sparkReporter = new ExtentSparkReporter("ChatAPI_TestReport.html");
        sparkReporter.config().setDocumentTitle("Chat API Test Report");
        sparkReporter.config().setReportName("AI Response Keyword Validation");

        extent = new ExtentReports();
        extent.attachReporter(sparkReporter);
    }

    @AfterSuite
    public void flushReportAndWriteFailures() {
        extent.flush();

        if (!failedTests.isEmpty()) {
            // Add timestamp to filename
            String timestamp = new SimpleDateFormat("yyyyMMdd_HHmmss").format(new Date());
            File outFile = new File("failed_tests_" + timestamp + ".txt");

            try (FileWriter fw = new FileWriter(outFile)) {
                for (String fail : failedTests) {
                    fw.write(fail + System.lineSeparator());
                }
            } catch (IOException e) {
                e.printStackTrace();
            }

            System.out.println("Failed test summary written to: " + outFile.getAbsolutePath());
        } else {
            System.out.println("All tests passed. No failed_tests file generated.");
        }
    }

    @DataProvider(name = "messagePayloads")
    public Object[][] getMessagePayloads() {
        return new Object[][]{
            {
                "Show me order details of 788, including stock availability and customer details, and highlight the articles that are low or out of stock",
                new String[]{
                    "Patricia Montgomery",
                    "patricia.montgomery@example.com",
                    "Gym Bag Mune"
                }
            },
            {
                "Plot a bar chart comparing revenue and order count for December 2024 sales revenue by day",
                new String[]{
                    "chart", "bar", "Revenue"
                }
            },
            {
                "Compose a well-formatted American-style business email with a quote for existing customer Enni Kivi for articles 1139 and 3876, apply discounts and shipping as per policies, include the total, shipping, and discount in the quote. Use a single table to show all the articles, subtotal, discount, shipping and total; Add a summary section with total before discount, total discount ($ and %), subtotal, shipping and total.",
                new String[]{
                    "Enni Kivi", "fred@webshop.com"
                }
            }
        };
    }

    @Test(dataProvider = "messagePayloads")
    public void testChatAPIResponseValidation(String message, String[] expectedFragments) throws Exception {
        ExtentTest test = extent.createTest("Prompt: " + message);
        testReport.set(test);
        test.info("Prompt: " + message);
        System.out.println("url: " + API_URL);
        System.out.println("Token: " + BEARER_TOKEN);

        // Create JSON payload
        JsonObject messageObj = new JsonObject();
        messageObj.addProperty("role", "user");
        messageObj.addProperty("content", message);

        JsonArray messagesArray = new JsonArray();
        messagesArray.add(messageObj);

        JsonObject payloadObj = new JsonObject();
        payloadObj.addProperty("model", "webshop:0.5");
        payloadObj.add("messages", messagesArray);

        // Write payload to temporary JSON file
        File tempFile = File.createTempFile("payload", ".json");
        try (FileWriter fw = new FileWriter(tempFile)) {
            fw.write(payloadObj.toString());
        }

        // Build and run curl command
        String[] curlCommand = {
            "curl", "-X", "POST", API_URL,
            "-H", "Authorization: Bearer " + BEARER_TOKEN,
            "-H", "Content-Type: application/json",
            "--data", "@" + tempFile.getAbsolutePath(),
            "-s"
        };

        ProcessBuilder pb = new ProcessBuilder(curlCommand);
        Process process = pb.start();

        BufferedReader reader = new BufferedReader(new InputStreamReader(process.getInputStream()));
        StringBuilder responseBuffer = new StringBuilder();
        String line;
        while ((line = reader.readLine()) != null) {
            responseBuffer.append(line);
        }
        process.waitFor();
        tempFile.delete();

        String response = responseBuffer.toString();
        test.info("Raw Response:<br><pre>" + escapeHtml(response) + "</pre>");

        JsonObject jsonResponse = JsonParser.parseString(response).getAsJsonObject();
        String content = jsonResponse
                .getAsJsonArray("choices")
                .get(0).getAsJsonObject()
                .getAsJsonObject("message")
                .get("content").getAsString();

        test.info("Parsed Content:<br><pre>" + escapeHtml(content) + "</pre>");

     // Assertion check with detailed logging
        boolean allPassed = true;
        StringBuilder missingFragments = new StringBuilder();

        for (String expected : expectedFragments) {
            if (content.contains(expected)) {
                test.pass("Found: **" + expected + "**");
                System.out.println("[PASS] Found: " + expected);
            } else {
                test.fail("Missing: **" + expected + "**");
                System.out.println("[FAIL] Missing: " + expected);
                allPassed = false;
                missingFragments.append(expected).append(", ");
            }
        }

        if (allPassed) {
            System.out.println("Prompt PASSED: " + message);
        } else {
            String failSummary = "Prompt FAILED: " + message + " | Missing: " + missingFragments;
            failedTests.add(failSummary);
            System.out.println(failSummary);
        }

        Assert.assertTrue(allPassed, "One or more expected fragments were not found in the response.");
    }

    private String escapeHtml(String input) {
        return input.replaceAll("&", "&amp;")
                .replaceAll("<", "&lt;")
                .replaceAll(">", "&gt;");
    }
}
