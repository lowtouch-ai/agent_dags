import org.openqa.selenium.*;
import org.openqa.selenium.chrome.ChromeDriver;
import org.openqa.selenium.chrome.ChromeOptions;
import org.openqa.selenium.support.ui.ExpectedConditions;
import org.openqa.selenium.support.ui.WebDriverWait;
import org.testng.Assert;
import org.testng.annotations.*;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.testng.annotations.Listeners;
import utils.EmailUtils;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Duration;
import java.util.Comparator;
import java.util.List;
import java.util.Properties;
import java.util.UUID;
import java.util.stream.Collectors;

@Listeners(utils.ExtentTestNGITestListener.class)
public class InvofluxTests {
    Logger log = LogManager.getLogger(InvofluxTests.class);

    WebDriver driver;
    Properties config;
    String baseUrl;
    WebDriverWait wait;
    private String tempProfileDir;
    
    private static final String FROM_EMAIL = System.getenv("FROM_EMAIL");
    private static final String APP_PASSWORD = System.getenv("GMAIL_TOKEN"); // generated from Gmail
    private static final String TO_EMAIL = System.getenv("INVOFLUX_AGENT_EMAIL");
    

    @BeforeClass
    public void setupAll() throws IOException {
        config = new Properties();
        FileInputStream fis = new FileInputStream("config.properties");
        config.load(fis);

        ChromeOptions options = new ChromeOptions();

        tempProfileDir = "/tmp/chrome-profile-" + UUID.randomUUID();
        Files.createDirectories(Paths.get(tempProfileDir));

        options.addArguments("--user-data-dir=" + tempProfileDir);
        options.addArguments("--profile-directory=Profile-" + UUID.randomUUID());
        options.addArguments("--no-sandbox");
        options.addArguments("--disable-dev-shm-usage");
        options.addArguments("--disable-gpu");
        options.addArguments("--remote-allow-origins=*");
        options.addArguments("--disable-extensions");
        options.addArguments("--disable-popup-blocking");
        // options.addArguments("--headless=new"); // if needed in CI

        driver = new ChromeDriver(options);
        driver.manage().timeouts().implicitlyWait(Duration.ofSeconds(10));
        driver.manage().window().maximize();
        wait = new WebDriverWait(driver, Duration.ofSeconds(60));

        //baseUrl = config.getProperty("base.url");
        baseUrl = System.getenv("BASE_URL");
        log.info("Launching browser and navigating to: " + baseUrl);
        driver.get(baseUrl);

        log.info("Attempting to login...");
        utils.ExtentLogger.log("Navigating to login page");

        wait.until(ExpectedConditions.visibilityOfElementLocated(By.name("email")));
        //driver.findElement(By.name("email")).sendKeys(config.getProperty("login.email"));
        //driver.findElement(By.name("current-password")).sendKeys(config.getProperty("login.password"));
        
        driver.findElement(By.name("email")).sendKeys(System.getenv("LOGIN_EMAIL"));
        driver.findElement(By.name("current-password")).sendKeys(System.getenv("LOGIN_PASSWORD"));
        
        driver.findElement(By.xpath("//button[contains(text(),'Sign in')]")).click();

        wait.until(ExpectedConditions.visibilityOfElementLocated(By.id("sidebar-toggle-button")));
        Assert.assertTrue(driver.findElement(By.id("sidebar-toggle-button")).isDisplayed(), "Login failed or dashboard not visible.");

        log.info("Login successful.");
        utils.ExtentLogger.log("Login successful.");
    }

    @AfterClass(alwaysRun = true)
    public void tearDown() {
        if (driver != null) {
            driver.quit();
        }

        // Kill leftover Chrome processes (Linux-safe)
        try {
            String os = System.getProperty("os.name").toLowerCase();
            if (!os.contains("win")) {
                Runtime.getRuntime().exec("pkill -f chrome");
            }
        } catch (IOException e) {
            e.printStackTrace();
        }

        // Clean up temp Chrome profile directory
        if (tempProfileDir != null) {
            try {
                Files.walk(Paths.get(tempProfileDir))
                    .sorted(Comparator.reverseOrder())
                    .map(Path::toFile)
                    .forEach(File::delete);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }


   @Test(priority = 1)
    public void test_prompt_invoice_detail_extraction_smart_vision() throws InterruptedException {
        log.info("Executing Smart Vision Invoice details prompt test...");
        utils.ExtentLogger.log("Test started: Smart Vision Invoice details");

        WebElement dropdown = driver.findElement(By.xpath("//button[@aria-label='Select a model']"));
        dropdown.click();
        utils.ExtentLogger.log("Model dropdown clicked.");

        driver.findElement(By.xpath("//div[contains(@class,'line-clamp')][contains(.,'InvoFlux')]")).click();
        utils.ExtentLogger.log("Model 'InvoFlux' selected.");

        Thread.sleep(5000);
        
        utils.ExtentLogger.log("Upload files....");
        
     // Locate the hidden input[type=file]
        WebElement fileInput = driver.findElement(By.cssSelector("input[type='file']"));

        // Convert to absolute path
        String filePath = Paths.get("src/test/java/resources/smartvision.png").toAbsolutePath().toString();
        System.out.println("File Path: " + filePath);
        // Send file path
        fileInput.sendKeys(filePath);

        WebElement prompt_option = wait.until(ExpectedConditions.elementToBeClickable(By.xpath("//button/div/div[contains(.,'extract the invoice  and prepare for system entry')]")));
        prompt_option.click();
        driver.findElement(By.id("send-message-button")).click();
        utils.ExtentLogger.log("Prompt sent to extract invoice details - Smart Vision");
        wait.until(ExpectedConditions.visibilityOfElementLocated(By.xpath("//p[contains(.,'Summary of Invoice')]")));
        utils.ExtentLogger.log("Invoice Summary is displaying with details");
        // --- Header Details ----
        // Vendor Name
        String vendorText = driver.findElement(By.xpath("//li[contains(.,'Vendor Name')]")).getText();
        Assert.assertTrue(
                vendorText.contains("Smart Vision for Information Systems"),
                "Vendor Name mismatch! Actual text: " + vendorText
        );
        utils.ExtentLogger.log(vendorText +" found");

        // Invoice Number
        String invoiceText = driver.findElement(By.xpath("//li[contains(.,'Invoice Number')]")).getText();
        Assert.assertTrue(
                invoiceText.contains("SV-I-0018223-20"),
                "Invoice Number mismatch! Actual text: " + invoiceText
        );
        utils.ExtentLogger.log(invoiceText +" found");

        // Due Date
        String dueDateText = driver.findElement(By.xpath("//li[contains(.,'Due Date')]")).getText();
        
        Assert.assertTrue(
                !(dueDateText.isEmpty()),
                "Due Date is not found!"
        );
        utils.ExtentLogger.log(dueDateText +" found");

        // Subtotal
        String subtotalText = driver.findElement(By.xpath("//li[contains(.,'Subtotal')]")).getText();
        Assert.assertTrue(
                subtotalText.contains("59,062.50"),
                "Subtotal mismatch! Actual text: " + subtotalText
        );
        utils.ExtentLogger.log(subtotalText +" found");

        // Tax Amount
        String taxText = driver.findElement(By.xpath("//li[contains(.,'Tax Amount')]")).getText();
        Assert.assertTrue(
                taxText.contains("2,953.13"),
                "Tax Amount mismatch! Actual text: " + taxText
        );
        utils.ExtentLogger.log(taxText +" found");


        // Total Amount
        String totalText = driver.findElement(By.xpath("//li[contains(.,'Total Amount')]")).getText();
        Assert.assertTrue(
                totalText.contains("62,015.63"),
                "Total Amount mismatch! Actual text: " + totalText
        );
        utils.ExtentLogger.log(totalText +" found");


        // Currency
        String currencyText = driver.findElement(By.xpath("//li[contains(.,'Currency')]")).getText();
        Assert.assertTrue(
                currencyText.contains("AED"),
                "Currency mismatch! Actual text: " + currencyText
        );
        utils.ExtentLogger.log(currencyText +" found");


        // Purchase Order
        String poText = driver.findElement(By.xpath("//li[contains(.,'Purchase Order')]")).getText();
        Assert.assertTrue(
                poText.contains("1017768"),
                "Purchase Order mismatch! Actual text: " + poText
        );
        utils.ExtentLogger.log(poText +" found");


        // ---- Product Line Details ----
        WebElement itemRow = driver.findElement(By.xpath("//table//tr[contains(@class,'bg-gray')]"));

        String itemName = itemRow.findElement(By.xpath("./td[contains(@class,'text-gray')][1]")).getText();
        String quantity = itemRow.findElement(By.xpath("./td[contains(@class,'text-gray')][2]")).getText();
        String unitPrice = itemRow.findElement(By.xpath("./td[contains(@class,'text-gray')][3]")).getText();
        String taxPercentage = itemRow.findElement(By.xpath("./td[contains(@class,'text-gray')][4]")).getText();
        String totalPrice = itemRow.findElement(By.xpath("./td[contains(@class,'text-gray')][5]")).getText();
        utils.ExtentLogger.log("Product lines details is displaying in tabular format...");

        Assert.assertTrue(
                itemName.contains("Sales AMC Enterprise Solution"),
                "Item Name mismatch! Expected: 'Sales AMC Enterprise Solution' but Actual: " + itemName
        );
        utils.ExtentLogger.log("Item Name: 'Sales AMC Enterprise Solution' found");

        
        Assert.assertTrue(
                quantity.contains("1"),
                "Quantity mismatch! Expected: '1' but Actual: " + quantity
        );
        utils.ExtentLogger.log("Item Quantity: '1' found");

        
      
        Assert.assertTrue(
                unitPrice.contains("59,062.50"),
                "Unit Price mismatch! Expected: '59,062.50' but Actual: " + unitPrice 
        );
        utils.ExtentLogger.log("Unit Price: '59,062.50' found");

       
        Assert.assertTrue(
                taxPercentage.contains("5%"),
                "Tax Percentage mismatch! Expected: '5%' but Actual: " + taxPercentage
        );
        utils.ExtentLogger.log("Tax Percentage: '5%' found");


        Assert.assertTrue(
                totalPrice.contains("62,015.63"),
                "Total Price mismatch! Expected: '62,015.63' but Actual: " + totalPrice
        );
        utils.ExtentLogger.log("Total Price: '62,015.63' found");

        
        wait.until(ExpectedConditions.visibilityOfElementLocated(By.xpath("//div[@id='chat-input']/p")));
        WebElement chatbox = driver.findElement(By.xpath("//div[@id='chat-input']/p"));
        chatbox.sendKeys("Yes");
        driver.findElement(By.id("send-message-button")).click();
        utils.ExtentLogger.log("Confirm prom...
        