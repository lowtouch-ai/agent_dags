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

        Path tempProfile = Files.createTempDirectory("chrome-profile-");
        ChromeOptions options = new ChromeOptions();
        // options.addArguments("--user-data-dir=" + tempProfile.toString());
        options.addArguments("--no-sandbox");
        options.addArguments("--disable-dev-shm-usage");
        options.addArguments("--disable-gpu");
        options.addArguments("--remote-allow-origins=*");
          
        // Optional: run in headless mode for CI
        options.addArguments("--headless=new");

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
        utils.ExtentLogger.log("Confirm prompt sent to create the vendor bill for Smart Vision");
        
        wait.until(ExpectedConditions.visibilityOfElementLocated(By.xpath("//p[contains(.,'The vendor bill has been created successfully in draft status')]")));

        utils.ExtentLogger.log("vendor bill detail is visible");
        
        // Invoice Number
        String invoiceNumberText = driver.findElement(By.xpath("//li[contains(.,'Invoice Number')]")).getText();
        Assert.assertTrue(
                invoiceNumberText.contains("SV-I-0018223-20"),
                "Invoice Number mismatch! Expected: 'SV-I-0018223-20' but Actual text: " + invoiceNumberText
        );
        utils.ExtentLogger.log("Invoice Number: 'SV-I-0018223-20' found");


        // Invoice ID
        WebElement invoiceIdText = driver.findElement(By.xpath("//li[contains(.,'Invoice ID')]"));
        Assert.assertTrue(
                invoiceIdText.isDisplayed(),
                "Invoice ID not found!"
        );
        utils.ExtentLogger.log("Invoice ID: "+invoiceIdText+" found");


        // State
        String stateText = driver.findElement(By.xpath("//li[contains(.,'State')]")).getText();
        Assert.assertTrue(
                stateText.contains("Draft"),
                "State mismatch! Expected: 'Draft' but Actual text: " + stateText
        );
        utils.ExtentLogger.log("State: "+stateText+" found");


        // Validation Result - General message
        String validationResultText = driver.findElement(By.xpath("//li[contains(.,'Validation Result')]")).getText();
        Assert.assertTrue(
                validationResultText.contains("The invoice validation failed"),
                "Validation Result message mismatch! Actual text: " + validationResultText
        );

        // Validation Result - Specific Issues
        List<String> validationIssues = driver.findElements(By.xpath("//strong[contains(.,'Validation Result')]/following-sibling::ul/li"))
                                              .stream()
                                              .map(WebElement::getText)
                                              .collect(Collectors.toList());

        // Check each expected issue
        Assert.assertTrue(
                validationIssues.stream().anyMatch(t -> t.contains("Sales AMC Enterprise Solution")),
                "Expected product not found issue missing! Actual: " + validationIssues
        );

        Assert.assertTrue(
                validationIssues.stream().anyMatch(t -> t.contains("Duplicate invoice number 'SV-I-0018223-20' found")),
                "Expected duplicate invoice number issue missing! Actual: " + validationIssues
        );

        Assert.assertTrue(
                validationIssues.stream().anyMatch(t -> t.contains("No valid invoice lines created")),
                "Expected missing invoice lines issue missing! Actual: " + validationIssues
        );

        log.info("Smart Vision Invoice details prompt test completed.");
    }

    @Test(priority = 2)
    public void test_prompt_invoice_detail_extraction_seer_tech() throws InterruptedException {
        log.info("Executing Invoice extraction prompt test for Seer tech...");
        utils.ExtentLogger.log("Test started: Invoice extraction prompt test for Seer tech");

        WebElement sideToggle = driver.findElement(By.id("sidebar-toggle-button"));
        sideToggle.click();

        WebElement newChat_button = driver.findElement(By.id("sidebar-new-chat-button"));
        newChat_button.click();

        utils.ExtentLogger.log("Upload files....");
        
        // Locate the hidden input[type=file]
           WebElement fileInput = driver.findElement(By.cssSelector("input[type='file']"));

           // Convert to absolute path
           String filePath = Paths.get("src/test/java/resources/seertech.png").toAbsolutePath().toString();
           System.out.println("File Path: " + filePath);
           // Send file path
           fileInput.sendKeys(filePath);

           WebElement prompt_option = wait.until(ExpectedConditions.elementToBeClickable(By.xpath("//button/div/div[contains(.,'extract the invoice  and prepare for system entry')]")));
           prompt_option.click();
           driver.findElement(By.id("send-message-button")).click();
           utils.ExtentLogger.log("Prompt sent to extract invoice details - Smart Vision");
           wait.until(ExpectedConditions.visibilityOfElementLocated(By.xpath("//p[contains(.,'Summary of Invoice')]")));
           Thread.sleep(50000);
           // --- Header Details ----
           // Vendor Name
           String vendorText = driver.findElement(By.xpath("//li[contains(.,'Vendor Name')]")).getText();
           Assert.assertTrue(
                   vendorText.contains("Seertech Solutions Pty Ltd"),
                   "Vendor Name mismatch! Actual text: " + vendorText
           );

           // Invoice Number
           String invoiceText = driver.findElement(By.xpath("//li[contains(.,'Invoice Number')]")).getText();
           Assert.assertTrue(
                   invoiceText.contains("2110"),
                   "Invoice Number mismatch! Actual text: " + invoiceText
           );

           // Due Date
           String dueDateText = driver.findElement(By.xpath("//li[contains(.,'Due Date')]")).getText();
           /*
           Assert.assertTrue(
                   dueDateText.contains("30 days from invoice date"),
                   "Due Date mismatch! Actual text: " + dueDateText
           );
           */

           // Subtotal
           String subtotalText = driver.findElement(By.xpath("//li[contains(.,'Subtotal')]")).getText();
           Assert.assertTrue(
                   subtotalText.contains("0"),
                   "Subtotal mismatch! Actual text: " + subtotalText
           );

           // Tax Amount
           String taxText = driver.findElement(By.xpath("//li[contains(.,'Tax Amount')]")).getText();
           Assert.assertTrue(
                   taxText.contains("0"),
                   "Tax Amount mismatch! Actual text: " + taxText
           );

           // Total Amount
           String totalText = driver.findElement(By.xpath("//li[contains(.,'Total Amount')]")).getText();
           Assert.assertTrue(
                   totalText.contains("44,920.75"),
                   "Total Amount mismatch! Actual text: " + totalText
           );

           // Currency
           String currencyText = driver.findElement(By.xpath("//li[contains(.,'Currency')]")).getText();
           Assert.assertTrue(
                   currencyText.contains("AED"),
                   "Currency mismatch! Actual text: " + currencyText
           );

           // Purchase Order
           String poText = driver.findElement(By.xpath("//li[contains(.,'Purchase Order')]")).getText();
           Assert.assertTrue(
                   poText.contains("24681"),
                   "Purchase Order mismatch! Actual text: " + poText
           );

           // ---- Product Line Details ----
           WebElement itemRow = driver.findElement(By.xpath("(//table//tr[contains(@class,'bg-gray')])[1]"));

           String itemName = itemRow.findElement(By.xpath("./td[contains(@class,'text-gray')][1]")).getText();
           String quantity = itemRow.findElement(By.xpath("./td[contains(@class,'text-gray')][2]")).getText();
           String unitPrice = itemRow.findElement(By.xpath("./td[contains(@class,'text-gray')][3]")).getText();
           String taxPercentage = itemRow.findElement(By.xpath("./td[contains(@class,'text-gray')][4]")).getText();
           String totalPrice = itemRow.findElement(By.xpath("./td[contains(@class,'text-gray')][5]")).getText();

           Assert.assertTrue(
                   itemName.contains("iLearning PLUS - ADAC Staff (User Subscription License)"),
                   "Item Name mismatch! Actual: " + itemName
           );
           
           Assert.assertTrue(
                   quantity.contains("1"),
                   "Quantity mismatch! Actual: " + quantity
           );
           
         
           Assert.assertTrue(
                   unitPrice.contains("33,030.00"),
                   "Unit Price mismatch! Actual: " + unitPrice
           );
           
           Assert.assertTrue(
                   taxPercentage.contains("0%"),
                   "Tax Percentage mismatch! Actual: " + taxPercentage
           );

           Assert.assertTrue(
                   totalPrice.contains("33,030.00"),
                   "Total Price mismatch! Actual: " + totalPrice
           );
           
           wait.until(ExpectedConditions.visibilityOfElementLocated(By.xpath("//div[@id='chat-input']/p")));
           WebElement chatbox = driver.findElement(By.xpath("//div[@id='chat-input']/p"));
           chatbox.sendKeys("Yes");
           driver.findElement(By.id("send-message-button")).click();
           utils.ExtentLogger.log("Confirm prompt sent to create the vendor bill for Smart Vision");
           
           wait.until(ExpectedConditions.visibilityOfElementLocated(By.xpath("//p[contains(.,'The vendor bill has been created successfully')]")));

           utils.ExtentLogger.log("vendor bill detail is visible");
           
           // Invoice Number
           String invoiceNumberText = driver.findElement(By.xpath("//li[contains(.,'Invoice Number')]")).getText();
           Assert.assertTrue(
                   invoiceNumberText.contains("2110180845"),
                   "Invoice Number mismatch! Actual text: " + invoiceNumberText
           );

           // Invoice ID
           WebElement invoiceIdText = driver.findElement(By.xpath("//li[contains(.,'Invoice ID')]"));
           Assert.assertTrue(
                   invoiceIdText.isDisplayed(),
                   "Invoice ID not found!"
           );

           // State
           String stateText = driver.findElement(By.xpath("//li[contains(.,'State')]")).getText();
           Assert.assertTrue(
                   stateText.contains("Posted"),
                   "State mismatch! Actual text: " + stateText
           );

          
           log.info("SeerTech invoice extraction details prompt test completed.");
    }

    @Test(priority = 3)
    public void test_prompt_invoice_detail_extraction_honey_well() throws InterruptedException {
        log.info("Honeywell invoice extraction prompt test...");
        utils.ExtentLogger.log("Test started: Invoice extraction for Honeywell");

        WebElement newChat_button = driver.findElement(By.id("sidebar-new-chat-button"));
        newChat_button.click();

        utils.ExtentLogger.log("Upload files....");
        
        // Locate the hidden input[type=file]
           WebElement fileInput = driver.findElement(By.cssSelector("input[type='file']"));

           // Convert to absolute path
           String filePath = Paths.get("src/test/java/resources/honeywell.png").toAbsolutePath().toString();
           System.out.println("File Path: " + filePath);
           // Send file path
           fileInput.sendKeys(filePath);

           WebElement prompt_option = wait.until(ExpectedConditions.elementToBeClickable(By.xpath("//button/div/div[contains(.,'extract the invoice  and prepare for system entry')]")));
           prompt_option.click();
           driver.findElement(By.id("send-message-button")).click();
           utils.ExtentLogger.log("Prompt sent to extract invoice details - Honeywell");
           wait.until(ExpectedConditions.visibilityOfElementLocated(By.xpath("//p[contains(.,'Summary of Invoice')]")));
           utils.ExtentLogger.log("Invoice Summary is displaying with details");
           // --- Header Details ----
           // Vendor Name
           String vendorText = driver.findElement(By.xpath("//li[contains(.,'Vendor Name')]")).getText();
           Assert.assertTrue(
                   vendorText.contains("Honeywell International ME Limited AD"),
                   "Vendor Name mismatch! Actual text: " + vendorText
           );
           utils.ExtentLogger.log(vendorText +" found");

           // Invoice Number
           String invoiceText = driver.findElement(By.xpath("//li[contains(.,'Invoice Number')]")).getText();
           Assert.assertTrue(
                   invoiceText.contains("5253783660"),
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
                   subtotalText.contains("1,995,000.00"),
                   "Subtotal mismatch! Actual text: " + subtotalText
           );
           utils.ExtentLogger.log(subtotalText +" found");

           // Tax Amount
           String taxText = driver.findElement(By.xpath("//li[contains(.,'Tax Amount')]")).getText();
           Assert.assertTrue(
                   taxText.contains("99,750.00"),
                   "Tax Amount mismatch! Actual text: " + taxText
           );
           utils.ExtentLogger.log(taxText +" found");


           // Total Amount
           String totalText = driver.findElement(By.xpath("//li[contains(.,'Total Amount')]")).getText();
           Assert.assertTrue(
                   totalText.contains("2,094,750.00"),
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
                   poText.contains("ADAC-CON-HML-2019-L-502"),
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
                   itemName.contains("Charges For Service Contract"),
                   "Item Name mismatch! Expected: 'Charges For Service Contract' but Actual: " + itemName
           );
           utils.ExtentLogger.log("Item Name: 'Sales AMC Enterprise Solution' found");

           
           Assert.assertTrue(
                   quantity.contains("1"),
                   "Quantity mismatch! Expected: '1' but Actual: " + quantity
           );
           utils.ExtentLogger.log("Item Quantity: '1' found");

           
         
           Assert.assertTrue(
                   unitPrice.contains("1,995,000.00"),
                   "Unit Price mismatch! Expected: '1,995,000.00' but Actual: " + unitPrice 
           );
           utils.ExtentLogger.log("Unit Price: '1,995,000.00' found");

          
           Assert.assertTrue(
                   taxPercentage.contains("5%"),
                   "Tax Percentage mismatch! Expected: '5%' but Actual: " + taxPercentage
           );
           utils.ExtentLogger.log("Tax Percentage: '5%' found");


           Assert.assertTrue(
                   totalPrice.contains("2,094,750.00"),
                   "Total Price mismatch! Expected: '2,094,750.00' but Actual: " + totalPrice
           );
           utils.ExtentLogger.log("Total Price: '2,094,750.00' found");

           
           wait.until(ExpectedConditions.visibilityOfElementLocated(By.xpath("//div[@id='chat-input']/p")));
           WebElement chatbox = driver.findElement(By.xpath("//div[@id='chat-input']/p"));
           chatbox.sendKeys("Yes");
           driver.findElement(By.id("send-message-button")).click();
           utils.ExtentLogger.log("Confirm prompt sent to create the vendor bill for Smart Vision");
           
           wait.until(ExpectedConditions.visibilityOfElementLocated(By.xpath("//p[contains(.,'The vendor bill has been created successfully in draft status')]")));

           utils.ExtentLogger.log("vendor bill detail is visible");
           
           // Invoice Number
           String invoiceNumberText = driver.findElement(By.xpath("//li[contains(.,'Invoice Number')]")).getText();
           Assert.assertTrue(
                   invoiceNumberText.contains("5253783660"),
                   "Invoice Number mismatch! Expected: '5253783660' but Actual text: " + invoiceNumberText
           );
           utils.ExtentLogger.log("Invoice Number: '5253783660' found");


           // Invoice ID
           WebElement invoiceIdText = driver.findElement(By.xpath("//li[contains(.,'Invoice ID')]"));
           Assert.assertTrue(
                   invoiceIdText.isDisplayed(),
                   "Invoice ID not found!"
           );
           utils.ExtentLogger.log("Invoice ID: "+invoiceIdText.getText()+" found");
           
           // State
           String stateText = driver.findElement(By.xpath("//li[contains(.,'State')]")).getText();
           Assert.assertTrue(
                   stateText.contains("Draft"),
                   "State mismatch! Expected: 'Draft' but Actual text: " + stateText
           );
           utils.ExtentLogger.log("State: "+stateText+" found");


           // Validation Result - General message
           String validationResultText = driver.findElement(By.xpath("//li[contains(.,'Validation Result')]")).getText();
           Assert.assertTrue(
                   validationResultText.contains("The invoice validation failed"),
                   "Validation Result message mismatch! Actual text: " + validationResultText
           );

           // Validation Result - Specific Issues
           List<String> validationIssues = driver.findElements(By.xpath("//strong[contains(.,'Validation Result')]/following-sibling::ul/li"))
                                                 .stream()
                                                 .map(WebElement::getText)
                                                 .collect(Collectors.toList());

           // Check each expected issue
           Assert.assertTrue(
                   validationIssues.stream().anyMatch(t -> t.contains("Price 1995000.0 for 'Charges For Service Contract Period 01.10.2020 to 31.10.2020' does not match PO price 1975000.0")),
                   "Expected product not found issue missing! Actual: " + validationIssues
           );

           Assert.assertTrue(
                   validationIssues.stream().anyMatch(t -> t.contains("Duplicate invoice number '5253783660' found")),
                   "Expected duplicate invoice number issue missing! Actual: " + validationIssues
           );
           
           wait.until(ExpectedConditions.visibilityOfElementLocated(By.xpath("//div[@id='chat-input']/p")));
           chatbox = driver.findElement(By.xpath("//div[@id='chat-input']/p"));
           chatbox.sendKeys("extract invoice details for "+invoiceIdText.getText());
           
           driver.findElement(By.id("send-message-button")).click();
           
           wait.until(ExpectedConditions.visibilityOfElementLocated(By.xpath("//p[contains(.,'Summary of Invoice')]")));
           utils.ExtentLogger.log("Invoice Summary is displaying with details");
           // --- Header Details ----
           // Vendor Name
           vendorText = driver.findElement(By.xpath("//li[contains(.,'Vendor Name')]")).getText();
           Assert.assertTrue(
                   vendorText.contains("Honeywell International ME Limited AD"),
                   "Vendor Name mismatch! Actual text: " + vendorText
           );
           utils.ExtentLogger.log(vendorText +" found");

           // Invoice Number
           invoiceText = driver.findElement(By.xpath("//li[contains(.,'Invoice Number')]")).getText();
           Assert.assertTrue(
                   invoiceText.contains("5253783660"),
                   "Invoice Number mismatch! Actual text: " + invoiceText
           );
           utils.ExtentLogger.log(invoiceText +" found");

           // Due Date
           dueDateText = driver.findElement(By.xpath("//li[contains(.,'Due Date')]")).getText();
           
           Assert.assertTrue(
                   !(dueDateText.isEmpty()),
                   "Due Date is not found!"
           );
           utils.ExtentLogger.log(dueDateText +" found");

           // Subtotal
           subtotalText = driver.findElement(By.xpath("//li[contains(.,'Subtotal')]")).getText();
           Assert.assertTrue(
                   subtotalText.contains("1,995,000.00"),
                   "Subtotal mismatch! Actual text: " + subtotalText
           );
           utils.ExtentLogger.log(subtotalText +" found");

           // Tax Amount
           taxText = driver.findElement(By.xpath("//li[contains(.,'Tax Amount')]")).getText();
           Assert.assertTrue(
                   taxText.contains("99,750.00"),
                   "Tax Amount mismatch! Actual text: " + taxText
           );
           utils.ExtentLogger.log(taxText +" found");


           // Total Amount
           totalText = driver.findElement(By.xpath("//li[contains(.,'Total Amount')]")).getText();
           Assert.assertTrue(
                   totalText.contains("2,094,750.00"),
                   "Total Amount mismatch! Actual text: " + totalText
           );
           utils.ExtentLogger.log(totalText +" found");


           // Currency
           currencyText = driver.findElement(By.xpath("//li[contains(.,'Currency')]")).getText();
           Assert.assertTrue(
                   currencyText.contains("AED"),
                   "Currency mismatch! Actual text: " + currencyText
           );
           utils.ExtentLogger.log(currencyText +" found");


           // Purchase Order
           poText = driver.findElement(By.xpath("//li[contains(.,'Purchase Order')]")).getText();
           Assert.assertTrue(
                   poText.contains("ADAC-CON-HML-2019-L-502"),
                   "Purchase Order mismatch! Actual text: " + poText
           );
           utils.ExtentLogger.log(poText +" found");


           // ---- Product Line Details ----
           itemRow = driver.findElement(By.xpath("//table//tr[contains(@class,'bg-gray')]"));

           itemName = itemRow.findElement(By.xpath("./td[contains(@class,'text-gray')][1]")).getText();
           quantity = itemRow.findElement(By.xpath("./td[contains(@class,'text-gray')][2]")).getText();
           unitPrice = itemRow.findElement(By.xpath("./td[contains(@class,'text-gray')][3]")).getText();
           taxPercentage = itemRow.findElement(By.xpath("./td[contains(@class,'text-gray')][4]")).getText();
           totalPrice = itemRow.findElement(By.xpath("./td[contains(@class,'text-gray')][5]")).getText();
           utils.ExtentLogger.log("Product lines details is displaying in tabular format...");

           Assert.assertTrue(
                   itemName.contains("Charges For Service Contract"),
                   "Item Name mismatch! Expected: 'Charges For Service Contract' but Actual: " + itemName
           );
           utils.ExtentLogger.log("Item Name: 'Sales AMC Enterprise Solution' found");

           
           Assert.assertTrue(
                   quantity.contains("1"),
                   "Quantity mismatch! Expected: '1' but Actual: " + quantity
           );
           utils.ExtentLogger.log("Item Quantity: '1' found");

           
         
           Assert.assertTrue(
                   unitPrice.contains("1,995,000.00"),
                   "Unit Price mismatch! Expected: '1,995,000.00' but Actual: " + unitPrice 
           );
           utils.ExtentLogger.log("Unit Price: '1,995,000.00' found");

          
           Assert.assertTrue(
                   taxPercentage.contains("5%"),
                   "Tax Percentage mismatch! Expected: '5%' but Actual: " + taxPercentage
           );
           utils.ExtentLogger.log("Tax Percentage: '5%' found");


           Assert.assertTrue(
                   totalPrice.contains("2,094,750.00"),
                   "Total Price mismatch! Expected: '2,094,750.00' but Actual: " + totalPrice
           );
           utils.ExtentLogger.log("Total Price: '2,094,750.00' found");

           log.info("Honeywell Invoice details prompt test completed.");
    }
    
   @Test(priority = 4)
   public void testSendAndReceiveEmail() throws Exception {
        EmailUtils emailUtils = new EmailUtils(FROM_EMAIL, APP_PASSWORD);

        // Send Email
        String subject = "Automation Test " + System.currentTimeMillis();
        String body = "Hello, this is a test email from automation.";
       // file inside resources folder
        String attachmentPath = "src/test/java/resources/smartvision.png";
        emailUtils.sendEmail(TO_EMAIL, subject, body, attachmentPath);
        utils.ExtentLogger.log("Sent an email with attaching invoice to Invoflux");
        // Wait for reply (simulate delay)
        System.out.println("Waiting for reply...");
        Thread.sleep( 3* 60 * 1000); // wait 3 minutes

        // Read latest email
        String receivedContent = emailUtils.readLatestUnreadEmail();
        utils.ExtentLogger.log("Reading email response"+ receivedContent);

        // Validate content
        Assert.assertNotNull(receivedContent, "No email received!");
        String expectedContent = "Invoice Number: SV-I-0018223-20";
        Assert.assertTrue(receivedContent.contains(expectedContent),
                "Email response did not contain expected -> " +expectedContent);
        utils.ExtentLogger.log(expectedContent+ " found");
        
        expectedContent = "Status: Draft";
        Assert.assertTrue(receivedContent.contains(expectedContent),
                "Email response did not contain expected -> " +expectedContent);
        utils.ExtentLogger.log(expectedContent+ " found");
        
        expectedContent = "Vendor Name: Smart Vision for Information Systems";
        Assert.assertTrue(receivedContent.contains(expectedContent),
                "Email response did not contain expected Vendor Name -> " +expectedContent);
        utils.ExtentLogger.log(expectedContent+ " found");
        
        expectedContent = "Purchase Order: 1017768";
        Assert.assertTrue(receivedContent.contains(expectedContent),
                "Email response did not contain expected Purchase Order -> " +expectedContent);
        utils.ExtentLogger.log(expectedContent+ " found");
        
        expectedContent = "Sales AMC Enterprise Solution";
        Assert.assertTrue(receivedContent.contains(expectedContent),
                "Email response did not contain expected product -> " +expectedContent);
        utils.ExtentLogger.log(expectedContent+ " found");
        
        expectedContent = "1";
        Assert.assertTrue(receivedContent.contains(expectedContent),
                "Email response did not contain expected product quantity -> " +expectedContent);
        utils.ExtentLogger.log("Quantity: "+expectedContent+ " found");
        
        expectedContent = "59,062.50 AED";
        Assert.assertTrue(receivedContent.contains(expectedContent),
                "Email response did not contain expected Unit Price -> " +expectedContent);
        utils.ExtentLogger.log(expectedContent+ " found");
        
        expectedContent = "5%";
        Assert.assertTrue(receivedContent.contains(expectedContent),
                "Email response did not contain expected Tax% -> " +expectedContent);
        utils.ExtentLogger.log(expectedContent+ " found");
        
        expectedContent = "62,015.63 AED";
        Assert.assertTrue(receivedContent.contains(expectedContent),
                "Email response did not contain expected Total Price -> " +expectedContent);
        utils.ExtentLogger.log(expectedContent+ " found");
        
        log.info("Invoice details extractation thru email prompt test has been completed.");
        
    }


    public void scrollElementToTop(By locator) {
        WebElement element = driver.findElement(locator);
        ((JavascriptExecutor) driver).executeScript("arguments[0].scrollIntoView({block: 'start', behavior: 'smooth'});", element);
    }
}
