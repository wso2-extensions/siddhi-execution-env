package org.wso2.extension.siddhi.execution.env;

import io.siddhi.core.SiddhiAppRuntime;
import io.siddhi.core.SiddhiManager;
import io.siddhi.core.event.Event;
import io.siddhi.core.exception.SiddhiAppCreationException;
import io.siddhi.core.query.output.callback.QueryCallback;
import io.siddhi.core.stream.input.InputHandler;
import io.siddhi.core.util.EventPrinter;
import io.siddhi.core.util.SiddhiTestHelper;
import io.siddhi.core.util.config.InMemoryConfigManager;
import org.apache.log4j.Logger;
import org.testng.AssertJUnit;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

public class GetUserAgentPropertyFunctionExtensionTestCase {

    private static Logger logger = Logger.getLogger(GetUserAgentPropertyFunctionExtensionTestCase.class);
    private AtomicInteger eventCount = new AtomicInteger(0);
    private int waitTime = 50;
    private int timeout = 30000;

    @BeforeMethod
    public void testInit() {
        eventCount.set(0);
    }

    @Test(expectedExceptions = SiddhiAppCreationException.class)
    public void exceptionTestCaseNonStringUserAgent() {

        logger.info("GetUserAgentPropertyFunctionExtensionTestCase exceptionTestCaseNonStringUserAgent");

        SiddhiManager siddhiManager = new SiddhiManager();

        String stream = "define stream inputStream (userAgent string);\n";

        String query = ("@info(name = 'query1') from inputStream "
                + "select env:getUserAgentProperty(5,'browser') as functionOutput "
                + "insert into outputStream;");

        siddhiManager.createSiddhiAppRuntime(stream + query);
    }

    @Test(expectedExceptions = SiddhiAppCreationException.class)
    public void exceptionTestCaseInvalidPropertyName() {

        logger.info("GetUserAgentPropertyFunctionExtensionTestCase exceptionTestCaseInvalidPropertyName");

        SiddhiManager siddhiManager = new SiddhiManager();

        String stream = "define stream inputStream (userAgent string);\n";

        String query = ("@info(name = 'query1') from inputStream "
                + "select env:getUserAgentProperty('Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like " +
                "Gecko) Chrome/67.0.3396.79 Safari/537.36',5) as functionOutput "
                + "insert into outputStream;");

        siddhiManager.createSiddhiAppRuntime(stream + query);

    }

    @Test(expectedExceptions = SiddhiAppCreationException.class)
    public void exceptionTestCaseDynamicPropertyName() throws InterruptedException {
        logger.info("GetUserAgentPropertyFunctionExtensionTestCase exceptionTestCaseDynamicPropertyName");

        SiddhiManager siddhiManager = new SiddhiManager();

        String stream = "define stream inputStream (userAgent string, property string);\n";

        String query = ("@info(name = 'query1') from inputStream "
                + "select env:getUserAgentProperty(userAgent, property) as functionOutput "
                + "insert into outputStream;");

        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(stream + query);
        InputHandler inputHandler = siddhiAppRuntime
                .getInputHandler("inputStream");
        siddhiAppRuntime.start();
        inputHandler.send(new Object[]{"Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) " +
                "Chrome/67.0.3396.79 Safari/537.36, os"});
        siddhiAppRuntime.shutdown();

    }

    @Test(expectedExceptions = SiddhiAppCreationException.class)
    public void exceptionTestCaseInvalidArgumentCount() {

        logger.info("GetUserAgentPropertyFunctionExtensionTestCase exceptionTestCaseInvalidArgumentCount");

        SiddhiManager siddhiManager = new SiddhiManager();

        String stream = "define stream inputStream (userAgent string);\n";

        String query = ("@info(name = 'query1') from inputStream "
                + "select env:getUserAgentProperty('Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like " +
                "Gecko) Chrome/67.0.3396.79 Safari/537.36') as functionOutput "
                + "insert into outputStream;");

        siddhiManager.createSiddhiAppRuntime(stream + query);
    }

    @Test
    public void testExtractingBrowser() throws Exception {
        logger.info("GetUserAgentPropertyFunctionExtensionTestCase testExtractingBrowser");

        SiddhiManager siddhiManager = new SiddhiManager();

        String stream = "define stream inputStream (userAgent string);\n";

        String query = ("@info(name = 'query1') from inputStream "
                + "select env:getUserAgentProperty(userAgent, 'browser') as functionOutput "
                + "insert into outputStream;");

        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(stream + query);
        siddhiAppRuntime.addCallback("query1", new QueryCallback() {
            @Override
            public void receive(long timeStamp, Event[] inEvents,
                                Event[] removeEvents) {
                EventPrinter.print(timeStamp, inEvents, removeEvents);
                String result;
                for (Event event : inEvents) {
                    eventCount.incrementAndGet();
                    result = (String) event.getData(0);
                    AssertJUnit.assertEquals("Chrome", result);
                }
            }
        });
        InputHandler inputHandler = siddhiAppRuntime
                .getInputHandler("inputStream");
        siddhiAppRuntime.start();
        inputHandler.send(new String[]{"Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) " +
                "Chrome/67.0.3396.79 Safari/537.36"});
        SiddhiTestHelper.waitForEvents(waitTime, 1, eventCount, timeout);
        siddhiAppRuntime.shutdown();
    }

    @Test
    public void testExtractingOS() throws Exception {
        logger.info("GetUserAgentPropertyFunctionExtensionTestCase testExtractingOS");

        SiddhiManager siddhiManager = new SiddhiManager();

        String stream = "define stream inputStream (userAgent string);\n";

        String query = ("@info(name = 'query1') from inputStream "
                + "select env:getUserAgentProperty(userAgent, 'os') as functionOutput "
                + "insert into outputStream;");

        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(stream + query);
        siddhiAppRuntime.addCallback("query1", new QueryCallback() {
            @Override
            public void receive(long timeStamp, Event[] inEvents,
                                Event[] removeEvents) {
                EventPrinter.print(timeStamp, inEvents, removeEvents);
                String result;
                for (Event event : inEvents) {
                    eventCount.incrementAndGet();
                    result = (String) event.getData(0);
                    AssertJUnit.assertEquals("Linux", result);
                }
            }
        });
        InputHandler inputHandler = siddhiAppRuntime
                .getInputHandler("inputStream");
        siddhiAppRuntime.start();
        inputHandler.send(new String[]{"Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) " +
                "Chrome/67.0.3396.79 Safari/537.36"});
        SiddhiTestHelper.waitForEvents(waitTime, 1, eventCount, timeout);
        siddhiAppRuntime.shutdown();
    }

    @Test
    public void testExtractingDevice() throws Exception {
        logger.info("GetUserAgentPropertyFunctionExtensionTestCase testExtractingDevice");

        SiddhiManager siddhiManager = new SiddhiManager();

        String stream = "define stream inputStream (userAgent string);\n";

        String query = ("@info(name = 'query1') from inputStream "
                + "select env:getUserAgentProperty(userAgent, 'device') as functionOutput "
                + "insert into outputStream;");

        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(stream + query);
        siddhiAppRuntime.addCallback("query1", new QueryCallback() {
            @Override
            public void receive(long timeStamp, Event[] inEvents,
                                Event[] removeEvents) {
                EventPrinter.print(timeStamp, inEvents, removeEvents);
                String result;
                for (Event event : inEvents) {
                    eventCount.incrementAndGet();
                    result = (String) event.getData(0);
                    AssertJUnit.assertEquals("Nexus 5X", result);
                }
            }
        });
        InputHandler inputHandler = siddhiAppRuntime
                .getInputHandler("inputStream");
        siddhiAppRuntime.start();
        inputHandler.send(new String[]{"Mozilla/5.0 (Linux; Android 7.1.1; Nexus 5X Build/N4F26T; wv) " +
                "AppleWebKit/537.36 (KHTML, like Gecko) Version/4.0 Chrome/62.0.3202.73 Mobile Safari/537.36 " +
                "GSA/7.23.26.21.arm64"});
        SiddhiTestHelper.waitForEvents(waitTime, 1, eventCount, timeout);
        siddhiAppRuntime.shutdown();
    }

    @Test
    public void testCustomRegexFile() throws Exception {
        logger.info("GetUserAgentPropertyFunctionExtensionTestCase testCustomRegexFile");

        Map<String, String> systemConfigs = new HashMap<>();
        ClassLoader classLoader = getClass().getClassLoader();
        String filePath = classLoader.getResource("regexes.yaml").getPath();
        systemConfigs.put("env.getUserAgentProperty.regexFilePath", filePath);
        InMemoryConfigManager inMemoryConfigManager = new InMemoryConfigManager(systemConfigs, null);

        SiddhiManager siddhiManager = new SiddhiManager();
        siddhiManager.setConfigManager(inMemoryConfigManager);

        String stream = "define stream inputStream (userAgent string);\n";

        String query = ("@info(name = 'query1') from inputStream "
                + "select env:getUserAgentProperty(userAgent, 'device') as functionOutput "
                + "insert into outputStream;");

        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(stream + query);
        siddhiAppRuntime.addCallback("query1", new QueryCallback() {
            @Override
            public void receive(long timeStamp, Event[] inEvents,
                                Event[] removeEvents) {
                EventPrinter.print(timeStamp, inEvents, removeEvents);
                String result;
                for (Event event : inEvents) {
                    eventCount.incrementAndGet();
                    result = (String) event.getData(0);
                    AssertJUnit.assertEquals("Nexus 5X", result);
                }
            }
        });
        InputHandler inputHandler = siddhiAppRuntime
                .getInputHandler("inputStream");
        siddhiAppRuntime.start();
        inputHandler.send(new String[]{"Mozilla/5.0 (Linux; Android 7.1.1; Nexus 5X Build/N4F26T; wv) " +
                "AppleWebKit/537.36 (KHTML, like Gecko) Version/4.0 Chrome/62.0.3202.73 Mobile Safari/537.36 " +
                "GSA/7.23.26.21.arm64"});
        SiddhiTestHelper.waitForEvents(waitTime, 1, eventCount, timeout);
        siddhiAppRuntime.shutdown();
    }

    @Test(expectedExceptions = SiddhiAppCreationException.class)
    public void testExceptionCustomRegexFileNotFound() {
        logger.info("GetUserAgentPropertyFunctionExtensionTestCase testExceptionCustomRegexFileNotFound");

        Map<String, String> systemConfigs = new HashMap<>();
        systemConfigs.put("env.getUserAgentProperty.regexFilePath", "/regexes.yaml");
        InMemoryConfigManager inMemoryConfigManager = new InMemoryConfigManager(systemConfigs, null);

        SiddhiManager siddhiManager = new SiddhiManager();
        siddhiManager.setConfigManager(inMemoryConfigManager);

        String stream = "define stream inputStream (userAgent string);\n";

        String query = ("@info(name = 'query1') from inputStream "
                + "select env:getUserAgentProperty(userAgent, 'device') as functionOutput "
                + "insert into outputStream;");

        siddhiManager.createSiddhiAppRuntime(stream + query);
    }

    @Test(expectedExceptions = SiddhiAppCreationException.class)
    public void testExceptionIncorrectCustomRegexFile() {
        logger.info("GetUserAgentPropertyFunctionExtensionTestCase testExceptionIncorrectCustomRegexFile");

        Map<String, String> systemConfigs = new HashMap<>();
        ClassLoader classLoader = getClass().getClassLoader();
        String filePath = classLoader.getResource("regexes-incorrect.yaml").getPath();
        systemConfigs.put("env.getUserAgentProperty.regexFilePath", filePath);
        InMemoryConfigManager inMemoryConfigManager = new InMemoryConfigManager(systemConfigs, null);

        SiddhiManager siddhiManager = new SiddhiManager();
        siddhiManager.setConfigManager(inMemoryConfigManager);

        String stream = "define stream inputStream (userAgent string);\n";

        String query = ("@info(name = 'query1') from inputStream "
                + "select env:getUserAgentProperty(userAgent, 'device') as functionOutput "
                + "insert into outputStream;");

        siddhiManager.createSiddhiAppRuntime(stream + query);
    }
}
