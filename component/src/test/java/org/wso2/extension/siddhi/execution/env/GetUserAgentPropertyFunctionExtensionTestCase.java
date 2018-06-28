package org.wso2.extension.siddhi.execution.env;

import org.apache.log4j.Logger;
import org.testng.AssertJUnit;
import org.testng.annotations.Test;
import org.wso2.siddhi.core.SiddhiAppRuntime;
import org.wso2.siddhi.core.SiddhiManager;
import org.wso2.siddhi.core.event.Event;
import org.wso2.siddhi.core.exception.SiddhiAppCreationException;
import org.wso2.siddhi.core.query.output.callback.QueryCallback;
import org.wso2.siddhi.core.stream.input.InputHandler;
import org.wso2.siddhi.core.util.EventPrinter;

public class GetUserAgentPropertyFunctionExtensionTestCase {

    private static Logger logger = Logger.getLogger(GetUserAgentPropertyFunctionExtensionTestCase.class);

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
        siddhiAppRuntime.shutdown();
    }
}
