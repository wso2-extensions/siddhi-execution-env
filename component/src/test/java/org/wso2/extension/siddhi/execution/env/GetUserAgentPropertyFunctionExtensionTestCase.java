package org.wso2.extension.siddhi.execution.env;

import org.apache.log4j.Logger;
import org.testng.annotations.Test;
import org.wso2.siddhi.core.SiddhiManager;
import org.wso2.siddhi.core.exception.SiddhiAppCreationException;

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
    public void exceptionTestCaseNonStringPropertyName() {

        logger.info("GetUserAgentPropertyFunctionExtensionTestCase exceptionTestCaseNonStringPropertyName");

        SiddhiManager siddhiManager = new SiddhiManager();

        String stream = "define stream inputStream (userAgent string);\n";

        String query = ("@info(name = 'query1') from inputStream "
                + "select env:getUserAgentProperty('Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like " +
                "Gecko) Chrome/67.0.3396.79 Safari/537.36',5) as functionOutput "
                + "insert into outputStream;");

        siddhiManager.createSiddhiAppRuntime(stream + query);
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

}
