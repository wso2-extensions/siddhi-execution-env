/*
 * Copyright (c) 2018, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
 *
 * WSO2 Inc. licenses this file to you under the Apache License,
 * Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

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

/**
 * Test case for GetSystemPropertyFunction function extension.
 */

public class GetSystemPropertyFunctionExtensionTestCase {

    private static Logger logger = Logger.getLogger(GetEnvironmentPropertyFunctionExtensionTestCase.class);

    @Test
    public void testDefaultBehaviour() throws Exception {
        logger.info("GetSystemPropertyFunctionExtensionTestDefaultBehaviour TestCase");

        SiddhiManager siddhiManager = new SiddhiManager();

        String stream = "define stream inputStream (key string);\n";

        String query = ("@info(name = 'query1') from inputStream "
                + "select env:getSystemProperty(key) as propertyValue "
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
                    AssertJUnit.assertEquals(System.getenv("JAVA_HOME"), result);
                }
            }
        });
        InputHandler inputHandler = siddhiAppRuntime.getInputHandler("inputStream");
        siddhiAppRuntime.start();
        inputHandler.send(new String[]{"JAVA_HOME"});
        siddhiAppRuntime.shutdown();
    }

    @Test
    public void testDefaultBehaviourWithWithDefaultValueProvided() throws Exception {
        logger.info("GetSystemPropertyFunctionExtensionTestDefaultBehaviourWithWithDefaultValueProvided TestCase");

        SiddhiManager siddhiManager = new SiddhiManager();

        String stream = "define stream inputStream (key string);\n";

        String query = ("@info(name = 'query1') from inputStream "
                + "select env:getSystemProperty(key,'defaultValue') as propertyValue "
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
                    AssertJUnit.assertEquals(System.getenv("JAVA_HOME"), result);
                }
            }
        });
        InputHandler inputHandler = siddhiAppRuntime
                .getInputHandler("inputStream");
        siddhiAppRuntime.start();
        inputHandler.send(new String[]{"JAVA_HOME"});
        siddhiAppRuntime.shutdown();
    }

    @Test
    public void testDefaultBehaviourWithWithDefaultValueProvided2() throws Exception {
        logger.info("GetSystemPropertyFunctionExtensionTestDefaultBehaviourWithWithDefaultValueProvided TestCase2");

        SiddhiManager siddhiManager = new SiddhiManager();

        String stream = "define stream inputStream (key string);\n";

        String query = ("@info(name = 'query1') from inputStream "
                + "select env:getSystemProperty(key,'defaultValue') as propertyValue "
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
                    AssertJUnit.assertEquals("defaultValue", result);
                }
            }
        });
        InputHandler inputHandler = siddhiAppRuntime.getInputHandler("inputStream");
        siddhiAppRuntime.start();
        inputHandler.send(new String[]{"NOT_JAVA_HOME"});
        siddhiAppRuntime.shutdown();
    }

    @Test(expectedExceptions = SiddhiAppCreationException.class)
    public void exceptionTestCaseNonStringKey() {
        logger.info("GetSystemPropertyFunctionExtension exceptionTestCaseNonStringKey");

        SiddhiManager siddhiManager = new SiddhiManager();

        String stream = "define stream inputStream (key string);\n";

        String query = ("@info(name = 'query1') from inputStream "
                + "select env:getSystemProperty(5) as functionOutput "
                + "insert into outputStream;");

        siddhiManager.createSiddhiAppRuntime(stream + query);
    }


    @Test(expectedExceptions = SiddhiAppCreationException.class)
    public void exceptionTestCaseNullKey() {
        logger.info("GetSystemPropertyFunctionExtension exceptionTestCaseNullKey");

        SiddhiManager siddhiManager = new SiddhiManager();

        String stream = "define stream inputStream (key string);\n";

        String query = ("@info(name = 'query1') from inputStream "
                + "select env:getSystemProperty() as functionOutput "
                + "insert into outputStream;");

        siddhiManager.createSiddhiAppRuntime(stream + query);
    }

    @Test(expectedExceptions = SiddhiAppCreationException.class)
    public void exceptionTestCaseNonStringDefault() {
        logger.info("GetSystemPropertyFunctionExtension exceptionTestCaseNonStringDefault");

        SiddhiManager siddhiManager = new SiddhiManager();

        String stream = "define stream inputStream (key string);\n";

        String query = ("@info(name = 'query1') from inputStream "
                + "select env:getSystemProperty(key, 5) as functionOutput "
                + "insert into outputStream;");

        siddhiManager.createSiddhiAppRuntime(stream + query);
    }
}
