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
import org.awaitility.Awaitility;
import org.awaitility.Duration;
import org.testng.AssertJUnit;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;
import org.wso2.siddhi.core.SiddhiAppRuntime;
import org.wso2.siddhi.core.SiddhiManager;
import org.wso2.siddhi.core.event.Event;
import org.wso2.siddhi.core.query.output.callback.QueryCallback;
import org.wso2.siddhi.core.stream.input.InputHandler;
import org.wso2.siddhi.core.util.EventPrinter;

import java.util.concurrent.atomic.AtomicInteger;

/**
 * Test case for ResourceIdentifierStreamProcessor extension.
 */
public class ResourceIdentifierStreamProcessorTestCase {
    private static Logger logger = Logger.getLogger(ResourceIdentifierStreamProcessorTestCase.class);
    private static AtomicInteger actualEventCount;

    @BeforeMethod
    public void beforeTest() {
        actualEventCount = new AtomicInteger(0);
    }

    @Test
    public void testDefaultBehaviour() throws Exception {
        logger.info("ResourceIdentifierStreamProcessorDefaultBehaviour TestCase");

        SiddhiManager siddhiManager = new SiddhiManager();

        String stream = "define stream inputStream (key string);\n";

        String query = ("@info(name = 'query1')\n" +
                "from inputStream#env:resourceIdentifier(\"Y\")\n"
                + "select *\n"
                + "insert into outputStream;");
        String query2 = ("@info(name = 'query2')\n" +
                "from inputStream#env:resourceIdentifier(\"Y\")\n"
                + "select *\n"
                + "insert into outputStream;");

        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(stream + query + query2);
        siddhiAppRuntime.addCallback("query2", new QueryCallback() {
            @Override
            public void receive(long timeStamp, Event[] inEvents,
                                Event[] removeEvents) {
                EventPrinter.print(timeStamp, inEvents, removeEvents);
                AssertJUnit.assertEquals(2, ResourceIdentifierStreamProcessor.
                        getResourceCount("Y"));
                actualEventCount.incrementAndGet();
            }
        });
        InputHandler inputHandler = siddhiAppRuntime.getInputHandler("inputStream");
        siddhiAppRuntime.start();
        inputHandler.send(new String[]{"stringProperty"});
        waitTillVariableCountMatches(1, Duration.ONE_MINUTE);
        siddhiAppRuntime.shutdown();
    }

    @Test(dependsOnMethods = "testDefaultBehaviour")
    public void testResourceGenerateWithTwoSiddhiRuntime() throws Exception {
        logger.info("ResourceIdentifierStreamProcessorResourceGenerateWithTwoSiddhiRuntime TestCase");

        SiddhiManager siddhiManager = new SiddhiManager();

        String stream = "define stream inputStream (key string);\n";

        String query = ("@info(name = 'query1')\n" +
                "from inputStream#env:resourceIdentifier(\"Y\")\n"
                + "select *\n"
                + "insert into outputStream;");
        String query2 = ("@info(name = 'query2')\n" +
                "from inputStream#env:resourceIdentifier(\"Y\")\n"
                + "select *\n"
                + "insert into outputStream;");

        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(stream + query);
        SiddhiAppRuntime siddhiAppRuntime2 = siddhiManager.createSiddhiAppRuntime(stream + query2);
        siddhiAppRuntime2.addCallback("query2", new QueryCallback() {
            @Override
            public void receive(long timeStamp, Event[] inEvents,
                                Event[] removeEvents) {
                EventPrinter.print(timeStamp, inEvents, removeEvents);
                AssertJUnit.assertEquals(2, ResourceIdentifierStreamProcessor.
                        getResourceCount("Y"));
                actualEventCount.incrementAndGet();
            }
        });
        InputHandler inputHandler = siddhiAppRuntime.getInputHandler("inputStream");
        InputHandler inputHandler2 = siddhiAppRuntime2.getInputHandler("inputStream");
        siddhiAppRuntime.start();
        siddhiAppRuntime2.start();
        inputHandler.send(new String[]{"stringProperty"});
        inputHandler2.send(new String[]{"stringProperty"});
        waitTillVariableCountMatches(1, Duration.ONE_MINUTE);
        siddhiAppRuntime.shutdown();
        siddhiAppRuntime2.shutdown();
    }


    @Test(dependsOnMethods = "testResourceGenerateWithTwoSiddhiRuntime")
    public void testRemoveResource() throws Exception {
        logger.info("ResourceIdentifierStreamProcessorRemoveResource TestCase");

        SiddhiManager siddhiManager = new SiddhiManager();

        String stream = "define stream inputStream (key string);\n";

        String query = ("@info(name = 'query1')\n" +
                "from inputStream#env:resourceIdentifier(\"Y\")\n"
                + "select *\n"
                + "insert into outputStream;");
        String query2 = ("@info(name = 'query2')\n" +
                "from inputStream#env:resourceIdentifier(\"Y\")\n"
                + "select *\n"
                + "insert into outputStream;");

        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(stream + query);
        siddhiAppRuntime.addCallback("query1", new QueryCallback() {
            @Override
            public void receive(long timeStamp, Event[] inEvents,
                                Event[] removeEvents) {
                EventPrinter.print(timeStamp, inEvents, removeEvents);
                AssertJUnit.assertEquals(1, ResourceIdentifierStreamProcessor.
                        getResourceCount("Y"));
                actualEventCount.incrementAndGet();
            }
        });
        InputHandler inputHandler = siddhiAppRuntime.getInputHandler("inputStream");
        siddhiAppRuntime.start();
        inputHandler.send(new String[]{"stringProperty1"});
        waitTillVariableCountMatches(1, Duration.ONE_MINUTE);
        siddhiAppRuntime.shutdown();
        Thread.sleep(1000);
        SiddhiAppRuntime siddhiAppRuntime2 = siddhiManager.createSiddhiAppRuntime(stream + query2);
        siddhiAppRuntime2.start();
        InputHandler inputHandler2 = siddhiAppRuntime2.getInputHandler("inputStream");
        siddhiAppRuntime2.addCallback("query2", new QueryCallback() {
            @Override
            public void receive(long timeStamp, Event[] inEvents,
                                Event[] removeEvents) {
                EventPrinter.print(timeStamp, inEvents, removeEvents);
                AssertJUnit.assertEquals(1, ResourceIdentifierStreamProcessor.
                        getResourceCount("Y"));
                actualEventCount.incrementAndGet();
            }
        });
        inputHandler2.send(new String[]{"stringProperty2"});
        waitTillVariableCountMatches(2, Duration.ONE_MINUTE);
        siddhiAppRuntime2.shutdown();
    }

    @Test(dependsOnMethods = "testRemoveResource")
    public void testTwoResources() throws Exception {
        logger.info("ResourceIdentifierStreamProcessorTwoResources TestCase");

        SiddhiManager siddhiManager = new SiddhiManager();

        String stream = "define stream inputStream (key string);\n";

        String query = ("@info(name = 'query1')\n" +
                "from inputStream#env:resourceIdentifier(\"Y\")\n"
                + "select *\n"
                + "insert into outputStream;");
        String query2 = ("@info(name = 'query2')\n" +
                "from inputStream#env:resourceIdentifier(\"Z\")\n"
                + "select *\n"
                + "insert into outputStream;");

        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(stream + query);
        SiddhiAppRuntime siddhiAppRuntime2 = siddhiManager.createSiddhiAppRuntime(stream + query2);
        siddhiAppRuntime2.addCallback("query2", new QueryCallback() {
            @Override
            public void receive(long timeStamp, Event[] inEvents,
                                Event[] removeEvents) {
                EventPrinter.print(timeStamp, inEvents, removeEvents);
                AssertJUnit.assertEquals(1, ResourceIdentifierStreamProcessor.
                        getResourceCount("Y"));
                AssertJUnit.assertEquals(1, ResourceIdentifierStreamProcessor.
                        getResourceCount("Z"));
                actualEventCount.incrementAndGet();
            }
        });
        InputHandler inputHandler = siddhiAppRuntime.getInputHandler("inputStream");
        InputHandler inputHandler2 = siddhiAppRuntime2.getInputHandler("inputStream");
        siddhiAppRuntime.start();
        siddhiAppRuntime2.start();
        inputHandler.send(new String[]{"stringProperty"});
        inputHandler2.send(new String[]{"stringProperty"});
        waitTillVariableCountMatches(1, Duration.ONE_MINUTE);
        siddhiAppRuntime.shutdown();
        siddhiAppRuntime2.shutdown();
    }

    private static void waitTillVariableCountMatches(long expected, Duration duration) {
        Awaitility.await().atMost(duration).until(() -> actualEventCount.get() == expected);
    }
}
