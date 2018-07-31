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

public class ResourceBatchWindowProcessorTestCase {
    private static Logger logger = Logger.getLogger(ResourceBatchWindowProcessorTestCase.class);
    private static AtomicInteger actualEventCount;

    @BeforeMethod
    public void beforeTest() {
        actualEventCount = new AtomicInteger(0);
    }

    @Test
    public void testDefaultBehaviour() throws Exception {
        logger.info("ResourceBatchWindowDefaultBehaviour TestCase");

        SiddhiManager siddhiManager = new SiddhiManager();

        String stream = "define stream inputStream (key string);\n";

        String query = ("@info(name = 'query1')\n" +
                "from inputStream#env:resourceIdentifier(\"X\")\n"
                + "select *\n"
                + "insert into outputStream;");
        String query2 = ("@info(name = 'query2')\n" +
                "from inputStream#window.env:resourceBatch(\"X\", key)\n"
                + "select *\n"
                + "insert into outputStream2;");

        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(stream + query + query2);
        siddhiAppRuntime.addCallback("query2", new QueryCallback() {
            @Override
            public void receive(long timeStamp, Event[] inEvents,
                                Event[] removeEvents) {
                EventPrinter.print(timeStamp, inEvents, removeEvents);
                AssertJUnit.assertEquals("stringProperty", inEvents[0].getData(0));
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
    public void testMultipleResourcesBehaviour() throws Exception {
        logger.info("ResourceBatchWindowMultipleResourcesBehaviour TestCase");

        SiddhiManager siddhiManager = new SiddhiManager();

        String stream = "define stream inputStream (key string);\n";

        String query = ("@info(name = 'query1')\n" +
                "from inputStream#env:resourceIdentifier(\"X\")\n"
                + "select *\n"
                + "insert into outputStream;");
        String query2 = ("@info(name = 'query2')\n" +
                "from inputStream#env:resourceIdentifier(\"X\")\n"
                + "select *\n"
                + "insert into outputStream2;");
        String query3 = ("@info(name = 'query3')\n" +
                "from inputStream#window.env:resourceBatch(\"X\", key)\n"
                + "select *\n"
                + "insert into outputStream3;");

        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(stream + query + query2 + query3);
        siddhiAppRuntime.addCallback("query3", new QueryCallback() {
            @Override
            public void receive(long timeStamp, Event[] inEvents,
                                Event[] removeEvents) {
                EventPrinter.print(timeStamp, inEvents, removeEvents);
                AssertJUnit.assertEquals("stringProperty", inEvents[0].getData(0));
                actualEventCount.incrementAndGet();
                AssertJUnit.assertEquals("stringProperty", inEvents[1].getData(0));
                actualEventCount.incrementAndGet();
            }
        });
        InputHandler inputHandler = siddhiAppRuntime.getInputHandler("inputStream");
        siddhiAppRuntime.start();
        inputHandler.send(new String[]{"stringProperty"});
        inputHandler.send(new String[]{"stringProperty"});
        waitTillVariableCountMatches(2, Duration.ONE_MINUTE);
        siddhiAppRuntime.shutdown();
    }

    @Test(dependsOnMethods = "testMultipleResourcesBehaviour")
    public void testTwoTypeResourcesBehaviour() throws Exception {
        logger.info("ResourceBatchWindowTwoTypeResourcesBehaviour TestCase");

        SiddhiManager siddhiManager = new SiddhiManager();

        String stream = "define stream inputStream (key string);\n";

        String query = ("@info(name = 'query1')\n" +
                "from inputStream#env:resourceIdentifier(\"X\")\n"
                + "select *\n"
                + "insert into outputStream;");
        String query2 = ("@info(name = 'query2')\n" +
                "from inputStream#env:resourceIdentifier(\"Y\")\n"
                + "select *\n"
                + "insert into outputStream2;");
        String query3 = ("@info(name = 'query3')\n" +
                "from inputStream#window.env:resourceBatch(\"X\", key)\n"
                + "select *\n"
                + "insert into outputStream3;");

        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(stream + query + query2 + query3);
        siddhiAppRuntime.addCallback("query3", new QueryCallback() {
            @Override
            public void receive(long timeStamp, Event[] inEvents,
                                Event[] removeEvents) {
                EventPrinter.print(timeStamp, inEvents, removeEvents);
                int count = actualEventCount.incrementAndGet();
                switch (count) {
                    case 1:
                        AssertJUnit.assertEquals("stringProperty", inEvents[0].getData(0));
                        break;
                    case 2:
                        AssertJUnit.assertEquals("stringProperty", inEvents[0].getData(0));
                        break;
                    default: //do nothing
                }
            }
        });
        InputHandler inputHandler = siddhiAppRuntime.getInputHandler("inputStream");
        siddhiAppRuntime.start();
        inputHandler.send(new String[]{"stringProperty"});
        inputHandler.send(new String[]{"stringProperty"});
        waitTillVariableCountMatches(2, Duration.ONE_MINUTE);
        siddhiAppRuntime.shutdown();
    }

    @Test(dependsOnMethods = "testTwoTypeResourcesBehaviour")
    public void testTwoTypeResourcesAndWindowBehaviour() throws Exception {
        logger.info("ResourceBatchWindowTwoTypeResourcesAndWindowBehaviour TestCase");

        SiddhiManager siddhiManager = new SiddhiManager();

        String stream = "define stream inputStream (key string);\n";

        String query = ("@info(name = 'query1')\n" +
                "from inputStream#env:resourceIdentifier(\"X\")\n"
                + "select *\n"
                + "insert into outputStream;");
        String query2 = ("@info(name = 'query2')\n" +
                "from inputStream#env:resourceIdentifier(\"Y\")\n"
                + "select *\n"
                + "insert into outputStream2;");
        String query3 = ("@info(name = 'query3')\n" +
                "from inputStream#window.env:resourceBatch(\"X\", key)\n"
                + "select *\n"
                + "insert into outputStream3;");
        String query4 = ("@info(name = 'query4')\n" +
                "from inputStream#window.env:resourceBatch(\"Y\", key)\n"
                + "select *\n"
                + "insert into outputStream4;");

        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(stream + query + query2 + query3 +
                query4);
        siddhiAppRuntime.addCallback("query3", new QueryCallback() {
            @Override
            public void receive(long timeStamp, Event[] inEvents,
                                Event[] removeEvents) {
                EventPrinter.print(timeStamp, inEvents, removeEvents);
                actualEventCount.incrementAndGet();
                AssertJUnit.assertEquals("stringProperty", inEvents[0].getData(0));
            }
        });
        siddhiAppRuntime.addCallback("query4", new QueryCallback() {
            @Override
            public void receive(long timeStamp, Event[] inEvents,
                                Event[] removeEvents) {
                EventPrinter.print(timeStamp, inEvents, removeEvents);
                actualEventCount.incrementAndGet();
                AssertJUnit.assertEquals("stringProperty", inEvents[0].getData(0));
            }
        });
        InputHandler inputHandler = siddhiAppRuntime.getInputHandler("inputStream");
        siddhiAppRuntime.start();
        inputHandler.send(new String[]{"stringProperty"});
        waitTillVariableCountMatches(2, Duration.ONE_MINUTE);
        siddhiAppRuntime.shutdown();
    }

    @Test(dependsOnMethods = "testTwoTypeResourcesAndWindowBehaviour")
    public void testResourcesWithAggregateBehaviour() throws Exception {
        logger.info("ResourceBatchWindowResourcesWithAggregateBehaviour TestCase");

        SiddhiManager siddhiManager = new SiddhiManager();

        String stream = "define stream inputStream (key string, volume long);\n";

        String query = ("@info(name = 'query1')\n" +
                "from inputStream#env:resourceIdentifier(\"X\")\n"
                + "select *\n"
                + "insert into outputStream;");
        String query2 = ("@info(name = 'query2')\n" +
                "from inputStream#env:resourceIdentifier(\"X\")\n"
                + "select *\n"
                + "insert into outputStream2;");
        String query3 = ("@info(name = 'query3')\n" +
                "from inputStream#window.env:resourceBatch(\"X\", key)\n"
                + "select key, sum(volume) as totalVolume\n"
                + "group by key\n"
                + "insert into outputStream3;");

        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(stream + query + query2 + query3);
        siddhiAppRuntime.addCallback("query3", new QueryCallback() {
            @Override
            public void receive(long timeStamp, Event[] inEvents,
                                Event[] removeEvents) {
                EventPrinter.print(timeStamp, inEvents, removeEvents);
                AssertJUnit.assertTrue(inEvents[0].getData(0).equals("stringProperty") &&
                        inEvents[0].getData(1).equals(30L));
                actualEventCount.incrementAndGet();
            }
        });
        InputHandler inputHandler = siddhiAppRuntime.getInputHandler("inputStream");
        siddhiAppRuntime.start();
        inputHandler.send(new Object[]{"stringProperty", 10L});
        inputHandler.send(new Object[]{"stringProperty", 20L});
        waitTillVariableCountMatches(1, Duration.ONE_MINUTE);
        siddhiAppRuntime.shutdown();
    }

    @Test(dependsOnMethods = "testResourcesWithAggregateBehaviour")
    public void testAggregateMoreEventsThanResourcesBehaviour() throws Exception {
        logger.info("ResourceBatchWindowAggregateMoreEventsThanResources TestCase");

        SiddhiManager siddhiManager = new SiddhiManager();

        String stream = "define stream inputStream (key string, volume long);\n";

        String query = ("@info(name = 'query1')\n" +
                "from inputStream#env:resourceIdentifier(\"X\")\n"
                + "select *\n"
                + "insert into outputStream;");
        String query2 = ("@info(name = 'query2')\n" +
                "from inputStream#env:resourceIdentifier(\"X\")\n"
                + "select *\n"
                + "insert into outputStream2;");
        String query3 = ("@info(name = 'query3')\n" +
                "from inputStream#window.env:resourceBatch(\"X\", key)\n"
                + "select key, sum(volume) as totalVolume\n"
                + "group by key\n"
                + "insert into outputStream3;");

        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(stream + query + query2 + query3);
        siddhiAppRuntime.addCallback("query3", new QueryCallback() {
            @Override
            public void receive(long timeStamp, Event[] inEvents,
                                Event[] removeEvents) {
                EventPrinter.print(timeStamp, inEvents, removeEvents);
                int count = actualEventCount.incrementAndGet();
                switch (count) {
                    case 1:
                        AssertJUnit.assertTrue(inEvents[0].getData(0).equals("stringProperty") &&
                                inEvents[0].getData(1).equals(30L));
                        break;
                    case 2:
                        AssertJUnit.assertTrue(inEvents[0].getData(0).equals("stringProperty") &&
                                inEvents[0].getData(1).equals(100L));
                        break;
                    default: //do nothing
                }
            }
        });
        InputHandler inputHandler = siddhiAppRuntime.getInputHandler("inputStream");
        siddhiAppRuntime.start();
        inputHandler.send(new Object[]{"stringProperty", 10L});
        inputHandler.send(new Object[]{"stringProperty", 20L});
        inputHandler.send(new Object[]{"stringProperty", 50L});
        inputHandler.send(new Object[]{"stringProperty", 50L});
        waitTillVariableCountMatches(2, Duration.ONE_MINUTE);
        siddhiAppRuntime.shutdown();
    }

    @Test(dependsOnMethods = "testAggregateMoreEventsThanResourcesBehaviour")
    public void testAggregateWithTwoKeysBehaviour() throws Exception {
        logger.info("ResourceBatchWindowAggregateWithTwoKeys TestCase");

        SiddhiManager siddhiManager = new SiddhiManager();

        String stream = "define stream inputStream (key string, volume long);\n";

        String query = ("@info(name = 'query1')\n" +
                "from inputStream#env:resourceIdentifier(\"X\")\n"
                + "select *\n"
                + "insert into outputStream;");
        String query2 = ("@info(name = 'query2')\n" +
                "from inputStream#env:resourceIdentifier(\"X\")\n"
                + "select *\n"
                + "insert into outputStream2;");
        String query3 = ("@info(name = 'query3')\n" +
                "from inputStream#window.env:resourceBatch(\"X\", key)\n"
                + "select key, sum(volume) as totalVolume\n"
                + "insert into outputStream3;");

        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(stream + query + query2 + query3);
        siddhiAppRuntime.addCallback("query3", new QueryCallback() {
            @Override
            public void receive(long timeStamp, Event[] inEvents,
                                Event[] removeEvents) {
                EventPrinter.print(timeStamp, inEvents, removeEvents);
                int count = actualEventCount.incrementAndGet();
                switch (count) {
                    case 1:
                        AssertJUnit.assertTrue(inEvents[0].getData(0).equals("stringProperty") &&
                                inEvents[0].getData(1).equals(30L));
                        break;
                    case 2:
                        AssertJUnit.assertTrue(inEvents[0].getData(0).equals("stringProperty1") &&
                                inEvents[0].getData(1).equals(100L));
                        break;
                    default: //do nothing
                }
            }
        });
        InputHandler inputHandler = siddhiAppRuntime.getInputHandler("inputStream");
        siddhiAppRuntime.start();
        inputHandler.send(new Object[]{"stringProperty", 10L});
        inputHandler.send(new Object[]{"stringProperty", 20L});
        inputHandler.send(new Object[]{"stringProperty1", 50L});
        inputHandler.send(new Object[]{"stringProperty1", 50L});
        waitTillVariableCountMatches(2, Duration.ONE_MINUTE);
        siddhiAppRuntime.shutdown();
    }

    @Test(dependsOnMethods = "testAggregateWithTwoKeysBehaviour")
    public void testAggregateWithTimeoutBehaviour() throws Exception {
        logger.info("ResourceBatchWindowAggregateWithTimeout TestCase");

        SiddhiManager siddhiManager = new SiddhiManager();

        String stream = "define stream inputStream (key string, volume long);\n";

        String query = ("@info(name = 'query1')\n" +
                "from inputStream#env:resourceIdentifier(\"X\")\n"
                + "select *\n"
                + "insert into outputStream;");
        String query2 = ("@info(name = 'query2')\n" +
                "from inputStream#env:resourceIdentifier(\"X\")\n"
                + "select *\n"
                + "insert into outputStream2;");
        String query3 = ("@info(name = 'query3')\n" +
                "from inputStream#window.env:resourceBatch(\"X\", key, 2000)\n"
                + "select key, sum(volume) as totalVolume\n"
                + "insert into outputStream3;");

        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(stream + query + query2 + query3);
        siddhiAppRuntime.addCallback("query3", new QueryCallback() {
            @Override
            public void receive(long timeStamp, Event[] inEvents,
                                Event[] removeEvents) {
                EventPrinter.print(timeStamp, inEvents, removeEvents);
                int count = actualEventCount.incrementAndGet();
                switch (count) {
                    case 1:
                        AssertJUnit.assertTrue(inEvents[0].getData(0).equals("stringProperty") &&
                                inEvents[0].getData(1).equals(10L));
                        break;
                    default: //do nothing
                }
            }
        });
        InputHandler inputHandler = siddhiAppRuntime.getInputHandler("inputStream");
        siddhiAppRuntime.start();
        inputHandler.send(new Object[]{"stringProperty", 10L});
        waitTillVariableCountMatches(1, Duration.ONE_MINUTE);
        siddhiAppRuntime.shutdown();
    }

    private static void waitTillVariableCountMatches(long expected, Duration duration) {
        Awaitility.await().atMost(duration).until(() -> actualEventCount.get() == expected);
    }
}
