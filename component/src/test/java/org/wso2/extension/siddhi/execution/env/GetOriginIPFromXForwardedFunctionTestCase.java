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

import io.siddhi.core.SiddhiAppRuntime;
import io.siddhi.core.SiddhiManager;
import io.siddhi.core.event.Event;
import io.siddhi.core.query.output.callback.QueryCallback;
import io.siddhi.core.stream.input.InputHandler;
import io.siddhi.core.util.EventPrinter;
import org.apache.log4j.Logger;
import org.testng.AssertJUnit;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.concurrent.atomic.AtomicInteger;

public class GetOriginIPFromXForwardedFunctionTestCase {
    private static Logger logger = Logger.getLogger(GetOriginIPFromXForwardedFunctionTestCase.class);
    private AtomicInteger count = new AtomicInteger(0);
    private volatile boolean eventArrived;

    @BeforeMethod
    public void init() {
        count.set(0);
        eventArrived = false;
    }

    @Test
    public void testDefaultBehaviour1() throws Exception {
        logger.info("GetOriginIPFromXForwardedFunction default behaviour testCase");
        SiddhiManager siddhiManager = new SiddhiManager();
        String stream = "define stream inputStream (xForwardedHeader string);\n";
        String query = ("@info(name = 'query1') from inputStream \n"
                + "select env:getOriginIPFromXForwarded(xForwardedHeader) as originIP \n" +
                "insert into outputStream;");
        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(stream + query);
        siddhiAppRuntime.addCallback("query1", new QueryCallback() {
            @Override
            public void receive(long timeStamp, Event[] inEvents,
                                Event[] removeEvents) {
                EventPrinter.print(timeStamp, inEvents, removeEvents);
                for (Event event : inEvents) {
                    count.incrementAndGet();
                    switch (count.get()) {
                        case 1:
                            AssertJUnit.assertEquals(null, event.getData(0));
                            eventArrived = true;
                            break;
                        case 2:
                            AssertJUnit.assertEquals("203.160.190.196", event.getData(0));
                            eventArrived = true;
                            break;
                        case 3:
                            AssertJUnit.assertEquals(null, event.getData(0));
                            eventArrived = true;
                            break;
                        case 4:
                            AssertJUnit.assertEquals("203.160.190.198", event.getData(0));
                            eventArrived = true;
                            break;
                        case 5:
                            AssertJUnit.assertEquals("199.207.253.101", event.getData(0));
                            eventArrived = true;
                            break;
                        case 6:
                            AssertJUnit.assertEquals("208.160.190.198", event.getData(0));
                            eventArrived = true;
                            break;
                        case 7:
                            AssertJUnit.assertEquals("210.160.190.198", event.getData(0));
                            eventArrived = true;
                            break;
                        case 8:
                            AssertJUnit.assertEquals("70.41.3.18", event.getData(0));
                            eventArrived = true;
                            break;
                        case 9:
                            AssertJUnit.assertEquals("70.41.3.18", event.getData(0));
                            eventArrived = true;
                            break;
                        case 10:
                            AssertJUnit.assertEquals("70.41.3.18", event.getData(0));
                            eventArrived = true;
                            break;
                        case 11:
                            AssertJUnit.assertEquals("14.3.49.206", event.getData(0));
                            eventArrived = true;
                            break;
                        case 12:
                            AssertJUnit.assertEquals("210.160.190.217", event.getData(0));
                            eventArrived = true;
                            break;
                        case 13:
                            AssertJUnit.assertEquals("210.160.180.117", event.getData(0));
                            eventArrived = true;
                            break;
                    }
                }
            }
        });
        InputHandler inputHandler = siddhiAppRuntime.getInputHandler("inputStream");
        siddhiAppRuntime.start();
        inputHandler.send(new Object[]{" "});
        inputHandler.send(new Object[]{"203.160.190.196,10.1.1.1"});
        inputHandler.send(new String[]{"192.168.1.103"});
        inputHandler.send(new String[]{"192.168.1.103, 203.160.190.198"});
        inputHandler.send(new String[]{"10.3.49.206,            199.207.253.101"});
        inputHandler.send(new String[]{"10.3.49.206, 192.168.1.103, 208.160.190.198"});
        inputHandler.send(new String[]{"10.3.50.233, 192.168.1.103,           210.160.190.198"});
        inputHandler.send(new String[]{"10.4.49.234,             70.41.3.18    , 210.160.190.200"});
        inputHandler.send(new String[]{"10.5.49.216, 70.41.3.18, 192.168.1.103,           210.160.190.208"});
        inputHandler.send(new String[]{"10.3.39.226, 70.41.3.18, 192.168.1.103, a.a.a.a"});
        inputHandler.send(new String[]{"14.3.49.206, , 192.168.1.103, 210.160.190.208"});
        inputHandler.send(new String[]{", a.a.a.a, 192.168.1.103, 210.160.190.217"});
        inputHandler.send(new String[]{",, 192.168.1.103, 210.160.180.117"});
        AssertJUnit.assertEquals(13, count.get());
        AssertJUnit.assertTrue(eventArrived);
        siddhiAppRuntime.shutdown();
    }
}
