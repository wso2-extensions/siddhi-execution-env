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
import org.testng.Assert;
import org.testng.AssertJUnit;
import org.testng.annotations.Test;
import org.wso2.extension.siddhi.execution.env.util.LoggerAppender;
import org.wso2.extension.siddhi.execution.env.util.LoggerCallBack;
import org.wso2.siddhi.core.SiddhiAppRuntime;
import org.wso2.siddhi.core.SiddhiManager;
import org.wso2.siddhi.core.event.Event;
import org.wso2.siddhi.core.exception.SiddhiAppCreationException;
import org.wso2.siddhi.core.query.output.callback.QueryCallback;
import org.wso2.siddhi.core.stream.input.InputHandler;
import org.wso2.siddhi.core.util.EventPrinter;
import org.wso2.siddhi.core.util.config.InMemoryConfigManager;

import java.util.HashMap;
import java.util.Map;

/**
 * Test case for GetYAMLPropertyFunction function extension.
 */

public class GetYAMLPropertyFunctionExtensionTestCase {

    private static Logger logger = Logger.getLogger(GetYAMLPropertyFunctionExtensionTestCase.class);
    private boolean isLogEventArrived;
    private static String regexPattern = "The type of property value and parameter dataType does not match ";

    @Test
    public void testDefaultBehaviour() throws Exception {
        logger.info("GetYAMLPropertyFunctionExtensiontestDefaultBehaviour TestCase");

        SiddhiManager siddhiManager = new SiddhiManager();
        Map<String, String> configMap = new HashMap<>();
        configMap.put("env.getYAMLProperty.stringProperty", "StringValue");
        configMap.put("env.getYAMLProperty.booleanProperty", "true");
        configMap.put("env.getYAMLProperty.integerProperty", "99");
        configMap.put("env.getYAMLProperty.longProperty", "999");
        configMap.put("env.getYAMLProperty.floatProperty", "99.99");
        configMap.put("env.getYAMLProperty.doubleProperty", "99.99");

        siddhiManager.setConfigManager(new InMemoryConfigManager(configMap, null));
        siddhiManager.setExtension("env:getYAMLProperty", GetYAMLPropertyFunction.class);

        String stream = "define stream inputStream (key string);\n";

        String query = ("@info(name = 'query1') from inputStream "
                + "select env:getYAMLProperty(key) as propertyValue "
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
                    AssertJUnit.assertEquals("StringValue", result);
                }
            }
        });
        InputHandler inputHandler = siddhiAppRuntime.getInputHandler("inputStream");
        siddhiAppRuntime.start();
        inputHandler.send(new String[]{"stringProperty"});
        siddhiAppRuntime.shutdown();
    }

    @Test
    public void testDefaultBehaviourWithDefaultType() throws Exception {
        logger.info("GetYAMLPropertyFunctionExtensionTestDefaultBehaviourWithType TestCase");

        SiddhiManager siddhiManager = new SiddhiManager();
        Map<String, String> configMap = new HashMap<>();
        configMap.put("env.getYAMLProperty.stringProperty", "StringValue");
        configMap.put("env.getYAMLProperty.booleanProperty", "true");
        configMap.put("env.getYAMLProperty.integerProperty", "99");
        configMap.put("env.getYAMLProperty.longProperty", "999");
        configMap.put("env.getYAMLProperty.floatProperty", "99.99");
        configMap.put("env.getYAMLProperty.doubleProperty", "99.99");

        siddhiManager.setConfigManager(new InMemoryConfigManager(configMap, null));
        siddhiManager.setExtension("env:getYAMLProperty", GetYAMLPropertyFunction.class);

        String stream = "define stream inputStream (key string);\n";

        String query = ("@info(name = 'query1') from inputStream "
                + "select env:getYAMLProperty(key,'string') as propertyValue "
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
                    AssertJUnit.assertEquals("StringValue", result);
                }
            }
        });
        InputHandler inputHandler = siddhiAppRuntime.getInputHandler("inputStream");
        siddhiAppRuntime.start();
        inputHandler.send(new String[]{"stringProperty"});
        siddhiAppRuntime.shutdown();
    }

    @Test
    public void testDefaultBehaviourWithWithDefaultValueProvided() throws Exception {
        logger.info("GetYAMLPropertyFunctionExtensionTestDefaultBehaviourWithWithDefaultValue TestCase");

        SiddhiManager siddhiManager = new SiddhiManager();

        String stream = "define stream inputStream (key string);\n";

        String query = ("@info(name = 'query1') from inputStream "
                + "select env:getYAMLProperty(key,'string','defaultValue') as propertyValue "
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
                    AssertJUnit.assertEquals(result, "defaultValue");
                }
            }
        });
        InputHandler inputHandler = siddhiAppRuntime.getInputHandler("inputStream");
        siddhiAppRuntime.start();
        inputHandler.send(new String[]{"stringProperty"});
        siddhiAppRuntime.shutdown();
    }

    @Test
    public void testBehaviourWithWithoutDefaultValue() throws Exception {
        logger.info("GetYAMLPropertyFunctionExtensionTestBehaviourWithWithoutDefaultValue TestCase");

        SiddhiManager siddhiManager = new SiddhiManager();

        String stream = "define stream inputStream (key string);\n";

        String query = ("@info(name = 'query1') from inputStream "
                + "select env:getYAMLProperty(key,'string') as propertyValue "
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
                    AssertJUnit.assertEquals(result, null);
                }
            }
        });
        InputHandler inputHandler = siddhiAppRuntime.getInputHandler("inputStream");
        siddhiAppRuntime.start();
        inputHandler.send(new String[]{"stringProperty"});
        siddhiAppRuntime.shutdown();
    }

    @Test
    public void testBehaviourWithBoolean() throws Exception {
        logger.info("GetYAMLPropertyFunctionExtensionBoolean TestCase");

        SiddhiManager siddhiManager = new SiddhiManager();
        Map<String, String> configMap = new HashMap<>();
        configMap.put("env.getYAMLProperty.booleanProperty", "true");

        siddhiManager.setConfigManager(new InMemoryConfigManager(configMap, null));
        siddhiManager.setExtension("env:getYAMLProperty", GetYAMLPropertyFunction.class);

        String stream = "define stream inputStream (key string);\n";

        String query = ("@info(name = 'query1') from inputStream "
                + "select env:getYAMLProperty(key,'bool') as propertyValue "
                + "insert into outputStream;");

        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(stream + query);
        siddhiAppRuntime.addCallback("query1", new QueryCallback() {
            @Override
            public void receive(long timeStamp, Event[] inEvents,
                                Event[] removeEvents) {
                EventPrinter.print(timeStamp, inEvents, removeEvents);
                boolean result;
                for (Event event : inEvents) {
                    result = (boolean) event.getData(0);
                    AssertJUnit.assertEquals(result, true);
                }
            }
        });
        InputHandler inputHandler = siddhiAppRuntime.getInputHandler("inputStream");
        siddhiAppRuntime.start();
        inputHandler.send(new String[]{"booleanProperty"});
        siddhiAppRuntime.shutdown();
    }

    @Test
    public void testBehaviourWithBooleanWithDefaultValueProvided() throws Exception {
        logger.info("GetYAMLPropertyFunctionExtensionTestBehaviourWithBooleanWithDefaultValueProvided TestCase");

        SiddhiManager siddhiManager = new SiddhiManager();

        String stream = "define stream inputStream (key string);\n";

        String query = ("@info(name = 'query1') from inputStream "
                + "select env:getYAMLProperty(key,'bool',false) as propertyValue "
                + "insert into outputStream;");

        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(stream + query);

        siddhiAppRuntime.addCallback("query1", new QueryCallback() {
            @Override
            public void receive(long timeStamp, Event[] inEvents,
                                Event[] removeEvents) {
                EventPrinter.print(timeStamp, inEvents, removeEvents);
                boolean result;
                for (Event event : inEvents) {
                    result = (boolean) event.getData(0);
                    AssertJUnit.assertEquals(result, false);
                }
            }
        });
        InputHandler inputHandler = siddhiAppRuntime.getInputHandler("inputStream");
        siddhiAppRuntime.start();
        inputHandler.send(new String[]{"booleanProperty"});
        siddhiAppRuntime.shutdown();
    }

    @Test
    public void testBehaviourWithInt() throws Exception {
        logger.info("GetYAMLPropertyFunctionExtensionTestBehaviourWithInt TestCase");

        SiddhiManager siddhiManager = new SiddhiManager();
        Map<String, String> configMap = new HashMap<>();
        configMap.put("env.getYAMLProperty.stringProperty", "StringValue");
        configMap.put("env.getYAMLProperty.booleanProperty", "true");
        configMap.put("env.getYAMLProperty.integerProperty", "99");
        configMap.put("env.getYAMLProperty.longProperty", "999");
        configMap.put("env.getYAMLProperty.floatProperty", "99.99");
        configMap.put("env.getYAMLProperty.doubleProperty", "99.99");

        siddhiManager.setConfigManager(new InMemoryConfigManager(configMap, null));
        siddhiManager.setExtension("env:getYAMLProperty", GetYAMLPropertyFunction.class);

        String stream = "define stream inputStream (key string);\n";

        String query = ("@info(name = 'query1') from inputStream "
                + "select env:getYAMLProperty(key,'int') as propertyValue "
                + "insert into outputStream;");

        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(stream + query);
        siddhiAppRuntime.addCallback("query1", new QueryCallback() {
            @Override
            public void receive(long timeStamp, Event[] inEvents,
                                Event[] removeEvents) {
                EventPrinter.print(timeStamp, inEvents, removeEvents);
                int result;
                for (Event event : inEvents) {
                    result = (Integer) event.getData(0);
                    AssertJUnit.assertEquals(99, result);
                }
            }
        });
        InputHandler inputHandler = siddhiAppRuntime.getInputHandler("inputStream");
        siddhiAppRuntime.start();
        inputHandler.send(new String[]{"integerProperty"});
        siddhiAppRuntime.shutdown();
    }

    @Test
    public void testBehaviourWithIntWithDefaultValueProvided() throws Exception {
        logger.info("GetYAMLPropertyFunctionExtensionTestBehaviourWithIntWithDefaultValueProvided TestCase");

        SiddhiManager siddhiManager = new SiddhiManager();
        Map<String, String> configMap = new HashMap<>();
        configMap.put("env.getYAMLProperty.stringProperty", "StringValue");
        configMap.put("env.getYAMLProperty.booleanProperty", "true");
        //     configMap.put("env.getYAMLProperty.integerProperty", "99");
        configMap.put("env.getYAMLProperty.longProperty", "999");
        configMap.put("env.getYAMLProperty.floatProperty", "99.99");
        configMap.put("env.getYAMLProperty.doubleProperty", "99.99");

        siddhiManager.setConfigManager(new InMemoryConfigManager(configMap, null));
        siddhiManager.setExtension("env:getYAMLProperty", GetYAMLPropertyFunction.class);

        String stream = "define stream inputStream (key string);\n";

        String query = ("@info(name = 'query1') from inputStream "
                + "select env:getYAMLProperty(key,'int',100) as propertyValue "
                + "insert into outputStream;");

        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(stream + query);
        siddhiAppRuntime.addCallback("query1", new QueryCallback() {
            @Override
            public void receive(long timeStamp, Event[] inEvents,
                                Event[] removeEvents) {
                EventPrinter.print(timeStamp, inEvents, removeEvents);
                int result;
                for (Event event : inEvents) {
                    result = (Integer) event.getData(0);
                    AssertJUnit.assertEquals(100, result);
                }
            }
        });
        InputHandler inputHandler = siddhiAppRuntime.getInputHandler("inputStream");
        siddhiAppRuntime.start();
        inputHandler.send(new String[]{"integerProperty"});
        siddhiAppRuntime.shutdown();
    }

    @Test
    public void testBehaviourWithLong() throws Exception {
        logger.info("GetYAMLPropertyFunctionExtensionTestBehaviourWithLong TestCase");

        SiddhiManager siddhiManager = new SiddhiManager();
        Map<String, String> configMap = new HashMap<>();
        configMap.put("env.getYAMLProperty.stringProperty", "StringValue");
        configMap.put("env.getYAMLProperty.booleanProperty", "true");
        configMap.put("env.getYAMLProperty.integerProperty", "99");
        configMap.put("env.getYAMLProperty.longProperty", "999");
        configMap.put("env.getYAMLProperty.floatProperty", "99.99");
        configMap.put("env.getYAMLProperty.doubleProperty", "99.99");

        siddhiManager.setConfigManager(new InMemoryConfigManager(configMap, null));
        siddhiManager.setExtension("env:getYAMLProperty", GetYAMLPropertyFunction.class);

        String stream = "define stream inputStream (key string);\n";

        String query = ("@info(name = 'query1') from inputStream "
                + "select env:getYAMLProperty(key,'long') as propertyValue "
                + "insert into outputStream;");

        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(stream + query);
        siddhiAppRuntime.addCallback("query1", new QueryCallback() {
            @Override
            public void receive(long timeStamp, Event[] inEvents,
                                Event[] removeEvents) {
                EventPrinter.print(timeStamp, inEvents, removeEvents);
                long result;
                for (Event event : inEvents) {
                    result = (long) event.getData(0);
                    AssertJUnit.assertEquals(999L, result);
                }
            }
        });
        InputHandler inputHandler = siddhiAppRuntime.getInputHandler("inputStream");
        siddhiAppRuntime.start();
        inputHandler.send(new String[]{"longProperty"});
        siddhiAppRuntime.shutdown();
    }

    @Test
    public void testBehaviourWithLongWithDefaultValueProvided() throws Exception {
        logger.info("GetYAMLPropertyFunctionExtensionTestBehaviourWithLongWithDefaultValueProvided TestCase");

        SiddhiManager siddhiManager = new SiddhiManager();
        Map<String, String> configMap = new HashMap<>();
        configMap.put("env.getYAMLProperty.stringProperty", "StringValue");
        configMap.put("env.getYAMLProperty.booleanProperty", "true");
        configMap.put("env.getYAMLProperty.integerProperty", "99");
        //  configMap.put("env.getYAMLProperty.longProperty", "999");
        configMap.put("env.getYAMLProperty.floatProperty", "99.99");
        configMap.put("env.getYAMLProperty.doubleProperty", "99.99");

        siddhiManager.setConfigManager(new InMemoryConfigManager(configMap, null));
        siddhiManager.setExtension("env:getYAMLProperty", GetYAMLPropertyFunction.class);

        String stream = "define stream inputStream (key string);\n";

        String query = ("@info(name = 'query1') from inputStream "
                + "select env:getYAMLProperty(key,'long',100l) as propertyValue "
                + "insert into outputStream;");

        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(stream + query);
        siddhiAppRuntime.addCallback("query1", new QueryCallback() {
            @Override
            public void receive(long timeStamp, Event[] inEvents,
                                Event[] removeEvents) {
                EventPrinter.print(timeStamp, inEvents, removeEvents);
                long result;
                for (Event event : inEvents) {
                    result = (long) event.getData(0);
                    AssertJUnit.assertEquals(100, result);
                }
            }
        });
        InputHandler inputHandler = siddhiAppRuntime.getInputHandler("inputStream");
        siddhiAppRuntime.start();
        inputHandler.send(new String[]{"longProperty"});
        siddhiAppRuntime.shutdown();
    }

    @Test
    public void testBehaviourWithDouble() throws Exception {
        logger.info("GetYAMLPropertyFunctionExtensionTestBehaviourWithDouble TestCase");

        SiddhiManager siddhiManager = new SiddhiManager();
        Map<String, String> configMap = new HashMap<>();
        configMap.put("env.getYAMLProperty.stringProperty", "StringValue");
        configMap.put("env.getYAMLProperty.booleanProperty", "true");
        configMap.put("env.getYAMLProperty.integerProperty", "99");
        configMap.put("env.getYAMLProperty.longProperty", "999");
        configMap.put("env.getYAMLProperty.floatProperty", "99.99");
        configMap.put("env.getYAMLProperty.doubleProperty", "99.99");

        siddhiManager.setConfigManager(new InMemoryConfigManager(configMap, null));
        siddhiManager.setExtension("env:getYAMLProperty", GetYAMLPropertyFunction.class);

        String stream = "define stream inputStream (key string);\n";

        String query = ("@info(name = 'query1') from inputStream "
                + "select env:getYAMLProperty(key,'double') as propertyValue "
                + "insert into outputStream;");

        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(stream + query);
        siddhiAppRuntime.addCallback("query1", new QueryCallback() {
            @Override
            public void receive(long timeStamp, Event[] inEvents,
                                Event[] removeEvents) {
                EventPrinter.print(timeStamp, inEvents, removeEvents);
                double result;
                for (Event event : inEvents) {
                    result = (double) event.getData(0);
                    AssertJUnit.assertEquals(99.99, result);
                }
            }
        });
        InputHandler inputHandler = siddhiAppRuntime.getInputHandler("inputStream");
        siddhiAppRuntime.start();
        inputHandler.send(new String[]{"doubleProperty"});
        siddhiAppRuntime.shutdown();
    }

    @Test
    public void testBehaviourWithDoubleWithDefaultValueProvided() throws Exception {
        logger.info("GetYAMLPropertyFunctionExtensionTestBehaviourWithDoubleWithDefaultValueProvided TestCase");

        SiddhiManager siddhiManager = new SiddhiManager();
        Map<String, String> configMap = new HashMap<>();
        configMap.put("env.getYAMLProperty.stringProperty", "StringValue");
        configMap.put("env.getYAMLProperty.booleanProperty", "true");
        configMap.put("env.getYAMLProperty.integerProperty", "99");
        configMap.put("env.getYAMLProperty.longProperty", "999");
        configMap.put("env.getYAMLProperty.floatProperty", "99.99");
        // configMap.put("env.getYAMLProperty.doubleProperty", "99.99");

        siddhiManager.setConfigManager(new InMemoryConfigManager(configMap, null));
        siddhiManager.setExtension("env:getYAMLProperty", GetYAMLPropertyFunction.class);

        String stream = "define stream inputStream (key string);\n";

        String query = ("@info(name = 'query1') from inputStream "
                + "select env:getYAMLProperty(key,'double',100.01) as propertyValue "
                + "insert into outputStream;");

        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(stream + query);
        siddhiAppRuntime.addCallback("query1", new QueryCallback() {
            @Override
            public void receive(long timeStamp, Event[] inEvents,
                                Event[] removeEvents) {
                EventPrinter.print(timeStamp, inEvents, removeEvents);
                double result;
                for (Event event : inEvents) {
                    result = (double) event.getData(0);
                    AssertJUnit.assertEquals(100.01, result);
                }
            }
        });
        InputHandler inputHandler = siddhiAppRuntime.getInputHandler("inputStream");
        siddhiAppRuntime.start();
        inputHandler.send(new String[]{"doubleProperty"});
        siddhiAppRuntime.shutdown();
    }

    @Test
    public void testBehaviourWithFloat() throws Exception {
        logger.info("GetYAMLPropertyFunctionExtensionTestBehaviourWithFloat TestCase");

        SiddhiManager siddhiManager = new SiddhiManager();
        Map<String, String> configMap = new HashMap<>();
        configMap.put("env.getYAMLProperty.stringProperty", "StringValue");
        configMap.put("env.getYAMLProperty.booleanProperty", "true");
        configMap.put("env.getYAMLProperty.integerProperty", "99");
        configMap.put("env.getYAMLProperty.longProperty", "999");
        configMap.put("env.getYAMLProperty.floatProperty", "99.99");
        configMap.put("env.getYAMLProperty.doubleProperty", "99.99");

        siddhiManager.setConfigManager(new InMemoryConfigManager(configMap, null));
        siddhiManager.setExtension("env:getYAMLProperty", GetYAMLPropertyFunction.class);

        String stream = "define stream inputStream (key string);\n";

        String query = ("@info(name = 'query1') from inputStream "
                + "select env:getYAMLProperty(key,'float') as propertyValue "
                + "insert into outputStream;");

        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(stream + query);
        siddhiAppRuntime.addCallback("query1", new QueryCallback() {
            @Override
            public void receive(long timeStamp, Event[] inEvents,
                                Event[] removeEvents) {
                EventPrinter.print(timeStamp, inEvents, removeEvents);
                float result;
                for (Event event : inEvents) {
                    result = (float) event.getData(0);
                    AssertJUnit.assertEquals(99.99f, result, 0.0f);
                }
            }
        });
        InputHandler inputHandler = siddhiAppRuntime.getInputHandler("inputStream");
        siddhiAppRuntime.start();
        inputHandler.send(new String[]{"floatProperty"});
        siddhiAppRuntime.shutdown();
    }

    @Test
    public void testBehaviourWithFloatWithDefaultValueProvided() throws Exception {
        logger.info("GetYAMLPropertyFunctionExtensionTestBehaviourWithFloatWithDefaultValueProvided TestCase");

        SiddhiManager siddhiManager = new SiddhiManager();
        Map<String, String> configMap = new HashMap<>();
        configMap.put("env.getYAMLProperty.stringProperty", "StringValue");
        configMap.put("env.getYAMLProperty.booleanProperty", "true");
        configMap.put("env.getYAMLProperty.integerProperty", "99");
        configMap.put("env.getYAMLProperty.longProperty", "999");
        //configMap.put("env.getYAMLProperty.floatProperty", "99.99");
        configMap.put("env.getYAMLProperty.doubleProperty", "99.99");

        siddhiManager.setConfigManager(new InMemoryConfigManager(configMap, null));
        siddhiManager.setExtension("env:getYAMLProperty", GetYAMLPropertyFunction.class);

        String stream = "define stream inputStream (key string);\n";

        String query = ("@info(name = 'query1') from inputStream "
                + "select env:getYAMLProperty(key,'float',100.01f) as propertyValue "
                + "insert into outputStream;");

        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(stream + query);
        siddhiAppRuntime.addCallback("query1", new QueryCallback() {
            @Override
            public void receive(long timeStamp, Event[] inEvents,
                                Event[] removeEvents) {
                EventPrinter.print(timeStamp, inEvents, removeEvents);
                float result;
                for (Event event : inEvents) {
                    result = (float) event.getData(0);
                    AssertJUnit.assertEquals(100.01f, result);
                }
            }
        });
        InputHandler inputHandler = siddhiAppRuntime.getInputHandler("inputStream");
        siddhiAppRuntime.start();
        inputHandler.send(new String[]{"floatProperty"});
        siddhiAppRuntime.shutdown();
    }

    @Test(expectedExceptions = SiddhiAppCreationException.class)
    public void exceptionTestCaseNullKey() {
        logger.info("GetYAMLPropertyFunctionExtension exceptionTestCaseNullKey");

        SiddhiManager siddhiManager = new SiddhiManager();

        String stream = "define stream inputStream (key string);\n";

        String query = ("@info(name = 'query1') from inputStream "
                + "select env:getYAMLProperty() as functionOutput "
                + "insert into outputStream;");

        siddhiManager.createSiddhiAppRuntime(stream + query);
    }

    @Test(expectedExceptions = SiddhiAppCreationException.class)
    public void exceptionTestCaseNonStringKey() {
        logger.info("GetYAMLPropertyFunctionExtension exceptionTestCaseNonStringKey");

        SiddhiManager siddhiManager = new SiddhiManager();

        String stream = "define stream inputStream (key string);\n";

        String query = ("@info(name = 'query1') from inputStream "
                + "select env:getYAMLProperty(5) as functionOutput "
                + "insert into outputStream;");

        siddhiManager.createSiddhiAppRuntime(stream + query);
    }

    @Test(expectedExceptions = SiddhiAppCreationException.class)
    public void exceptionTestCaseNonMatchingTypeDefaultValue() {
        logger.info("GetYAMLPropertyFunctionExtension exceptionTestCaseNonMatchingTypeDefaultValue");

        SiddhiManager siddhiManager = new SiddhiManager();

        String stream = "define stream inputStream (key string);\n";

        String query = ("@info(name = 'query1') from inputStream "
                + "select env:getYAMLProperty(key,'string',3) as functionOutput "
                + "insert into outputStream;");

        siddhiManager.createSiddhiAppRuntime(stream + query);
    }

    @Test(expectedExceptions = SiddhiAppCreationException.class)
    public void exceptionTestCaseInvalidDataType() {
        logger.info("GetYAMLPropertyFunctionExtension exceptionTestCaseInvalidDataType");

        SiddhiManager siddhiManager = new SiddhiManager();

        String stream = "define stream inputStream (key string);\n";

        String query = ("@info(name = 'query1') from inputStream "
                + "select env:getYAMLProperty(key,'invalidType',3) as functionOutput "
                + "insert into outputStream;");

        siddhiManager.createSiddhiAppRuntime(stream + query);
    }

    @Test(expectedExceptions = SiddhiAppCreationException.class)
    public void exceptionTestCaseVariableDataType() {
        logger.info("GetYAMLPropertyFunctionExtension exceptionTestCaseVariableDataType");

        SiddhiManager siddhiManager = new SiddhiManager();

        String stream = "define stream inputStream (key string);\n";

        String query = ("@info(name = 'query1') from inputStream "
                + "select env:getYAMLProperty(key,key,3) as functionOutput "
                + "insert into outputStream;");

        siddhiManager.createSiddhiAppRuntime(stream + query);
    }

    @Test
    public void exceptionTestCaseNonMatchingDataTypeForPropertyValue() throws Exception {
        logger.info("GetYAMLPropertyFunctionExtension exceptionTestCaseNonMatchingDataTypeForPropertyValue");

        SiddhiManager siddhiManager = new SiddhiManager();
        Map<String, String> configMap = new HashMap<>();
        configMap.put("env.getYAMLProperty.stringProperty", "StringValue");
        configMap.put("env.getYAMLProperty.booleanProperty", "true");
        configMap.put("env.getYAMLProperty.integerProperty", "99");
        configMap.put("env.getYAMLProperty.longProperty", "999");
        configMap.put("env.getYAMLProperty.floatProperty", "99.99f");
        configMap.put("env.getYAMLProperty.doubleProperty", "99.99");

        siddhiManager.setConfigManager(new InMemoryConfigManager(configMap, null));
        siddhiManager.setExtension("env:getYAMLProperty", GetYAMLPropertyFunction.class);

        String stream = "define stream inputStream (key string);\n";

        String query = ("@info(name = 'query1') from inputStream "
                + "select env:getYAMLProperty(key,'int',100) as propertyValue "
                + "insert into outputStream;");

        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(stream + query);
        InputHandler inputHandler = siddhiAppRuntime.getInputHandler("inputStream");
        LoggerCallBack loggerCallBack = new LoggerCallBack(regexPattern) {
            @Override
            public void receive(String logEventMessage) {
                isLogEventArrived = true;
            }
        };
        LoggerAppender.setLoggerCallBack(loggerCallBack);
        siddhiAppRuntime.start();
        inputHandler.send(new String[]{"floatProperty"});
        Assert.assertEquals(isLogEventArrived, true,
                "Matching log event not found for pattern: '" + regexPattern + "'");
        LoggerAppender.setLoggerCallBack(null);
        siddhiAppRuntime.shutdown();
    }
}
