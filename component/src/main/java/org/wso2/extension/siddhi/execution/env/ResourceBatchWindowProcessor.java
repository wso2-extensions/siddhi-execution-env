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

import org.wso2.siddhi.annotation.Example;
import org.wso2.siddhi.annotation.Extension;
import org.wso2.siddhi.annotation.Parameter;
import org.wso2.siddhi.annotation.util.DataType;
import org.wso2.siddhi.core.config.SiddhiAppContext;
import org.wso2.siddhi.core.event.ComplexEvent;
import org.wso2.siddhi.core.event.ComplexEventChunk;
import org.wso2.siddhi.core.event.stream.StreamEvent;
import org.wso2.siddhi.core.event.stream.StreamEventCloner;
import org.wso2.siddhi.core.executor.ConstantExpressionExecutor;
import org.wso2.siddhi.core.executor.ExpressionExecutor;
import org.wso2.siddhi.core.query.processor.Processor;
import org.wso2.siddhi.core.query.processor.SchedulingProcessor;
import org.wso2.siddhi.core.query.processor.stream.window.WindowProcessor;
import org.wso2.siddhi.core.util.Scheduler;
import org.wso2.siddhi.core.util.config.ConfigReader;
import org.wso2.siddhi.query.api.definition.Attribute;
import org.wso2.siddhi.query.api.exception.SiddhiAppValidationException;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Implementation of {@link WindowProcessor} which represent a Resource Batch Window operating based on
 * the inferred resources count from env:resourceIdentify extension as length.
 */
@Extension(
        name = "resourceBatch",
        namespace = "env",
        description = "A resource batch (tumbling) window that holds a number of events with specified attribute " +
                "as grouping key and based on the resource count inferred from env:resourceIdentifier extension. " +
                "The window is updated each time a batch of events with same key value that equals the number of " +
                "resources count.",
        parameters = {
                @Parameter(name = "resource.group.id",
                        description = "The resource group name.",
                        type = {DataType.STRING}),
                @Parameter(name = "correlation.id",
                        description = "The attribute that should be used for event correlation.",
                        type = {DataType.INT, DataType.LONG, DataType.FLOAT,
                                DataType.BOOL, DataType.DOUBLE}),
                @Parameter(name = "time.in.milliseconds",
                        description = "Time to wait for arrival of new event, before flushing " +
                                "and giving output for events belonging to a specific batch.",
                        type = {DataType.INT, DataType.LONG, DataType.TIME},
                        optional = true,
                        defaultValue = "300000")
        },
        examples = {
                @Example(
                        syntax = "define stream SweetProductDefectsDetector(productId string, colorCode string, " +
                                "height long, width long);\n" +
                                "define stream SweetProductDefectAlert(productId string, isDefected bool);\n" +
                                "\n" +
                                "@info(name='product_color_code_rule') \n" +
                                "from SweetProductDefectsDetector#env:resourceIdentifier(\"rule-group-1\")\n" +
                                "select productId, if(colorCode == '#FF0000', true, false) as isValid\n" +
                                "insert into DefectDetectionResult;\n" +
                                "\n" +
                                "@info(name='product_dimensions_rule') \n" +
                                "from SweetProductDefectsDetector#env:resourceIdentifier(\"rule-group-1\")\n" +
                                "select productId, if(height == 5 && width ==10, true, false) as isValid\n" +
                                "insert into DefectDetectionResult;\n" +
                                "\n" +
                                "@info(name='defect_analyzer') \n" +
                                "from DefectDetectionResult#window.env:resourceBatch(\"rule-group-1\", productId, " +
                                "60000)\n" +
                                "select productId, and(not isValid) as isDefected\n" +
                                "insert into SweetProductDefectAlert;",
                        description = "This example demonstrate the usage of 'env:resourceBatch' widow " +
                                "extension with 'env:resourceIdentifier' stream processor and 'and' attribute " +
                                "aggregator extensions.\n " +
                                "Use Case: The SweetProductDefectsDetector gets the Sweet Production data as " +
                                "an input stream and each event will be sent to the 'rule' queries( " +
                                "'product_color_code_rule' and 'product_dimensions_rule') . The query " +
                                "'defect_analyzer' should wait for both the output results from the 'rule' " +
                                "queries output and based on the aggregated results(take the logical AND " +
                                "aggregation of the 'isValid' attribute both events from 'product_color_code_rule' " +
                                "and 'product_dimensions_rule'), generate events and insert into the output stream  " +
                                "'SweetProductDefectAlert'.\n" +
                                "In the above example, a number of 'rule' queries can be changed and the " +
                                "'defect_analyzer' query should wait for results from the all available rules.\n" +
                                "\n" +
                                "To address this use case, we have defined the same resource.group.id: rule-group-1 " +
                                "in all the 'rule' queries, and its registering the resources using " +
                                "'resourceIdentifier' extension.  In the 'defect_analyzer' " +
                                "query we defined the env:resourceBatch(\"rule-group-1\", productId, 2000) " +
                                "window as it will accumulating the events with correlation.id:productId, " +
                                "where it holds the events for same 'productId' until it matches the number of " +
                                "available \"rule-group-1\" resources or flushing the events if the " +
                                "timeout(time.in.milliseconds:2000) occurs.\n" +
                                "To aggregate the results from 'rule' queries, we have used 'and(not isValid)' " +
                                "attribute aggregator where it logically computes AND operation of not isValid " +
                                "boolean attribute values and outputs the results as a boolean value.\n" +
                                "\n" +
                                "Input 1: [SweetProductDefectsDetector]\n" +
                                "{  \n" +
                                "   \"event\":{  \n" +
                                "      \"productId\":\"Cake\",\n" +
                                "      \"colorCode\":\"FF0000\",\n" +
                                "      \"height\": 5,\n" +
                                "      \"width\": 10\n" +
                                "\n" +
                                "   }\n" +
                                "}\n" +
                                "\n" +
                                "Output 1:[SweetProductDefectAlert]\n" +
                                "{  \n" +
                                "   \"event\":{  \n" +
                                "      \"productId\":\"Cake\",\n" +
                                "      \"isDefected\":\"false\"\n" +
                                "   }\n" +
                                "}\n" +
                                "\n" +
                                "Input 2: [SweetProductDefectsDetector]\n" +
                                "{  \n" +
                                "   \"event\":{  \n" +
                                "      \"productId\":\"Cake\",\n" +
                                "      \"colorCode\":\"FF0000\",\n" +
                                "      \"height\": 10,\n" +
                                "      \"width\": 20\n" +
                                "\n" +
                                "   }\n" +
                                "}\n" +
                                "\n" +
                                "Output 2:[SweetProductDefectAlert]\n" +
                                "{  \n" +
                                "   \"event\":{  \n" +
                                "      \"productId\":\"Cake\",\n" +
                                "      \"isDefected\":\"true\"\n" +
                                "   }\n" +
                                "}"
                )
        }
)
public class ResourceBatchWindowProcessor extends WindowProcessor implements SchedulingProcessor {
    private ComplexEventChunk<StreamEvent> currentEventChunk = new ComplexEventChunk<StreamEvent>(false);
    private ComplexEventChunk<StreamEvent> eventsToBeExpired;
    private SiddhiAppContext siddhiAppContext;
    private StreamEvent resetEvent = null;
    private ExpressionExecutor groupKeyExpressionExecutor;
    private Map<Object, ResourceStreamEventList> groupEventMap = new LinkedHashMap<>();
    private boolean outputExpectsExpiredEvents;
    private int windowLength;
    private long timeInMilliSeconds = 300000; //5 minutes
    private Scheduler scheduler;
    private long nextEmitTime = -1;
    private String resourceName;

    @Override
    protected void init(ExpressionExecutor[] attributeExpressionExecutors, ConfigReader configReader, boolean
            outputExpectsExpiredEvents, SiddhiAppContext siddhiAppContext) {
        this.siddhiAppContext = siddhiAppContext;
        this.outputExpectsExpiredEvents = outputExpectsExpiredEvents;
        if (outputExpectsExpiredEvents) {
            eventsToBeExpired = new ComplexEventChunk<StreamEvent>(false);
        }
        if (attributeExpressionExecutors.length >= 2) {
            if (attributeExpressionExecutors[0] instanceof ConstantExpressionExecutor) {
                if (attributeExpressionExecutors[0].getReturnType() == Attribute.Type.STRING) {
                    resourceName = (String) (((ConstantExpressionExecutor) attributeExpressionExecutors[0])
                            .getValue());
                    windowLength = ResourceIdentifierStreamProcessor.getResourceCount(resourceName);
                } else {
                    throw new SiddhiAppValidationException(
                            "Resource Batch window's 'resource.group.id' parameter should be String, but found "
                                    + attributeExpressionExecutors[1].getReturnType());
                }
            } else {
                throw new SiddhiAppValidationException("Resource Batch window should have constant "
                        + "for 'resource.group.id' parameter but found a dynamic attribute " +
                        attributeExpressionExecutors[1].getClass().getCanonicalName());
            }
            this.groupKeyExpressionExecutor = attributeExpressionExecutors[1];
            if (attributeExpressionExecutors.length == 3) {
                this.groupKeyExpressionExecutor = attributeExpressionExecutors[1];
                if ((attributeExpressionExecutors[2] instanceof ConstantExpressionExecutor)) {
                    if (attributeExpressionExecutors[2].getReturnType() == Attribute.Type.INT) {
                        timeInMilliSeconds = Integer.parseInt(String.valueOf(((ConstantExpressionExecutor)
                                attributeExpressionExecutors[2]).getValue()));
                    } else if (attributeExpressionExecutors[2].getReturnType() == Attribute.Type.LONG) {
                        timeInMilliSeconds = Long.parseLong(String.valueOf(((ConstantExpressionExecutor)
                                attributeExpressionExecutors[2]).getValue()));
                    } else {
                        throw new SiddhiAppValidationException("ResourceBatch window's 3rd parameter " +
                                "'time.in.milliseconds' should be either be a constant (of type int or long), " +
                                "but found " + attributeExpressionExecutors[2].getReturnType());
                    }
                } else {
                    throw new SiddhiAppValidationException("ResourceBatch window's 3rd parameter " +
                            "'time.in.milliseconds' should either be a constant (of type int or long)");
                }
            }
        } else {
            throw new SiddhiAppValidationException(
                    "Resource batch window should only have two or three parameters, but found "
                            + attributeExpressionExecutors.length + " input attributes");
        }
    }

    @Override
    protected void process(ComplexEventChunk<StreamEvent> streamEventChunk, Processor nextProcessor,
                           StreamEventCloner streamEventCloner) {
        synchronized (this) {
            long currentTime = siddhiAppContext.getTimestampGenerator().currentTime();
            ComplexEventChunk<StreamEvent> outputStreamEventChunk = new ComplexEventChunk<StreamEvent>(true);
            while (streamEventChunk.hasNext()) {
                StreamEvent streamEvent = streamEventChunk.next();
                StreamEvent clonedStreamEvent = streamEventCloner.copyStreamEvent(streamEvent);
                if (!streamEvent.getType().equals(ComplexEvent.Type.TIMER)) {
                    Object groupEventMapKey = groupKeyExpressionExecutor.execute(clonedStreamEvent);
                    ResourceStreamEventList resourceStreamEventList = groupEventMap.get(groupEventMapKey);
                    if (resourceStreamEventList != null) {
                        resourceStreamEventList.streamEventList.add(clonedStreamEvent);
                    } else {
                        List<StreamEvent> list = new ArrayList<>();
                        list.add(clonedStreamEvent);
                        groupEventMap.put(groupEventMapKey, new ResourceStreamEventList(list,
                                clonedStreamEvent.getTimestamp() + timeInMilliSeconds));
                        long newNextEmitTime = currentTime + timeInMilliSeconds;
                        if (nextEmitTime == -1 || newNextEmitTime > nextEmitTime) {
                            nextEmitTime = newNextEmitTime;
                            if (scheduler != null) {
                                scheduler.notifyAt(nextEmitTime);
                            }
                        }
                    }
                }
                if (outputExpectsExpiredEvents) {
                    for (Map.Entry<Object, ResourceStreamEventList> entry : groupEventMap.entrySet()) {
                        windowLength = ResourceIdentifierStreamProcessor.getResourceCount(resourceName);
                        if (entry.getValue().streamEventList.size() >= windowLength ||
                                entry.getValue().expiryTimestamp < currentTime) {
                            //update current event chunk with event batch
                            for (StreamEvent event : entry.getValue().streamEventList) {
                                currentEventChunk.add(event);
                            }
                            groupEventMap.remove(entry.getKey());
                            //update outputStreamEventChunk with expired events chunk
                            if (eventsToBeExpired.getFirst() != null) {
                                while (eventsToBeExpired.hasNext()) {
                                    StreamEvent expiredEvent = eventsToBeExpired.next();
                                    expiredEvent.setTimestamp(currentTime);
                                }
                                outputStreamEventChunk.add(eventsToBeExpired.getFirst());
                            }
                            eventsToBeExpired.clear();
                            //update outputStreamEventChunk and eventsToBeExpired with current event chunk
                            if (currentEventChunk.getFirst() != null) {
                                // add reset event in front of current events
                                outputStreamEventChunk.add(resetEvent);
                                resetEvent = null;
                                currentEventChunk.reset();
                                while (currentEventChunk.hasNext()) {
                                    StreamEvent currentEvent = currentEventChunk.next();
                                    StreamEvent toExpireEvent = streamEventCloner.copyStreamEvent(currentEvent);
                                    toExpireEvent.setType(StreamEvent.Type.EXPIRED);
                                    eventsToBeExpired.add(toExpireEvent);
                                }
                                resetEvent = streamEventCloner.copyStreamEvent(eventsToBeExpired.getFirst());
                                resetEvent.setType(ComplexEvent.Type.RESET);
                                outputStreamEventChunk.add(currentEventChunk.getFirst());
                            }
                            currentEventChunk.clear();
                        }
                    }
                } else {
                    for (Map.Entry<Object, ResourceStreamEventList> entry : groupEventMap.entrySet()) {
                        windowLength = ResourceIdentifierStreamProcessor.getResourceCount(resourceName);
                        if (entry.getValue().streamEventList.size() >= windowLength ||
                                entry.getValue().expiryTimestamp < currentTime) {
                            //update current event chunk with event batch
                            for (StreamEvent event : entry.getValue().streamEventList) {
                                currentEventChunk.add(event);
                            }
                            groupEventMap.remove(entry.getKey());
                            //update outputStreamEventChunk and eventsToBeExpired with current event chunk
                            if (currentEventChunk.getFirst() != null) {
                                // add reset event in front of current events
                                outputStreamEventChunk.add(resetEvent);
                                resetEvent = null;
                                resetEvent = streamEventCloner.copyStreamEvent(currentEventChunk.getFirst());
                                resetEvent.setType(ComplexEvent.Type.RESET);
                                outputStreamEventChunk.add(currentEventChunk.getFirst());
                            }
                            currentEventChunk.clear();
                        }
                    }
                }

            }
            streamEventChunk.clear();
            if (outputStreamEventChunk.getFirst() != null) {
                outputStreamEventChunk.setBatch(true);
                nextProcessor.process(outputStreamEventChunk);
                outputStreamEventChunk.setBatch(false);
            }
        }
    }

    @Override
    public void start() {
        //Do nothing
    }

    @Override
    public void stop() {
        //Do nothing
    }

    @Override
    public Map<String, Object> currentState() {
        Map<String, Object> state = new HashMap<>();
        synchronized (this) {
            state.put("CurrentEventChunk", currentEventChunk.getFirst());
            state.put("ExpiredEventChunk", eventsToBeExpired != null ? eventsToBeExpired.getFirst() : null);
            state.put("ResetEvent", resetEvent);
            state.put("GroupEventMap", groupEventMap);
        }
        return state;
    }

    @Override
    public synchronized void restoreState(Map<String, Object> state) {
        currentEventChunk.clear();
        currentEventChunk.add((StreamEvent) state.get("CurrentEventChunk"));
        if (eventsToBeExpired != null) {
            eventsToBeExpired.clear();
            eventsToBeExpired.add((StreamEvent) state.get("ExpiredEventChunk"));
        } else {
            if (outputExpectsExpiredEvents) {
                eventsToBeExpired = new ComplexEventChunk<StreamEvent>(false);
            }
        }
        resetEvent = (StreamEvent) state.get("ResetEvent");
        groupEventMap = (Map<Object, ResourceStreamEventList>) state.get("GroupEventMap");
    }

    @Override
    public Scheduler getScheduler() {
        return scheduler;
    }

    @Override
    public void setScheduler(Scheduler scheduler) {
        this.scheduler = scheduler;
    }

    /**
     * Resource Stream Event List internal data structure.
     */
    public static class ResourceStreamEventList {
        private List<StreamEvent> streamEventList;
        private long expiryTimestamp;

        public ResourceStreamEventList(List<StreamEvent> streamEventList, long expiryTimestamp) {
            this.streamEventList = streamEventList;
            this.expiryTimestamp = expiryTimestamp;
        }
    }
}
