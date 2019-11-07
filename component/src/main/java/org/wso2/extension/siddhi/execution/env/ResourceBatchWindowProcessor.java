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

import io.siddhi.annotation.Example;
import io.siddhi.annotation.Extension;
import io.siddhi.annotation.Parameter;
import io.siddhi.annotation.util.DataType;
import io.siddhi.core.config.SiddhiAppContext;
import io.siddhi.core.config.SiddhiQueryContext;
import io.siddhi.core.event.ComplexEvent;
import io.siddhi.core.event.ComplexEventChunk;
import io.siddhi.core.event.stream.MetaStreamEvent;
import io.siddhi.core.event.stream.StreamEvent;
import io.siddhi.core.event.stream.StreamEventCloner;
import io.siddhi.core.event.stream.holder.SnapshotableStreamEventQueue;
import io.siddhi.core.event.stream.holder.StreamEventClonerHolder;
import io.siddhi.core.event.stream.populater.ComplexEventPopulater;
import io.siddhi.core.executor.ConstantExpressionExecutor;
import io.siddhi.core.executor.ExpressionExecutor;
import io.siddhi.core.query.processor.ProcessingMode;
import io.siddhi.core.query.processor.Processor;
import io.siddhi.core.query.processor.SchedulingProcessor;
import io.siddhi.core.query.processor.stream.window.WindowProcessor;
import io.siddhi.core.util.Scheduler;
import io.siddhi.core.util.config.ConfigReader;
import io.siddhi.core.util.snapshot.state.SnapshotStateList;
import io.siddhi.core.util.snapshot.state.State;
import io.siddhi.core.util.snapshot.state.StateFactory;
import io.siddhi.query.api.definition.AbstractDefinition;
import io.siddhi.query.api.definition.Attribute;
import io.siddhi.query.api.exception.SiddhiAppValidationException;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
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
        description = "This extension is a resource batch (tumbling) window that holds a number of events based on" +
                " the resource count inferred from the 'env:resourceIdentifier' extension, and with a specific" +
                " attribute as the grouping key. " +
                "The window is updated each time a batch of events arrive with a matching value for the grouping key," +
                " and where the number of events is equal to the resource count.",
        parameters = {
                @Parameter(name = "resource.group.id",
                        description = "The name of the resource group.",
                        type = {DataType.STRING}),
                @Parameter(name = "correlation.id",
                        description = "The attribute that should be used for event correlation.",
                        type = {DataType.INT, DataType.LONG, DataType.FLOAT,
                                DataType.BOOL, DataType.DOUBLE}),
                @Parameter(name = "time.in.milliseconds",
                        description = "The time period to wait for the arrival of a new event before generating the" +
                                " output for events belonging to a specific batch and flushing them.",
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
                                "select productId, ifThenElse(colorCode == '#FF0000', true, false) as isValid\n" +
                                "insert into DefectDetectionResult;\n" +
                                "\n" +
                                "@info(name='product_dimensions_rule') \n" +
                                "from SweetProductDefectsDetector#env:resourceIdentifier(\"rule-group-1\")\n" +
                                "select productId, ifThenElse(height == 5 && width ==10, true, false) as isValid\n" +
                                "insert into DefectDetectionResult;\n" +
                                "\n" +
                                "@info(name='defect_analyzer') \n" +
                                "from DefectDetectionResult#window.env:resourceBatch(\"rule-group-1\", productId, " +
                                "60000)\n" +
                                "select productId, and(not isValid) as isDefected\n" +
                                "insert into SweetProductDefectAlert;",
                        description = "This example demonstrates the usage of the 'env:resourceBatch' widow " +
                                "extension with the 'env:resourceIdentifier' stream processor and the 'and' attribute" +
                                " aggregator extension.\n" +
                                "\n" +
                                "Data relating to the sweet production is received into the " +
                                "'SweetProductDefectsDetector' input stream. This data is consumed by two rule " +
                                "queries named 'product_color_code_rule' and 'product_dimensions_rule'.\n" +
                                "\n" +
                                "The follow-up query named 'defect_analyzer' needs to wait for the output results of" +
                                " both the rule queries mentioned above, and generate an output based on aggregated" +
                                " results.\n" +
                                "\n" +
                                "To ensure that each event from the 'SweetProductDefectsDetector' input stream is" +
                                " processed by both the rule queries, they are grouped together. This is done by" +
                                " assigning a resource identifier named 'rule-group-1' to each rule query.\n" +
                                "\n" +
                                "The 'defect_analyzer' follow-up query waits until an equal number of output events" +
                                " with the same value for the 'productID' attribute are generated by both the rule" +
                                " queries for the 'rule-group-1' resource identifier. Then it selects the events" +
                                " where  the product ID is matching and the value for the 'isValid' attribute is not" +
                                " 'true'.\n" +
                                "\n" +
                                "When deriving this output, a 'resourceBatch' time window of 2000 milliseconds is " +
                                "considered. This checks whether events that match the criteria outlined above " +
                                "occurs within a time period of 2000 milliseconds in a tumbling manner. If the " +
                                "criteria is not met within 2000 events, the events within that time window are " +
                                "considered expired and flushed from the window. If the criteria is met within the " +
                                "time window of 2000 milliseconds, the output is inserted into the" +
                                " \"SweetProductDefectAlert' output stream as boolean values where 'isDefected'" +
                                " attribute is set to 'true'. The sample output can be as given below.\n" +
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
public class ResourceBatchWindowProcessor extends WindowProcessor<ResourceBatchWindowProcessor.ExtensionState>
        implements SchedulingProcessor {
    private static final int LATE_EVENT_FLUSHING_DURATION = 300000; //5 minutes
    private SnapshotableStreamEventQueue eventsToBeExpiredEventChunk =
            new SnapshotableStreamEventQueue(streamEventClonerHolder);
    private SiddhiAppContext siddhiAppContext;
    private ExpressionExecutor groupKeyExpressionExecutor;
    private Map<Object, ResourceStreamEventList> groupEventMap = new LinkedHashMap<>();
    private boolean outputExpectsExpiredEvents;
    private long timeInMilliSeconds = 300000; //5 minutes
    private Scheduler scheduler;
    private long nextEmitTime = -1;
    private String resourceName;

    @Override
    protected StateFactory<ExtensionState> init(MetaStreamEvent metaStreamEvent, AbstractDefinition inputDefinition,
                                                ExpressionExecutor[] attributeExpressionExecutors,
                                                ConfigReader configReader,
                                                StreamEventClonerHolder streamEventClonerHolder,
                                                boolean outputExpectsExpiredEvents, boolean findToBeExecuted,
                                                SiddhiQueryContext siddhiQueryContext) {
        this.siddhiAppContext = siddhiQueryContext.getSiddhiAppContext();
        this.outputExpectsExpiredEvents = outputExpectsExpiredEvents;
        if (attributeExpressionExecutors.length >= 2) {
            if (attributeExpressionExecutors[0] instanceof ConstantExpressionExecutor) {
                if (attributeExpressionExecutors[0].getReturnType() == Attribute.Type.STRING) {
                    resourceName = (String) (((ConstantExpressionExecutor) attributeExpressionExecutors[0])
                            .getValue());
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
        return () -> new ExtensionState();
    }

    @Override
    protected void processEventChunk(ComplexEventChunk<StreamEvent> streamEventChunk, Processor nextProcessor,
                                     StreamEventCloner streamEventCloner, ComplexEventPopulater complexEventPopulater,
                                     ExtensionState state) {
        synchronized (this) {
            long currentTime = siddhiAppContext.getTimestampGenerator().currentTime();
            ComplexEventChunk<StreamEvent> outputStreamEventChunk = new ComplexEventChunk<StreamEvent>(true);
            //update outputStreamEventChunk with expired events chunk
            if (eventsToBeExpiredEventChunk.getFirst() != null) {
                while (eventsToBeExpiredEventChunk.hasNext()) {
                    eventsToBeExpiredEventChunk.next().setTimestamp(currentTime);
                }
                outputStreamEventChunk.add(eventsToBeExpiredEventChunk.getFirst());
            }
            eventsToBeExpiredEventChunk.clear();
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
                        if (newNextEmitTime > nextEmitTime) {
                            nextEmitTime = newNextEmitTime;
                            if (scheduler != null) {
                                scheduler.notifyAt(nextEmitTime);
                                //schedule the notifier for late event flushing
                                scheduler.notifyAt(nextEmitTime + LATE_EVENT_FLUSHING_DURATION);
                            }
                        }
                    }
                }
                Iterator entryIterator = groupEventMap.entrySet().iterator();
                while (entryIterator.hasNext()) {
                    Map.Entry<Object, ResourceStreamEventList> entry = (Map.Entry) entryIterator.next();
                    int windowLength = ResourceIdentifierStreamProcessor.getResourceCount(resourceName);
                    List<StreamEvent> streamEventList = null;
                    if (entry.getValue().isExpired) {
                        //flushing the late events if the entries already expired and late event flushing interval
                        //less than the current time.
                        if ((entry.getValue().expiryTimestamp + LATE_EVENT_FLUSHING_DURATION) < currentTime) {
                            entryIterator.remove();
                        }
                    } else {
                        if (entry.getValue().streamEventList.size() >= windowLength ||
                                entry.getValue().expiryTimestamp + LATE_EVENT_FLUSHING_DURATION < currentTime) {
                            //events add into output stream event list and remove entry map entry as
                            // the LATE_EVENT_FLUSHING_DURATION interval exceed
                            streamEventList = entry.getValue().streamEventList;
                            entryIterator.remove();
                        } else if (entry.getValue().expiryTimestamp < currentTime) {
                            //events add into output stream event list, and wait 'LATE_EVENT_FLUSHING_DURATION' for
                            // late events before removing the map entry
                            streamEventList = entry.getValue().streamEventList;
                            entry.getValue().setExpired(true);
                        }
                    }
                    if (streamEventList != null) {
                        if (outputExpectsExpiredEvents) {
                            for (StreamEvent event : entry.getValue().streamEventList) {
                                StreamEvent toExpireEvent = streamEventCloner.copyStreamEvent(event);
                                toExpireEvent.setType(StreamEvent.Type.EXPIRED);
                                eventsToBeExpiredEventChunk.add(toExpireEvent);
                            }
                        }
                        StreamEvent toResetEvent = streamEventCloner.copyStreamEvent(streamEventList.get(0));
                        toResetEvent.setType(ComplexEvent.Type.RESET);
                        eventsToBeExpiredEventChunk.add(toResetEvent);
                        for (StreamEvent event : entry.getValue().streamEventList) {
                            event.setType(StreamEvent.Type.CURRENT);
                            outputStreamEventChunk.add(event);
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
    public Scheduler getScheduler() {
        return scheduler;
    }

    @Override
    public void setScheduler(Scheduler scheduler) {
        this.scheduler = scheduler;
    }

    @Override
    public ProcessingMode getProcessingMode() {
        return ProcessingMode.BATCH;
    }

    /**
     * Resource Stream Event List internal data structure.
     */
    public static class ResourceStreamEventList {
        private List<StreamEvent> streamEventList;
        private long expiryTimestamp;
        private boolean isExpired;

        public ResourceStreamEventList(List<StreamEvent> streamEventList, long expiryTimestamp) {
            this.streamEventList = streamEventList;
            this.expiryTimestamp = expiryTimestamp;
        }

        public void setExpired(boolean isExpired) {
            this.isExpired = isExpired;
        }
    }

    class ExtensionState extends State {

        @Override
        public boolean canDestroy() {

            return false;
        }

        @Override
        public Map<String, Object> snapshot() {
            Map<String, Object> state = new HashMap<>();
            synchronized (this) {
                if (eventsToBeExpiredEventChunk != null) {
                    state.put("ExpiredEventQueue", eventsToBeExpiredEventChunk.getSnapshot());
                }
                state.put("GroupEventMap", groupEventMap);
            }
            return state;
        }

        @Override
        public synchronized void restore(Map<String, Object> state) {
            if (eventsToBeExpiredEventChunk != null) {
                eventsToBeExpiredEventChunk.restore((SnapshotStateList) state.get("ExpiredEventQueue"));
            }
            groupEventMap = (Map<Object, ResourceStreamEventList>) state.get("GroupEventMap");
        }
    }
}
