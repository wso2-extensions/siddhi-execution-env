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
import org.wso2.siddhi.core.event.state.StateEvent;
import org.wso2.siddhi.core.event.stream.StreamEvent;
import org.wso2.siddhi.core.event.stream.StreamEventCloner;
import org.wso2.siddhi.core.executor.ConstantExpressionExecutor;
import org.wso2.siddhi.core.executor.ExpressionExecutor;
import org.wso2.siddhi.core.executor.VariableExpressionExecutor;
import org.wso2.siddhi.core.query.processor.Processor;
import org.wso2.siddhi.core.query.processor.stream.window.FindableProcessor;
import org.wso2.siddhi.core.query.processor.stream.window.WindowProcessor;
import org.wso2.siddhi.core.table.Table;
import org.wso2.siddhi.core.util.collection.operator.CompiledCondition;
import org.wso2.siddhi.core.util.collection.operator.MatchingMetaInfoHolder;
import org.wso2.siddhi.core.util.collection.operator.Operator;
import org.wso2.siddhi.core.util.config.ConfigReader;
import org.wso2.siddhi.core.util.parser.OperatorParser;
import org.wso2.siddhi.query.api.definition.Attribute;
import org.wso2.siddhi.query.api.exception.SiddhiAppValidationException;
import org.wso2.siddhi.query.api.expression.Expression;

import java.util.ArrayList;
import java.util.HashMap;
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
                "as grouping key and based on the resource count inferred from env:resourceIdentify extension. " +
                "The window is updated each time a batch of events with same key value that equals the number of " +
                "resources count.",
        parameters = {
                @Parameter(name = "resource.name",
                        description = "The resource name.",
                        type = {DataType.STRING}),
                @Parameter(name = "group.key.name",
                        description = "The attribute that should be used for events grouping.",
                        type = {DataType.INT, DataType.LONG, DataType.FLOAT,
                                DataType.BOOL, DataType.DOUBLE})
        },
        examples = {
                @Example(
                        syntax = "define window cseEventWindow (symbol string, price float, volume int) " +
                                "evn:resourceBatch('X', symbol) output all events;\n\n" +
                                "@info(name = 'query0')\n" +
                                "from cseEventStream\n" +
                                "insert into cseEventWindow;\n\n" +
                                "@info(name = 'query1')\n" +
                                "from cseEventWindow\n" +
                                "select symbol, sum(price) as price\n" +
                                "insert all events into outputStream ;",
                        description = "This will processing events as batches based on the registered 'X' resource " +
                                "count as a batch length and 'symbol' as grouping key and out put all events as " +
                                "chunk once the batches expired."
                )
        }
)
public class ResourceBatchWindowProcessor extends WindowProcessor implements FindableProcessor {
    private ComplexEventChunk<StreamEvent> currentEventChunk = new ComplexEventChunk<StreamEvent>(false);
    private ComplexEventChunk<StreamEvent> eventsToBeExpired = new ComplexEventChunk<StreamEvent>(false);
    private SiddhiAppContext siddhiAppContext;
    private StreamEvent resetEvent = null;
    private ExpressionExecutor groupKeyExpressionExecutor;
    private Map<Object, List<StreamEvent>> groupEventMap = new HashMap<>();
    private boolean outputExpectsExpiredEvents;
    private int windowLength;


    @Override
    protected void init(ExpressionExecutor[] attributeExpressionExecutors, ConfigReader configReader, boolean
            outputExpectsExpiredEvents, SiddhiAppContext siddhiAppContext) {
        this.siddhiAppContext = siddhiAppContext;
        this.outputExpectsExpiredEvents = outputExpectsExpiredEvents;
        if (attributeExpressionExecutors.length == 2) {
            if (attributeExpressionExecutors[0] instanceof ConstantExpressionExecutor) {
                if (attributeExpressionExecutors[0].getReturnType() == Attribute.Type.STRING) {
                    String resourceName = (String) (((ConstantExpressionExecutor) attributeExpressionExecutors[0])
                            .getValue());
                    windowLength = ResourceIdentifyStreamProcessor.getResourceCount(resourceName);
                } else {
                    throw new SiddhiAppValidationException(
                            "Resource Batch window's resource name parameter should be String, but found "
                                    + attributeExpressionExecutors[1].getReturnType());
                }
            } else {
                throw new SiddhiAppValidationException("Resource Batch window should have constant "
                        + "for resource name parameter but found a dynamic attribute " + attributeExpressionExecutors[1]
                        .getClass().getCanonicalName());
            }
            this.groupKeyExpressionExecutor = attributeExpressionExecutors[1];
        } else {
            throw new SiddhiAppValidationException(
                    "Resource batch window should only have two parameters, " + "but found "
                            + attributeExpressionExecutors.length + " input attributes");
        }
    }

    @Override
    protected void process(ComplexEventChunk<StreamEvent> streamEventChunk, Processor nextProcessor,
                           StreamEventCloner streamEventCloner) {
        List<ComplexEventChunk<StreamEvent>> streamEventChunks = new ArrayList<ComplexEventChunk<StreamEvent>>();
        synchronized (this) {
            ComplexEventChunk<StreamEvent> outputStreamEventChunk = new ComplexEventChunk<StreamEvent>(true);
            long currentTime = siddhiAppContext.getTimestampGenerator().currentTime();
            while (streamEventChunk.hasNext()) {
                StreamEvent streamEvent = streamEventChunk.next();
                StreamEvent clonedStreamEvent = streamEventCloner.copyStreamEvent(streamEvent);
                Object groupEventMapKey = groupKeyExpressionExecutor.execute(clonedStreamEvent);
                if (groupEventMap.containsKey(groupEventMapKey)) {
                    groupEventMap.get(groupEventMapKey).add(clonedStreamEvent);
                } else {
                    List<StreamEvent> list = new ArrayList<>();
                    list.add(clonedStreamEvent);
                    groupEventMap.put(groupEventMapKey, list);
                }
                int groupEventMapSize = groupEventMap.get(groupEventMapKey).size();
                if (groupEventMapSize == windowLength) {
                    //update current event chunk with event batch
                    for (StreamEvent event : groupEventMap.get(groupEventMapKey)) {
                        event.setTimestamp(currentTime);
                        currentEventChunk.add(event);
                    }
                    groupEventMap.remove(groupEventMapKey);
                    //update outputStreamEventChunk with expired events chunk
                    if (outputExpectsExpiredEvents) {
                        if (eventsToBeExpired.getFirst() != null) {
                            while (eventsToBeExpired.hasNext()) {
                                StreamEvent expiredEvent = eventsToBeExpired.next();
                                expiredEvent.setTimestamp(currentTime);
                            }
                            outputStreamEventChunk.add(eventsToBeExpired.getFirst());
                        }
                        eventsToBeExpired.clear();
                    }
                    //update outputStreamEventChunk and eventsToBeExpired with current event chunk
                    if (currentEventChunk.getFirst() != null) {
                        // add reset event in front of current events
                        outputStreamEventChunk.add(resetEvent);
                        resetEvent = null;
                        if (outputExpectsExpiredEvents) {
                            currentEventChunk.reset();
                            while (currentEventChunk.hasNext()) {
                                StreamEvent currentEvent = currentEventChunk.next();
                                StreamEvent toExpireEvent = streamEventCloner.copyStreamEvent(currentEvent);
                                toExpireEvent.setType(StreamEvent.Type.EXPIRED);
                                eventsToBeExpired.add(toExpireEvent);
                            }
                        }
                        resetEvent = streamEventCloner.copyStreamEvent(currentEventChunk.getFirst());
                        resetEvent.setType(ComplexEvent.Type.RESET);
                        outputStreamEventChunk.add(currentEventChunk.getFirst());
                    }
                    currentEventChunk.clear();
                    if (outputStreamEventChunk.getFirst() != null) {
                        streamEventChunks.add(outputStreamEventChunk);
                    }
                }
            }
        }
        for (ComplexEventChunk<StreamEvent> outputStreamEventChunk : streamEventChunks) {
            nextProcessor.process(outputStreamEventChunk);
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
            state.put("ExpiredEventChunk", eventsToBeExpired.getFirst());
            state.put("ResetEvent", resetEvent);
            state.put("GroupEventMap", groupEventMap);
        }
        return state;
    }


    @Override
    public synchronized void restoreState(Map<String, Object> state) {
        currentEventChunk.clear();
        currentEventChunk.add((StreamEvent) state.get("CurrentEventChunk"));
        eventsToBeExpired.clear();
        eventsToBeExpired.add((StreamEvent) state.get("ExpiredEventChunk"));
        resetEvent = (StreamEvent) state.get("ResetEvent");
        groupEventMap = (Map<Object, List<StreamEvent>>) state.get("GroupEventMap");
    }

    @Override
    public synchronized StreamEvent find(StateEvent matchingEvent, CompiledCondition compiledCondition) {
        synchronized (this) {
            return ((Operator) compiledCondition).find(matchingEvent, eventsToBeExpired, streamEventCloner);
        }
    }

    @Override
    public CompiledCondition compileCondition(Expression condition, MatchingMetaInfoHolder matchingMetaInfoHolder,
                                              SiddhiAppContext siddhiAppContext,
                                              List<VariableExpressionExecutor> variableExpressionExecutors,
                                              Map<String, Table> tableMap, String queryName) {
        synchronized (this) {
            return OperatorParser.constructOperator(eventsToBeExpired, condition, matchingMetaInfoHolder,
                    siddhiAppContext, variableExpressionExecutors, tableMap, this.queryName);
        }
    }
}
