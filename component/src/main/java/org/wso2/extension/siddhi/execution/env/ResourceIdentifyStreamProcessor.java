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
import org.wso2.siddhi.core.event.ComplexEventChunk;
import org.wso2.siddhi.core.event.stream.StreamEvent;
import org.wso2.siddhi.core.event.stream.StreamEventCloner;
import org.wso2.siddhi.core.event.stream.populater.ComplexEventPopulater;
import org.wso2.siddhi.core.executor.ConstantExpressionExecutor;
import org.wso2.siddhi.core.executor.ExpressionExecutor;
import org.wso2.siddhi.core.query.processor.Processor;
import org.wso2.siddhi.core.query.processor.stream.StreamProcessor;
import org.wso2.siddhi.core.util.config.ConfigReader;
import org.wso2.siddhi.query.api.definition.AbstractDefinition;
import org.wso2.siddhi.query.api.definition.Attribute;
import org.wso2.siddhi.query.api.exception.SiddhiAppValidationException;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Siddhi Resource Identify Stream Processor Extension to register the resources with name and serve registered
 * resource count for given name.
 */
@Extension(
        name = "resourceIdentify",
        namespace = "env",
        description = "The resource identify stream processor registering the resource name with reference " +
                "in static map. And serve static resources count for specific resource name.",
        parameters = {
                @Parameter(name = "resource.name",
                        description = "The resource name.",
                        type = {DataType.STRING})
        },
        examples = {
                @Example(
                        syntax = "from fooStream#env:resourceIdentify(\"X\")" +
                                "select *\n" +
                                "insert into barStream;",
                        description = "This will registering the resource 'X' in static map and unregister at the stop."
                )
        }
)
public class ResourceIdentifyStreamProcessor extends StreamProcessor {
    private static Map<String, List<ResourceIdentifyStreamProcessor>> resourceIdentifyStreamProcessorMap =
            new HashMap<>();
    private String resourceName;

    @Override
    protected void process(ComplexEventChunk<StreamEvent> complexEventChunk, Processor processor,
                           StreamEventCloner streamEventCloner, ComplexEventPopulater complexEventPopulater) {
        nextProcessor.process(complexEventChunk);
    }

    @Override
    protected List<Attribute> init(AbstractDefinition abstractDefinition, ExpressionExecutor[] expressionExecutors,
                                   ConfigReader configReader, SiddhiAppContext siddhiAppContext) {
        int inputExecutorLength = attributeExpressionExecutors.length;
        if (inputExecutorLength == 1) {
            if (attributeExpressionExecutors[0] instanceof ConstantExpressionExecutor) {
                if (attributeExpressionExecutors[0].getReturnType() == Attribute.Type.STRING) {
                    resourceName = (String) ((ConstantExpressionExecutor) attributeExpressionExecutors[0]).getValue();
                    if (resourceIdentifyStreamProcessorMap.containsKey(resourceName)) {
                        resourceIdentifyStreamProcessorMap.get(resourceName).add(this);
                    } else {
                        List<ResourceIdentifyStreamProcessor> list = new ArrayList<>();
                        list.add(this);
                        resourceIdentifyStreamProcessorMap.put(resourceName, list);
                    }
                } else {
                    throw new SiddhiAppValidationException("Resource Identify Stream Processor first parameter " +
                            "attribute should be string type but found " +
                            attributeExpressionExecutors[0].getReturnType());
                }
            } else {
                throw new SiddhiAppValidationException("Resource Identify Stream Processor should have " +
                        "constant parameter attributes but found a dynamic attribute " +
                        attributeExpressionExecutors[0].getClass().getCanonicalName());
            }
        } else {
            throw new SiddhiAppValidationException("Resource Identify Stream Processor should only have one " +
                    "parameter (<string> resource.name), but found " + attributeExpressionExecutors.length +
                    "input attributes");
        }
        return new ArrayList<Attribute>();
    }

    @Override
    public void start() {

    }

    @Override
    public void stop() {
        resourceIdentifyStreamProcessorMap.get(resourceName).remove(this);
        if (resourceIdentifyStreamProcessorMap.get(resourceName).size() == 0) {
            resourceIdentifyStreamProcessorMap.remove(resourceName);
        }
    }

    @Override
    public Map<String, Object> currentState() {
        return null;
    }

    @Override
    public void restoreState(Map<String, Object> map) {

    }

    public static int getResourceCount(String resourceName) {
        if (resourceIdentifyStreamProcessorMap.containsKey(resourceName)) {
            return resourceIdentifyStreamProcessorMap.get(resourceName).size();
        } else {
            return 0;
        }
    }
}
