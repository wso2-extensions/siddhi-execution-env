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
import io.siddhi.core.config.SiddhiQueryContext;
import io.siddhi.core.event.ComplexEventChunk;
import io.siddhi.core.event.stream.MetaStreamEvent;
import io.siddhi.core.event.stream.StreamEvent;
import io.siddhi.core.event.stream.StreamEventCloner;
import io.siddhi.core.event.stream.holder.StreamEventClonerHolder;
import io.siddhi.core.event.stream.populater.ComplexEventPopulater;
import io.siddhi.core.executor.ConstantExpressionExecutor;
import io.siddhi.core.executor.ExpressionExecutor;
import io.siddhi.core.query.processor.ProcessingMode;
import io.siddhi.core.query.processor.Processor;
import io.siddhi.core.query.processor.stream.StreamProcessor;
import io.siddhi.core.util.config.ConfigReader;
import io.siddhi.core.util.snapshot.state.State;
import io.siddhi.core.util.snapshot.state.StateFactory;
import io.siddhi.query.api.definition.AbstractDefinition;
import io.siddhi.query.api.definition.Attribute;
import io.siddhi.query.api.exception.SiddhiAppValidationException;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Siddhi Resource Identifier Stream Processor Extension to register the resources with name, and serve registered
 * resource count for given name.
 */
@Extension(
        name = "resourceIdentifier",
        namespace = "env",
        description = "The resource identifier stream processor is an extension to register a resource name with a" +
                " reference in a static map and serve a static resources count for a specific resource name.",
        parameters = {
                @Parameter(name = "resource.group.id",
                        description = "The name of the resource group.",
                        type = {DataType.STRING})
        },
        examples = {
                @Example(
                        syntax =
                                "@info(name='product_color_code_rule') \n" +
                                        "from SweetProductDefectsDetector#env:resourceIdentifier(\"rule-group-1\")\n" +
                                        "select productId, ifThenElse(colorCode == '#FF0000', true, false) as " +
                                        "isValid\n" +
                                        "insert into DefectDetectionResult;\n" +
                                        "\n" +
                                        "@info(name='product_dimensions_rule') \n" +
                                        "from SweetProductDefectsDetector#env:resourceIdentifier(\"rule-group-1\")\n" +
                                        "select productId, ifThenElse(height == 5 && width ==10, true, false) as " +
                                        "isValid\n" +
                                        "insert into DefectDetectionResult;\n" +
                                        "@info(name='defect_analyzer') \n" +
                                        "from DefectDetectionResult#window.env:resourceBatch(\"rule-group-1\", " +
                                        "productId, 60000)\n" +
                                        "select productId, and(not isValid) as isDefected\n" +
                                        "insert into SweetProductDefectAlert;",
                        description = "'product_color_code_rule' and 'product_dimensions_rule' are two rule-based " +
                                "queries that process the same events from the 'SweetProductDefectsDetector' stream." +
                                " They both insert their process results as the output into the " +
                                "'DefectDetectionResult' output stream.\n" +
                                "\n" +
                                "Multiple queries like this can be added in the Siddhi Application and the number of" +
                                " output events inserted into the 'DefectDetectionResult' stream depends on the " +
                                "number of available queries. If you need to further aggregate results for a " +
                                "particular correlation ID ('productId' in this scenario) from the " +
                                "'DefectDetectionResult' stream, follow-up queries need to wait for events with same" +
                                " value for the 'productId' attribute from all the available queries. For this, " +
                                "follow-up queries need to identify the number of events that can be expected from " +
                                "these rule-based queries with a specific value for 'productID'. To address this " +
                                "requirement, a resource identifier named 'rule-group-1' is assigned to both the " +
                                "rule queries. The 'defect_analyzer' query includes the 'env:resourceBatch' window" +
                                " to derive the count for the registered resource named 'rule-group-1' count from the" +
                                " output of both the queries within a specific time period. All of these factors " +
                                "determine the event waiting condition for events from the 'DefectDetectionResult'" +
                                " stream."
                )
        }
)
public class ResourceIdentifierStreamProcessor extends StreamProcessor<State> {
    private static Map<String, List<ResourceIdentifierStreamProcessor>> resourceIdentifyStreamProcessorMap =
            new ConcurrentHashMap<>();
    private String resourceName;

    @Override
    protected void process(ComplexEventChunk<StreamEvent> complexEventChunk, Processor nextProcessor,
                           StreamEventCloner streamEventCloner, ComplexEventPopulater complexEventPopulater,
                           State state) {
        nextProcessor.process(complexEventChunk);
    }

    @Override
    protected StateFactory<State> init(MetaStreamEvent metaStreamEvent, AbstractDefinition abstractDefinition,
                                       ExpressionExecutor[] attributeExpressionExecutors, ConfigReader configReader,
                                       StreamEventClonerHolder streamEventClonerHolder,
                                       boolean outputExpectsExpiredEvents, boolean findToBeExecuted,
                                       SiddhiQueryContext siddhiQueryContext) {
        int inputExecutorLength = attributeExpressionExecutors.length;
        if (inputExecutorLength == 1) {
            if (attributeExpressionExecutors[0] instanceof ConstantExpressionExecutor) {
                if (attributeExpressionExecutors[0].getReturnType() == Attribute.Type.STRING) {
                    resourceName = (String) ((ConstantExpressionExecutor) attributeExpressionExecutors[0]).getValue();
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
        return null;
    }

    @Override
    public void start() {
        if (resourceName != null) {
            List<ResourceIdentifierStreamProcessor> resourceIdentifierStreamProcessorList =
                    resourceIdentifyStreamProcessorMap.get(resourceName);
            if (resourceIdentifierStreamProcessorList != null) {
                resourceIdentifierStreamProcessorList.add(this);
            } else {
                List<ResourceIdentifierStreamProcessor> list = new ArrayList<>();
                list.add(this);
                resourceIdentifyStreamProcessorMap.put(resourceName, list);
            }
        }
    }

    @Override
    public void stop() {
        if (resourceName != null) {
            List<ResourceIdentifierStreamProcessor> resourceIdentifierStreamProcessorList =
                    resourceIdentifyStreamProcessorMap.get(resourceName);
            if (resourceIdentifierStreamProcessorList != null) {
                resourceIdentifierStreamProcessorList.remove(this);
                if (resourceIdentifierStreamProcessorList.size() == 0) {
                    resourceIdentifyStreamProcessorMap.remove(resourceName);
                }
            }
        }
    }

    public static int getResourceCount(String resourceName) {
        List<ResourceIdentifierStreamProcessor> resourceIdentifierStreamProcessorList =
                resourceIdentifyStreamProcessorMap.get(resourceName);
        if (resourceIdentifierStreamProcessorList != null) {
            return resourceIdentifierStreamProcessorList.size();
        } else {
            return 0;
        }
    }

    @Override
    public List<Attribute> getReturnAttributes() {
        return new ArrayList<Attribute>();
    }

    @Override
    public ProcessingMode getProcessingMode() {
        return ProcessingMode.BATCH;
    }
}
