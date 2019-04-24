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
import io.siddhi.annotation.ReturnAttribute;
import io.siddhi.annotation.util.DataType;
import io.siddhi.core.config.SiddhiQueryContext;
import io.siddhi.core.exception.SiddhiAppRuntimeException;
import io.siddhi.core.executor.ExpressionExecutor;
import io.siddhi.core.executor.function.FunctionExecutor;
import io.siddhi.core.util.config.ConfigReader;
import io.siddhi.core.util.snapshot.state.State;
import io.siddhi.core.util.snapshot.state.StateFactory;
import io.siddhi.query.api.definition.Attribute;
import io.siddhi.query.api.exception.SiddhiAppValidationException;

/**
 * Siddhi Function getEnvironmentProperty to read Java environment Properties.
 */

@Extension(
        name = "getEnvironmentProperty",
        namespace = "env",
        description = "This function returns the Java environment property that corresponds with the key provided",
        returnAttributes = @ReturnAttribute(
                description = "A string value is returned.",
                type = {io.siddhi.annotation.util.DataType.STRING}),
        parameters = {
                @Parameter(name = "key",
                        description = "This specifies the key of the property to be read.",
                        type = {DataType.STRING},
                        optional = false),
                @Parameter(name = "default.value",
                        description = "This specifies the default value to be returned if the property value is not" +
                                " available.",
                        type = {DataType.STRING},
                        optional = true,
                        defaultValue = "null")
        },
        examples = {
                @Example(
                        syntax = "define stream keyStream (key string);\n" +
                                "from keyStream env:getEnvironmentProperty(key) as FunctionOutput \n" +
                                "insert into outputStream;",
                        description = "This query returns the Java environment property that corresponds with " +
                                "the key from the 'keyStream' as 'FunctionOutput' to the outputStream."
                )
        }
)
public class GetEnvironmentPropertyFunction extends FunctionExecutor {

    /**
     * The initialization method for GetEnvironmentPropertyFunction,
     * this method will be called before the other methods.
     *
     * @param attributeExpressionExecutors the executors of each function parameter.
     * @param siddhiQueryContext           the context of the siddhi query.
     */

    protected StateFactory init(ExpressionExecutor[] attributeExpressionExecutors, ConfigReader reader,
                                SiddhiQueryContext siddhiQueryContext) {
        int attributeExpressionExecutorsLength = attributeExpressionExecutors.length;
        if ((attributeExpressionExecutorsLength > 0) && (attributeExpressionExecutorsLength < 3)) {
            Attribute.Type typeofKeyAttribute = attributeExpressionExecutors[0].getReturnType();
            if (typeofKeyAttribute != Attribute.Type.STRING) {
                throw new SiddhiAppValidationException("Invalid parameter type found " +
                        "for the argument key of getEnvironmentProperty() function, " +
                        "required " + Attribute.Type.STRING +
                        ", but found " + typeofKeyAttribute.toString());
            }
            if (attributeExpressionExecutorsLength == 2) {
                Attribute.Type typeofDefaultValueAttribute = attributeExpressionExecutors[1].getReturnType();
                if (typeofDefaultValueAttribute != Attribute.Type.STRING) {
                    throw new SiddhiAppValidationException("Invalid parameter type found " +
                            "for the argument default.value of getEnvironmentProperty() function, " +
                            "required " + Attribute.Type.STRING +
                            ", but found " + typeofDefaultValueAttribute.toString());
                }
            }
        } else {
            throw new SiddhiAppValidationException("Invalid no of arguments passed to " +
                    "env:getEnvironmentProperty(Key,default.value) function, " +
                    "required 1 or 2, but found " + attributeExpressionExecutors.length);
        }
        return null;
    }

    /**
     * The main execution method which will be called upon event arrival
     * when there are more than one function parameter.
     *
     * @param data the runtime values of function parameters.
     * @return the function result.
     */
    @Override
    protected Object execute(Object[] data, State state) {

        String key = (String) data[0];
        String defaultValue = (String) data[1];

        return System.getProperty(key, defaultValue);
    }

    /**
     * The main execution method which will be called upon event arrival
     * when there are zero or one function parameter.
     *
     * @param data null if the function parameter count is zero or
     *             runtime data value of the function parameter.
     * @return the function result.
     */
    @Override
    protected Object execute(Object data, State state) {
        if (data != null) {
            if (data instanceof String) {
                String inputString = (String) data;
                return System.getProperty(inputString);
            } else {
                throw new SiddhiAppRuntimeException("Input to the getEnvironmentProperty() function must be a String");
            }
        } else {
            throw new SiddhiAppRuntimeException("Input to the getEnvironmentProperty() function cannot be null");
        }
    }

    /**
     * This will be called only once and this can be used to acquire
     * required resources for the processing element.
     * This will be called after initializing the system and before
     * starting to process the events.
     */

    @Override
    public Attribute.Type getReturnType() {
        return Attribute.Type.STRING;
    }
}
