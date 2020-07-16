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
import io.siddhi.core.executor.ConstantExpressionExecutor;
import io.siddhi.core.executor.ExpressionExecutor;
import io.siddhi.core.executor.function.FunctionExecutor;
import io.siddhi.core.util.config.ConfigReader;
import io.siddhi.core.util.snapshot.state.State;
import io.siddhi.core.util.snapshot.state.StateFactory;
import io.siddhi.query.api.definition.Attribute;
import io.siddhi.query.api.exception.SiddhiAppValidationException;

/**
 * Siddhi Function getYAMLProperty to read property values from deployment.yaml.
 */

@Extension(
        name = "getYAMLProperty",
        namespace = "env",
        description = "This function returns the YAML property requested or the default values specified if such a" +
                "variable is not specified in the 'deployment.yaml'.",
        parameters = {
                @Parameter(name = "key",
                        description = "This specifies key of the property to be read.",
                        type = {DataType.STRING},
                        optional = false),
                @Parameter(name = "data.type",
                        description = "A string constant parameter expressing the data type of the property" +
                                "using one of the following string values:\n int, long, float, double, string, bool.",
                        type = {DataType.STRING},
                        optional = false,
                        defaultValue = "string"),
                @Parameter(name = "default.value",
                        description = "This specifies the default value to be returned " +
                                "if the property value is not available.",
                        type = {DataType.INT, DataType.LONG, DataType.DOUBLE, DataType.FLOAT,
                                DataType.STRING, DataType.BOOL},
                        optional = true,
                        defaultValue = "null")
        },
        returnAttributes = @ReturnAttribute(
                description = "The default return type is 'string', but it can also be any of the following:\n " +
                        "'int', 'long', 'float', 'double', 'string',' bool'.",
                type = {io.siddhi.annotation.util.DataType.INT, io.siddhi.annotation.util.DataType.LONG,
                        io.siddhi.annotation.util.DataType.DOUBLE, io.siddhi.annotation.util.DataType.FLOAT,
                        io.siddhi.annotation.util.DataType.STRING, io.siddhi.annotation.util.DataType.BOOL
                }),
        examples = {
                @Example(
                        syntax = "define stream KeyStream (key string);\n" +
                                "from KeyStream \n" +
                                "select env:getYAMLProperty(key) as FunctionOutput \n" +
                                "insert into outputStream;",
                        description = "This query returns the corresponding YAML property for the corresponding key " +
                                "from the 'KeyStream' stream as 'FunctionOutput', and inserts it into the to the" +
                                " 'OutputStream' stream."
                )
        }
)

public class GetYAMLPropertyFunction extends FunctionExecutor {

    private ConfigReader configReader;
    private Attribute.Type returnType = Attribute.Type.STRING;
    private boolean hasDefaultValue = false;

    /**
     * The initialization method for TheFun, this method will be called before the other methods.
     *
     * @param attributeExpressionExecutors the executors of each function parameter.
     * @param siddhiQueryContext           the context of the siddhi query.
     */
    protected StateFactory init(ExpressionExecutor[] attributeExpressionExecutors, ConfigReader reader,
                                SiddhiQueryContext siddhiQueryContext) {
        int attributeExpressionExecutorsLength = attributeExpressionExecutors.length;
        if ((attributeExpressionExecutorsLength > 0) && (attributeExpressionExecutorsLength < 4)) {
            Attribute.Type typeofKeyAttribute = attributeExpressionExecutors[0].getReturnType();
            checkKeyAttribute(typeofKeyAttribute);
            if (attributeExpressionExecutorsLength > 1) {
                if (!(attributeExpressionExecutors[1] instanceof ConstantExpressionExecutor)) {
                    throw new SiddhiAppValidationException("The second argument has to be a string constant " +
                            "specifying one of the supported data types (int, long, float, double, string, bool)");
                } else {
                    String type = attributeExpressionExecutors[1].execute(null).toString();
                    returnType = getReturnType(type);
                }
            }
            if (attributeExpressionExecutors.length > 2) {
                Attribute.Type typeofDefaultValueAttribute = attributeExpressionExecutors[2].getReturnType();
                if (typeofDefaultValueAttribute != returnType) {
                    throw new SiddhiAppValidationException("Type of parameter default.Value " +
                            "needs to match parameter data.type");
                }
            }
        } else {
            throw new SiddhiAppValidationException("Invalid no of arguments passed to " +
                    "env:getYAMLProperty(Key, data.type, default.value) function, " +
                    "required 1,2 or 3, but found " + attributeExpressionExecutorsLength);
        }

        this.configReader = reader;
        hasDefaultValue = (attributeExpressionExecutorsLength > 2);
        return null;
    }

    private Attribute.Type getReturnType(String type) {
        Attribute.Type theReturnType;
        if ("int".equalsIgnoreCase(type)) {
            theReturnType = Attribute.Type.INT;
        } else if ("long".equalsIgnoreCase(type)) {
            theReturnType = Attribute.Type.LONG;
        } else if ("float".equalsIgnoreCase(type)) {
            theReturnType = Attribute.Type.FLOAT;
        } else if ("double".equalsIgnoreCase(type)) {
            theReturnType = Attribute.Type.DOUBLE;
        } else if ("bool".equalsIgnoreCase(type)) {
            theReturnType = Attribute.Type.BOOL;
        } else if ("string".equalsIgnoreCase(type)) {
            theReturnType = Attribute.Type.STRING;
        } else {
            throw new SiddhiAppValidationException("Type must be one of int, long, float, double, bool, " +
                    "string");
        }
        return theReturnType;
    }

    private void checkKeyAttribute(Attribute.Type typeofKeyAttribute) {
        if (typeofKeyAttribute != Attribute.Type.STRING) {
            throw new SiddhiAppValidationException("Invalid parameter type found " +
                    "for the argument Key of getYAMLProperty() function, " +
                    "required " + Attribute.Type.STRING +
                    ", but found " + typeofKeyAttribute.toString());
        }
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
        String value = configReader.readConfig(key, null);
        if (value != null) {
            try {
                switch (returnType) {
                    case INT:
                        return Integer.parseInt(value);
                    case LONG:
                        return Long.parseLong(value);
                    case FLOAT:
                        return Float.parseFloat(value);
                    case DOUBLE:
                        return Double.parseDouble(value);
                    case BOOL:
                        return Boolean.parseBoolean(value);
                    default:  // case STRING:
                        break;
                }
                return value;
            } catch (NumberFormatException e) {
                throw new SiddhiAppRuntimeException
                        ("The type of property value and parameter dataType does not match", e);
            }
        } else {
            return ((hasDefaultValue) ? data[2] : null);
        }
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
                String key = (String) data;
                return configReader.readConfig(key, null);
            }
        } else {
            throw new SiddhiAppRuntimeException("Input to the getYAMLProperty function cannot be null");
        }
        return null;
    }

    /**
     * This will be called only once and this can be used to acquire
     * required resources for the processing element.
     * This will be called after initializing the system and before
     * starting to process the events.
     */

    @Override
    public Attribute.Type getReturnType() {
        return returnType;
    }
}
