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
import org.wso2.siddhi.annotation.ReturnAttribute;
import org.wso2.siddhi.annotation.util.DataType;
import org.wso2.siddhi.core.config.SiddhiAppContext;
import org.wso2.siddhi.core.exception.SiddhiAppRuntimeException;
import org.wso2.siddhi.core.executor.ConstantExpressionExecutor;
import org.wso2.siddhi.core.executor.ExpressionExecutor;
import org.wso2.siddhi.core.executor.function.FunctionExecutor;
import org.wso2.siddhi.core.util.config.ConfigReader;
import org.wso2.siddhi.query.api.definition.Attribute;
import org.wso2.siddhi.query.api.exception.SiddhiAppValidationException;

import java.util.Map;

/**
 * Siddhi Function getYAMLProperty to read property values from deployment YAML.
 */

@Extension(
        name = "getYAMLProperty",
        namespace = "env",
        description = "This function returns the YAML Property requested or the default values specified if such a" +
                "variable is not available",
        parameters = {
                @Parameter(name = "key",
                        description = "This specifies Key of the property to be read.",
                        type = {DataType.STRING}),
                @Parameter(name = "data.type",
                        description = "A string constant parameter expressing the cast to type using one of the " +
                                "following strings values: int, long, float, double, string, bool.",
                        type = {DataType.STRING}),
                @Parameter(name = "default.value",
                        description = "This specifies the default Value to be returned " +
                                "if the property value is not available.",
                        type = {DataType.INT, DataType.LONG, DataType.DOUBLE, DataType.FLOAT,
                                DataType.STRING, DataType.BOOL})
        },
        returnAttributes = @ReturnAttribute(
                description = "Returned type will be default to string, but it could be any of the following: " +
                        "int, long, float, double, string, bool.",
                type = {org.wso2.siddhi.annotation.util.DataType.INT, org.wso2.siddhi.annotation.util.DataType.LONG,
                        org.wso2.siddhi.annotation.util.DataType.DOUBLE, org.wso2.siddhi.annotation.util.DataType.FLOAT,
                        org.wso2.siddhi.annotation.util.DataType.STRING, org.wso2.siddhi.annotation.util.DataType.BOOL
                }),
        examples = {
                @Example(
                        syntax = "define stream inputStream (symbol string, price long, volume long);\n" +
                                "from inputStream select symbol , env:getYAMLProperty() as FunctionOutput \n" +
                                "insert into outputStream;",
                        description = "This query returns symbol from inputStream and"
                                + "TheFun function output as "
                                + " FunctionOutput to the outputStream"
                )
        }
)

public class GetYAMLProperty extends FunctionExecutor {

    private ConfigReader reader;
    private Attribute.Type returnType = Attribute.Type.STRING;


    /**
     * The initialization method for TheFun, this method will be called before the other methods.
     *
     * @param attributeExpressionExecutors the executors of each function parameter.
     * @param siddhiAppContext             the context of the execution plan.
     */

    protected void init(ExpressionExecutor[] attributeExpressionExecutors, ConfigReader reader,
                        SiddhiAppContext siddhiAppContext) {

        if (attributeExpressionExecutors.length < 1) {
            throw new SiddhiAppValidationException(
                    "Invalid no of arguments passed to env:getYAMLProperty() function, " +
                            "required at least 1, but found " + attributeExpressionExecutors.length);
        }
        Attribute.Type attribute1Type = attributeExpressionExecutors[0].getReturnType();
        if (!((attribute1Type == Attribute.Type.STRING))) {
            throw new SiddhiAppValidationException("Invalid parameter type found " +
                    "for the argument Key of getYAMLProperty() function, " +
                    "required " + Attribute.Type.STRING +
                    ", but found " + attribute1Type.toString());
        }

        if (attributeExpressionExecutors.length > 1) {
            if (!(attributeExpressionExecutors[1] instanceof ConstantExpressionExecutor)) {
                throw new SiddhiAppValidationException("The second argument has to be a string constant specifying " +
                        "one of the supported data types "
                        + "(int, long, float, double, string, bool)");
            } else {
                String type = attributeExpressionExecutors[1].execute(null).toString();
                if ("int".equals(type)) {
                    returnType = Attribute.Type.INT;
                } else if ("long".equalsIgnoreCase(type)) {
                    returnType = Attribute.Type.LONG;
                } else if ("float".equalsIgnoreCase(type)) {
                    returnType = Attribute.Type.FLOAT;
                } else if ("double".equalsIgnoreCase(type)) {
                    returnType = Attribute.Type.DOUBLE;
                } else if ("bool".equalsIgnoreCase(type)) {
                    returnType = Attribute.Type.BOOL;
                } else if ("string".equalsIgnoreCase(type)) {
                    returnType = Attribute.Type.STRING;
                } else {
                    throw new SiddhiAppValidationException("Type must be one of int, long, float, double, bool, " +
                            "string");
                }
            }
        }

        if (attributeExpressionExecutors.length > 2) {

            Attribute.Type attribute3Type = attributeExpressionExecutors[2].getReturnType();

            if (!(attribute3Type == returnType)) {
                throw new SiddhiAppValidationException("Type of parameter default.Value " +
                        "needs to match parameter data.type");
            }
        }

        this.reader = reader;
    }

    /**
     * The main execution method which will be called upon event arrival
     * when there are more than one function parameter.
     *
     * @param data the runtime values of function parameters.
     * @return the function result.
     */
    @Override
    protected Object execute(Object[] data) {

        if (data.length == 2) {
            if (data[0] instanceof String) {
                String key = (String) data[0];

                String value = reader.readConfig(key, null);

                if (value != null) {

                    try {

                        switch (returnType) {
                            case INT:
                                int intValue = Integer.parseInt(value);
                                return intValue;
                            case LONG:
                                long longValue = Long.parseLong(value);
                                return longValue;
                            case FLOAT:
                                float floatValue = Float.parseFloat(value);
                                return floatValue;
                            case DOUBLE:
                                double doubleValue = Double.parseDouble(value);
                                return doubleValue;
                            case BOOL:
                                boolean boolValue = Boolean.parseBoolean(value);
                                return boolValue;
                            case STRING:
                                break;
                        }
                        return value;
                    } catch (ClassCastException e) {
                        throw new SiddhiAppRuntimeException
                                ("The type of property value and parameter dataType does not match");
                    }

                } else {
                    return value;
                }

            } else {
                throw new SiddhiAppRuntimeException
                        ("The value of parameter Key to the getYAMLProperty function must be String");
            }
        } else if (data.length == 3) {

            if (data[0] instanceof String) {
                String key = (String) data[0];

                String value = reader.readConfig(key, null);

                if (value != null) {

                    try {

                        switch (returnType) {
                            case INT:
                                int intValue = Integer.parseInt(value);
                                return intValue;
                            case LONG:
                                long longValue = Long.parseLong(value);
                                return longValue;
                            case FLOAT:
                                float floatValue = Float.parseFloat(value);
                                return floatValue;
                            case DOUBLE:
                                double doubleValue = Double.parseDouble(value);
                                return doubleValue;
                            case BOOL:
                                boolean boolValue = Boolean.parseBoolean(value);
                                return boolValue;
                            case STRING:
                                break;
                        }
                        return value;
                    } catch (NumberFormatException e) {
                        throw new SiddhiAppRuntimeException
                                ("The type of property value and parameter dataType does not match");
                    }

                } else {

                    switch (returnType) {
                        case STRING:
                            if (data[2] instanceof String) {
                                return data[2];
                            } else {
                                throw new SiddhiAppRuntimeException
                                        ("The type of default value and parameter dataType does not match");
                            }
                        case BOOL:
                            if (data[2] instanceof Boolean) {
                                return data[2];
                            } else {
                                throw new SiddhiAppRuntimeException
                                        ("The type of default value and parameter dataType does not match");

                            }
                        case LONG:
                            if (data[2] instanceof Long) {
                                return data[2];
                            } else {
                                throw new SiddhiAppRuntimeException
                                        ("The type of default value and parameter dataType does not match");

                            }
                        case INT:
                            if (data[2] instanceof Integer) {
                                return data[2];
                            } else {
                                throw new SiddhiAppRuntimeException
                                        ("The type of default value and parameter dataType does not match");

                            }
                        case DOUBLE:
                            if (data[2] instanceof Double) {
                                return data[2];
                            } else {
                                throw new SiddhiAppRuntimeException
                                        ("The type of default value and parameter dataType does not match");

                            }
                        case FLOAT:
                            if (data[2] instanceof Float) {
                                return data[2];
                            } else {
                                throw new SiddhiAppRuntimeException
                                        ("The type of default value and parameter dataType does not match");

                            }
                    }
                    return null;
                }

            } else {
                throw new SiddhiAppRuntimeException
                        ("The value of parameter Key to the getYAMLProperty function must be String");
            }
        } else {
            return null;
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
    protected Object execute(Object data) {

        if (data != null) {

            if (data instanceof String) {
                String key = (String) data;

                String value = reader.readConfig(key, null);

                return value;
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

    /**
     * Used to collect the serializable state of the processing element, that need to be
     * persisted for the reconstructing the element to the same state on a different point of time.
     *
     * @return stateful objects of the processing element as an array.
     */
    @Override
    public Map<String, Object> currentState() {
        return null;
    }

    /**
     * Used to restore serialized state of the processing element, for reconstructing
     * the element to the same state as if was on a previous point of time.
     *
     * @param state the stateful objects of the element as an array on
     *              the same order provided by currentState().
     */
    @Override
    public void restoreState(Map<String, Object> state) {
        //Implement restore state logic.
    }

}
