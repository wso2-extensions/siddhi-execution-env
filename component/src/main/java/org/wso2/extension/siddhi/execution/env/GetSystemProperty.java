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
import org.wso2.siddhi.core.executor.ExpressionExecutor;
import org.wso2.siddhi.core.executor.function.FunctionExecutor;
import org.wso2.siddhi.core.util.config.ConfigReader;
import org.wso2.siddhi.query.api.definition.Attribute;
import org.wso2.siddhi.query.api.exception.SiddhiAppValidationException;

import java.util.Map;


/**
 * Siddhi Function getSystemProperty to read Operating system Properties.
 */

@Extension(
        name = "getSystemProperty",
        namespace = "env",
        description = "This function returns system property given the system property key",
        returnAttributes = @ReturnAttribute(
                description = "Returned type will be string.",
                type = {org.wso2.siddhi.annotation.util.DataType.STRING}),
        parameters = {
                @Parameter(name = "key",
                        description = "This specifies Key of the property to be read.",
                        type = {DataType.STRING}),
                @Parameter(name = "default.value",
                        description = "This specifies the default Value to be returned " +
                                "if the property value is not available.",
                        type = {DataType.STRING})
        },
        examples = {
                @Example(
                        syntax = "define stream inputStream (symbol string, price long, volume long);\n" +
                                "from inputStream select symbol , env:getSystemProperty() as FunctionOutput \n" +
                                "insert into outputStream;",
                        description = "This query returns symbol from inputStream and"
                                + "TheFun function output as "
                                + " FunctionOutput to the outputStream"
                )
        }
)

public class GetSystemProperty extends FunctionExecutor {


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
                    "Invalid no of arguments passed to env:getSystemProperty() function, " +
                            "required at least 1, but found " + attributeExpressionExecutors.length);
        }
        Attribute.Type attributeType = attributeExpressionExecutors[0].getReturnType();
        if (!((attributeType == Attribute.Type.STRING))) {
            throw new SiddhiAppValidationException("Invalid parameter type found " +
                    "for the argument key of getSystemProperty() function, " +
                    "required " + Attribute.Type.STRING +
                    ", but found " + attributeType.toString());
        }

        if (attributeExpressionExecutors.length == 2) {
            Attribute.Type attribute2Type = attributeExpressionExecutors[1].getReturnType();
            if (!((attribute2Type == Attribute.Type.STRING))) {
                throw new SiddhiAppValidationException("Invalid parameter type found " +
                        "for the argument default.value of getSystemProperty() function, " +
                        "required " + Attribute.Type.STRING +
                        ", but found " + attributeType.toString());
            }
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
    protected Object execute(Object[] data) {

        if (data.length > 1) {
            if (data[0] instanceof String) {
                String key = (String) data[0];
                String defaultValue = (String) data[1];

                String returnValue = System.getenv(key);

                if (returnValue != null) {
                    return returnValue;
                } else {
                    return defaultValue;
                }

            } else {
                throw new SiddhiAppRuntimeException("Input to the getSystemProperty() function must be a String");
            }
        }

        return null;
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
                return System.getenv(key);
            } else {
                throw new SiddhiAppRuntimeException("Input to the getSystemProperty() function must be a String");
            }
        } else {
            throw new SiddhiAppRuntimeException("Input to the getSystemProperty() function cannot be null");
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
