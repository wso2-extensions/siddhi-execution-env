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
import ua_parser.Client;
import ua_parser.Parser;

import java.io.IOException;
import java.util.Map;

/**
 * Siddhi Function getUserAgentPropertyFunction to extract properties from the user agent.
 */
@Extension(
        name = "getUserAgentProperty",
        namespace = "env",
        description = "This function returns the value corresponding to a given property name in a given user agent",
        returnAttributes = @ReturnAttribute(
                description = "Returned type will be string.",
                type = {DataType.STRING}),
        parameters = {
                @Parameter(name = "user.agent",
                        description = "This specifies the user agent from which property is extracted.",
                        type = {DataType.STRING},
                        optional = false),
                @Parameter(name = "property.name",
                        description = "This specifies property name which should be extracted. Currently " +
                                "supported properties are " + UserAgentConstants.BROWSER + ", "
                                + UserAgentConstants.OPERATING_SYSTEM + ", " + UserAgentConstants.DEVICE + ".",
                        type = {DataType.STRING},
                        optional = false)
        },
        examples = {
                @Example(
                        syntax = "define stream userAgentStream (userAgent string);\n" +
                                "from userAgentStream env:getUserAgentProperty(userAgent, \"browser\") as " +
                                "FunctionOutput \n" +
                                "insert into outputStream;",
                        description = "This query returns browser name of the userAgent from userAgentStream as " +
                                "FunctionOutput to the outputStream"
                )
        }
)
public class GetUserAgentPropertyFunction extends FunctionExecutor {

    /**
     * The initialization method for GetUserAgentPropertyFunction,
     * this method will be called before the other methods.
     *
     * @param attributeExpressionExecutors the executors of each function parameter.
     * @param siddhiAppContext             the context of the execution plan.
     */
    protected void init(ExpressionExecutor[] attributeExpressionExecutors, ConfigReader configReader, SiddhiAppContext
            siddhiAppContext) {

        int attributeExpressionExecutorsLength = attributeExpressionExecutors.length;
        if (attributeExpressionExecutorsLength == 2) {
            Attribute.Type typeofUserAgentAttribute = attributeExpressionExecutors[0].getReturnType();
            if (typeofUserAgentAttribute != Attribute.Type.STRING) {
                throw new SiddhiAppValidationException("Invalid parameter type found " +
                        "for the argument user.agent of getUserAgentProperty() function, " +
                        "required " + Attribute.Type.STRING +
                        ", but found " + typeofUserAgentAttribute.toString());
            }
            Attribute.Type typeofPropertyNameAttribute = attributeExpressionExecutors[1].getReturnType();
            if (typeofPropertyNameAttribute != Attribute.Type.STRING) {
                throw new SiddhiAppValidationException("Invalid parameter type found " +
                        "for the argument property.name of getUserAgentProperty() function, " +
                        "required " + Attribute.Type.STRING +
                        ", but found " + typeofPropertyNameAttribute.toString());
            }
        } else {
            throw new SiddhiAppValidationException("Invalid no of arguments passed to " +
                    "env:getUserAgentProperty(user.agent,property.name) function, " +
                    "required 2, but found " + attributeExpressionExecutors.length);
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
        String userAgent = (String) data[0];
        String propertyName = (String) data[1];
        String propertyValue;
        try {
            Parser parser = new Parser();
            Client client = parser.parse(userAgent);
            switch (propertyName) {
                case UserAgentConstants.BROWSER:
                    propertyValue = client.userAgent.family;
                    break;
                case UserAgentConstants.OPERATING_SYSTEM:
                    propertyValue = client.os.family;
                    break;
                case UserAgentConstants.DEVICE:
                    propertyValue = client.device.family;
                    break;
                default:
                    propertyValue = null;
                    break;
            }
        } catch (IOException e) {
            throw new SiddhiAppRuntimeException("This event is dropped to an exception occurred while processing the " +
                    "user agent.", e);
        }
        return propertyValue;
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
        throw new SiddhiAppRuntimeException("Number of parameters passed to getUserAgentProperty() function " +
                "is invalid");
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

    }

}
