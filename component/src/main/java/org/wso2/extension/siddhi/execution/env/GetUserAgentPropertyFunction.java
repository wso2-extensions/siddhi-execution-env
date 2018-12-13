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
import org.wso2.siddhi.annotation.SystemParameter;
import org.wso2.siddhi.annotation.util.DataType;
import org.wso2.siddhi.core.config.SiddhiAppContext;
import org.wso2.siddhi.core.exception.SiddhiAppCreationException;
import org.wso2.siddhi.core.exception.SiddhiAppRuntimeException;
import org.wso2.siddhi.core.executor.ConstantExpressionExecutor;
import org.wso2.siddhi.core.executor.ExpressionExecutor;
import org.wso2.siddhi.core.executor.function.FunctionExecutor;
import org.wso2.siddhi.core.util.config.ConfigReader;
import org.wso2.siddhi.query.api.definition.Attribute;
import org.wso2.siddhi.query.api.exception.SiddhiAppValidationException;
import ua_parser.Client;
import ua_parser.OS;
import ua_parser.Parser;
import ua_parser.UserAgent;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.Map;

/**
 * Siddhi Function getUserAgentPropertyFunction to extract properties from the user agent.
 */
@Extension(
        name = "getUserAgentProperty",
        namespace = "env",
        description = "This function returns the value that corresponds with a specified property name of a specified" +
                " user agent",
        returnAttributes = @ReturnAttribute(
                description = "The property to be extracted from the user agent.",
                type = {DataType.STRING}),
        parameters = {
                @Parameter(name = "user.agent",
                        description = "This specifies the user agent from which the property needs to be extracted.",
                        type = {DataType.STRING}),
                @Parameter(name = "property.name",
                        description = "This specifies the property name that needs to be extracted. " +
                                "Supported property names are 'browser', 'os', and 'device'.",
                        type = {DataType.STRING})
        },
        systemParameter = {
                @SystemParameter(
                        name = "regexFilePath",
                        description = "The location of the yaml file that contains the regex to process the user " +
                                "agent.",
                        defaultValue = "Default regexes included in the ua_parser library",
                        possibleParameters = "N/A"
                )
        },
        examples = {
                @Example(
                        syntax = "define stream UserAgentStream (userAgent string);\n" +
                                "from UserAgentStream \n" +
                                "select env:getUserAgentProperty(userAgent, \"browser\") as " +
                                "functionOutput \n" +
                                "insert into OutputStream;",
                        description = "This query returns the browser name of the 'userAgent' from the" +
                                " 'UserAgentStream' stream as 'functionOutput', and inserts it into the " +
                                "'OutputStream'stream."
                )
        }
)
public class GetUserAgentPropertyFunction extends FunctionExecutor {

    private static final String BROWSER = "browser";
    private static final String OPERATING_SYSTEM = "os";
    private static final String DEVICE = "device";
    private final List<String> listOfProperties = Arrays.asList(DEVICE, OPERATING_SYSTEM, BROWSER);
    private Parser uaParser;
    private String propertyName;

    @Override

    protected void init(ExpressionExecutor[] attributeExpressionExecutors, ConfigReader configReader, SiddhiAppContext
            siddhiAppContext) {

        int attributeExpressionExecutorsLength = attributeExpressionExecutors.length;
        if (attributeExpressionExecutorsLength == 2) {
            Attribute.Type typeofUserAgentAttribute = attributeExpressionExecutors[0].getReturnType();
            if (typeofUserAgentAttribute != Attribute.Type.STRING) {
                throw new SiddhiAppValidationException("Invalid parameter type found " +
                        "for the first argument 'user.agent' of getUserAgentProperty() function, " +
                        "required " + Attribute.Type.STRING +
                        ", but found '" + typeofUserAgentAttribute.toString() + "'.");
            }
            if (attributeExpressionExecutors[1] instanceof ConstantExpressionExecutor) {
                propertyName = ((ConstantExpressionExecutor) attributeExpressionExecutors[1])
                        .getValue().toString();

                if (!listOfProperties.contains(propertyName.toLowerCase(Locale.ENGLISH))) {
                    throw new SiddhiAppValidationException("Invalid parameter found " +
                            "for the second argument 'property.name' of getUserAgentProperty() function, " +
                            "required one of " + listOfProperties.toString() + ", but found " + propertyName);
                }
            } else {
                throw new SiddhiAppValidationException("Second parameter 'property.name' of getUserAgentProperty() " +
                        "should be a constant, but found a dynamic attribute.");
            }
        } else {
            throw new SiddhiAppValidationException("Invalid no of arguments passed to " +
                    "env:getUserAgentProperty(user.agent,property.name) function, " +
                    "required 2, but found " + attributeExpressionExecutors.length);
        }
        String regexFilePath = configReader.readConfig("regexFilePath", "");
        try {
            if (!regexFilePath.isEmpty()) {
                InputStream inputStream = new FileInputStream(regexFilePath);
                uaParser = new Parser(inputStream);
            } else {
                uaParser = new Parser();
            }
        } catch (FileNotFoundException e) {
            throw new SiddhiAppCreationException("Regexes file is not found in the given location '" + regexFilePath +
                    "', failed to initiate user agent parser.", e);
        } catch (IllegalArgumentException e) {
            throw new SiddhiAppCreationException("Invalid Regexes file found at " + regexFilePath + ", failed to " +
                    "initiate user agent parser.", e);
        } catch (IOException e) {
            throw new SiddhiAppCreationException("Failed to initiate user agent parser for the Siddhi app.", e);
        }

    }

    protected Object execute(Object[] data) {
        String userAgent = (String) data[0];
        switch (propertyName.toLowerCase(Locale.ENGLISH)) {
            case BROWSER:
                UserAgent agent = uaParser.parseUserAgent(userAgent);
                return agent.family;
            case OPERATING_SYSTEM:
                OS operatingSystem = uaParser.parseOS(userAgent);
                return operatingSystem.family;
            case DEVICE:
                Client clientParser = uaParser.parse(userAgent);
                return clientParser.device.family;
            // Default condition will never occur.
            default:
                return null;
        }
    }

    @Override
    protected Object execute(Object data) {
        // This function is never reached.
        throw new SiddhiAppRuntimeException("Number of parameters passed to getUserAgentProperty() function " +
                "is invalid");
    }

    @Override
    public Attribute.Type getReturnType() {
        return Attribute.Type.STRING;
    }

    @Override
    public Map<String, Object> currentState() {
        return null;
    }

    @Override
    public void restoreState(Map<String, Object> state) {

    }

}
