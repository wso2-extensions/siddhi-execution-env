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
import io.siddhi.core.executor.ExpressionExecutor;
import io.siddhi.core.executor.function.FunctionExecutor;
import io.siddhi.core.util.config.ConfigReader;
import io.siddhi.core.util.snapshot.state.State;
import io.siddhi.core.util.snapshot.state.StateFactory;
import io.siddhi.query.api.definition.Attribute;
import io.siddhi.query.api.exception.SiddhiAppValidationException;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * This class provides a function to get immediate public IP of the origin from X-Forwarded-For header.
 */
@Extension(
        name = "getOriginIPFromXForwarded",
        namespace = "env",
        description = "This function returns the public origin IP from the given X-Forwarded header.",
        returnAttributes = @ReturnAttribute(
                description = "The public IP related to the origin that is retrieved using the given 'X-Forwarded' " +
                        "header.",
                type = {DataType.STRING}),
        parameters = {
                @Parameter(name = "xforwardedheader",
                        description = "The 'X-Forwarded-For' header of the request.",
                        type = {DataType.STRING}
                )
        },
        examples = {
                @Example(
                        syntax = "define stream InputStream (xForwardedHeader string);\n" +
                                "from InputStream " +
                                "select env:getOriginIPFromXForwarded(xForwardedHeader) as originIP \n" +
                                "insert into OutputStream;",
                        description = "This query returns the public origin IP from the given X-Forwarded header as" +
                                " 'originIP', and inserts it to the 'OutputStream' stream."
                )
        }
)
public class GetOriginIPFromXForwardedFunction extends FunctionExecutor {

    private static final String IP_ADDRESS_PATTERN = "^([01]?\\d\\d?|2[0-4]\\d|25[0-5])\\." +
            "([01]?\\d\\d?|2[0-4]\\d|25[0-5])\\." +
            "([01]?\\d\\d?|2[0-4]\\d|25[0-5])\\." +
            "([01]?\\d\\d?|2[0-4]\\d|25[0-5])$";
    private static final String PRIVATE_IP_ADDRESS_PATTERN = "(^127\\..*)|(^10\\..*)|(^172\\.1[6-9]\\..*)|" +
            "(^172\\.2[0-9]\\..*)|(^172\\.3[0-1]\\..*)|(^192\\.168\\..*)";
    private static Pattern ipPAddressPattern = Pattern.compile(IP_ADDRESS_PATTERN);
    private static Pattern privateIPPAddressPattern = Pattern.compile(PRIVATE_IP_ADDRESS_PATTERN);

    /**
     * The initialization method for GetOriginIPFromXForwardedFunction,
     * this method will be called before the other methods.
     *
     * @param attributeExpressionExecutors the executors of each function parameter.
     * @param siddhiQueryContext           the context of the siddhi query.
     */
    protected StateFactory init(ExpressionExecutor[] attributeExpressionExecutors, ConfigReader reader,
                                SiddhiQueryContext siddhiQueryContext) {
        if (attributeExpressionExecutors.length != 1) {
            throw new SiddhiAppValidationException("Invalid no of arguments passed to env:getOriginIPFromXForwarded" +
                    "() function, required 1, but found " + attributeExpressionExecutors.length);
        }

        Attribute.Type attributeType = attributeExpressionExecutors[0].getReturnType();
        if (attributeType != Attribute.Type.STRING) {
            throw new SiddhiAppValidationException("Invalid parameter type found for first argument " +
                    "'xForwardedHeader' of env:getOriginIPFromXForwarded() function, required " + Attribute.Type
                    .STRING + ", but found " + attributeType.toString());
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
        //we only allow single parameter for this function
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
    protected Object execute(Object data, State state) {
        String xForwardedFor = data.toString();
        if (xForwardedFor.isEmpty()) {
            return null;
        }
        String extractedIPs[] = xForwardedFor.split(",", -1);
        Matcher privateIPAddressMatcher;
        Matcher ipAddressMatcher;
        String filteredPublicIp = null;
        String extractedIP;
        for (int i = 0, extractedIPsLength = extractedIPs.length; i < extractedIPsLength; i++) {
            extractedIP = extractedIPs[i].trim();
            privateIPAddressMatcher = privateIPPAddressPattern.matcher(extractedIP);
            if (!privateIPAddressMatcher.matches()) {
                ipAddressMatcher = ipPAddressPattern.matcher(extractedIP);
                if (ipAddressMatcher.matches()) {
                    filteredPublicIp = extractedIP;
                    break;
                }

            }
        }
        return filteredPublicIp;
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
