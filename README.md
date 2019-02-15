Siddhi Execution Extension - Env
======================================

The **siddhi-execution-env extension** is an extension to <a target="_blank" href="https://wso2.github.io/siddhi">Siddhi</a> that provides the capability to read environment properties inside Siddhi stream definitions and use it inside queries. Functions of the env extension are as follows..

Find some useful links below:

* <a target="_blank" href="https://github.com/wso2-extensions/siddhi-execution-env">Source code</a>
* <a target="_blank" href="https://github.com/wso2-extensions/siddhi-execution-env/releases">Releases</a>
* <a target="_blank" href="https://github.com/wso2-extensions/siddhi-execution-env/issues">Issue tracker</a>

## Latest API Docs 

Latest API Docs is <a target="_blank" href="https://wso2-extensions.github.io/siddhi-execution-env/api/1.1.0">1.1.0</a>.

## How to use 

**Using the extension in <a target="_blank" href="https://github.com/wso2/product-sp">WSO2 Stream Processor</a>**

* You can use this extension in the latest <a target="_blank" href="https://github.com/wso2/product-sp/releases">WSO2 Stream Processor</a> that is a part of <a target="_blank" href="http://wso2.com/analytics?utm_source=gitanalytics&utm_campaign=gitanalytics_Jul17">WSO2 Analytics</a> offering, with editor, debugger and simulation support. 

* This extension is shipped by default with WSO2 Stream Processor, if you wish to use an alternative version of this extension you can replace the component <a target="_blank" href="https://github.com/wso2-extensions/siddhi-execution-env/releases">jar</a> that can be found in the `<STREAM_PROCESSOR_HOME>/lib` 
directory.

**Using the extension as a <a target="_blank" href="https://wso2.github.io/siddhi/documentation/running-as-a-java-library">java library</a>**

* This extension can be added as a maven dependency along with other Siddhi dependencies to your project.

```
     <dependency>
        <groupId>org.wso2.extension.siddhi.execution.env</groupId>
        <artifactId>siddhi-execution-env</artifactId>
        <version>x.x.x</version>
     </dependency>
```

## Jenkins Build Status

---

|  Branch | Build Status |
| :------ |:------------ | 
| master  | [![Build Status](https://wso2.org/jenkins/view/All%20Builds/job/siddhi/job/siddhi-execution-env/badge/icon)](https://wso2.org/jenkins/view/All%20Builds/job/siddhi/job/siddhi-execution-env/) |

---

## Features

* <a target="_blank" href="https://wso2-extensions.github.io/siddhi-execution-env/api/1.1.0/#getenvironmentproperty-function">getEnvironmentProperty</a> *<a target="_blank" href="https://wso2.github.io/siddhi/documentation/siddhi-4.0/#function">(Function)</a>*<br><div style="padding-left: 1em;"><p>This function returns the Java environment property that corresponds with the key provided</p></div>
* <a target="_blank" href="https://wso2-extensions.github.io/siddhi-execution-env/api/1.1.0/#getoriginipfromxforwarded-function">getOriginIPFromXForwarded</a> *<a target="_blank" href="https://wso2.github.io/siddhi/documentation/siddhi-4.0/#function">(Function)</a>*<br><div style="padding-left: 1em;"><p>This function returns the public origin IP from the given X-Forwarded header.</p></div>
* <a target="_blank" href="https://wso2-extensions.github.io/siddhi-execution-env/api/1.1.0/#getsystemproperty-function">getSystemProperty</a> *<a target="_blank" href="https://wso2.github.io/siddhi/documentation/siddhi-4.0/#function">(Function)</a>*<br><div style="padding-left: 1em;"><p>This function returns the system property referred to via the system property key.</p></div>
* <a target="_blank" href="https://wso2-extensions.github.io/siddhi-execution-env/api/1.1.0/#getuseragentproperty-function">getUserAgentProperty</a> *<a target="_blank" href="https://wso2.github.io/siddhi/documentation/siddhi-4.0/#function">(Function)</a>*<br><div style="padding-left: 1em;"><p>This function returns the value that corresponds with a specified property name of a specified user agent</p></div>
* <a target="_blank" href="https://wso2-extensions.github.io/siddhi-execution-env/api/1.1.0/#getyamlproperty-function">getYAMLProperty</a> *<a target="_blank" href="https://wso2.github.io/siddhi/documentation/siddhi-4.0/#function">(Function)</a>*<br><div style="padding-left: 1em;"><p>This function returns the YAML property requested or the default values specified if such avariable is not specified in the 'deployment.yaml'.</p></div>
* <a target="_blank" href="https://wso2-extensions.github.io/siddhi-execution-env/api/1.1.0/#resourceidentifier-stream-processor">resourceIdentifier</a> *<a target="_blank" href="https://wso2.github.io/siddhi/documentation/siddhi-4.0/#stream-processor">(Stream Processor)</a>*<br><div style="padding-left: 1em;"><p>The resource identifier stream processor is an extension to register a resource name with a reference in a static map and serve a static resources count for a specific resource name.</p></div>
* <a target="_blank" href="https://wso2-extensions.github.io/siddhi-execution-env/api/1.1.0/#resourcebatch-window">resourceBatch</a> *<a target="_blank" href="https://wso2.github.io/siddhi/documentation/siddhi-4.0/#window">(Window)</a>*<br><div style="padding-left: 1em;"><p>This extension is a resource batch (tumbling) window that holds a number of events based on the resource count inferred from the 'env:resourceIdentifier' extension, and with a specific attribute as the grouping key. The window is updated each time a batch of events arrive with a matching value for the grouping key, and where the number of events is equal to the resource count.</p></div>

## How to Contribute
 
  * Please report issues at <a target="_blank" href="https://github.com/wso2-extensions/siddhi-execution-env/issues">GitHub Issue Tracker</a>.
  
  * Send your contributions as pull requests to <a target="_blank" href="https://github.com/wso2-extensions/siddhi-execution-env/tree/master">master branch</a>. 
 
## Contact us 

 * Post your questions with the <a target="_blank" href="http://stackoverflow.com/search?q=siddhi">"Siddhi"</a> tag in <a target="_blank" href="http://stackoverflow.com/search?q=siddhi">Stackoverflow</a>. 
 
 * Siddhi developers can be contacted via the mailing lists:
 
    Developers List   : [dev@wso2.org](mailto:dev@wso2.org)
    
    Architecture List : [architecture@wso2.org](mailto:architecture@wso2.org)
 
## Support 

* We are committed to ensuring support for this extension in production. Our unique approach ensures that all support leverages our open development methodology and is provided by the very same engineers who build the technology. 

* For more details and to take advantage of this unique opportunity contact us via <a target="_blank" href="http://wso2.com/support?utm_source=gitanalytics&utm_campaign=gitanalytics_Jul17">http://wso2.com/support/</a>.
