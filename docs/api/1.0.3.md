# API Docs - v1.0.3

## Env

### getEnvironmentProperty *<a target="_blank" href="https://wso2.github.io/siddhi/documentation/siddhi-4.0/#function">(Function)</a>*

<p style="word-wrap: break-word">This function returns Java environment property corresponding to the key provided</p>

<span id="syntax" class="md-typeset" style="display: block; font-weight: bold;">Syntax</span>
```
<STRING> env:getEnvironmentProperty(<STRING> key, <STRING> default.value)
```

<span id="query-parameters" class="md-typeset" style="display: block; color: rgba(0, 0, 0, 0.54); font-size: 12.8px; font-weight: bold;">QUERY PARAMETERS</span>
<table>
    <tr>
        <th>Name</th>
        <th style="min-width: 20em">Description</th>
        <th>Default Value</th>
        <th>Possible Data Types</th>
        <th>Optional</th>
        <th>Dynamic</th>
    </tr>
    <tr>
        <td style="vertical-align: top">key</td>
        <td style="vertical-align: top; word-wrap: break-word">This specifies Key of the property to be read.</td>
        <td style="vertical-align: top"></td>
        <td style="vertical-align: top">STRING</td>
        <td style="vertical-align: top">No</td>
        <td style="vertical-align: top">No</td>
    </tr>
    <tr>
        <td style="vertical-align: top">default.value</td>
        <td style="vertical-align: top; word-wrap: break-word">This specifies the default Value to be returned if the property value is not available.</td>
        <td style="vertical-align: top">null</td>
        <td style="vertical-align: top">STRING</td>
        <td style="vertical-align: top">Yes</td>
        <td style="vertical-align: top">No</td>
    </tr>
</table>

<span id="examples" class="md-typeset" style="display: block; font-weight: bold;">Examples</span>
<span id="example-1" class="md-typeset" style="display: block; color: rgba(0, 0, 0, 0.54); font-size: 12.8px; font-weight: bold;">EXAMPLE 1</span>
```
define stream keyStream (key string);
from keyStream env:getEnvironmentProperty(key) as FunctionOutput 
insert into outputStream;
```
<p style="word-wrap: break-word">This query returns Java environment property corresponding to the key from keyStream as FunctionOutput to the outputStream</p>

### getSystemProperty *<a target="_blank" href="https://wso2.github.io/siddhi/documentation/siddhi-4.0/#function">(Function)</a>*

<p style="word-wrap: break-word">This function returns the system property pointed by the system property key</p>

<span id="syntax" class="md-typeset" style="display: block; font-weight: bold;">Syntax</span>
```
<STRING> env:getSystemProperty(<STRING> key, <STRING> default.value)
```

<span id="query-parameters" class="md-typeset" style="display: block; color: rgba(0, 0, 0, 0.54); font-size: 12.8px; font-weight: bold;">QUERY PARAMETERS</span>
<table>
    <tr>
        <th>Name</th>
        <th style="min-width: 20em">Description</th>
        <th>Default Value</th>
        <th>Possible Data Types</th>
        <th>Optional</th>
        <th>Dynamic</th>
    </tr>
    <tr>
        <td style="vertical-align: top">key</td>
        <td style="vertical-align: top; word-wrap: break-word">This specifies Key of the property to be read.</td>
        <td style="vertical-align: top"></td>
        <td style="vertical-align: top">STRING</td>
        <td style="vertical-align: top">No</td>
        <td style="vertical-align: top">No</td>
    </tr>
    <tr>
        <td style="vertical-align: top">default.value</td>
        <td style="vertical-align: top; word-wrap: break-word">This specifies the default Value to be returned if the property value is not available.</td>
        <td style="vertical-align: top">null</td>
        <td style="vertical-align: top">STRING</td>
        <td style="vertical-align: top">Yes</td>
        <td style="vertical-align: top">No</td>
    </tr>
</table>

<span id="examples" class="md-typeset" style="display: block; font-weight: bold;">Examples</span>
<span id="example-1" class="md-typeset" style="display: block; color: rgba(0, 0, 0, 0.54); font-size: 12.8px; font-weight: bold;">EXAMPLE 1</span>
```
define stream keyStream (key string);
from keyStream env:getSystemProperty(key) as FunctionOutput 
insert into outputStream;
```
<p style="word-wrap: break-word">This query returns system property corresponding to the key from keyStream as FunctionOutput to the outputStream</p>

### getUserAgentProperty *<a target="_blank" href="https://wso2.github.io/siddhi/documentation/siddhi-4.0/#function">(Function)</a>*

<p style="word-wrap: break-word">This function returns the value corresponding to a given property name in a given user agent</p>

<span id="syntax" class="md-typeset" style="display: block; font-weight: bold;">Syntax</span>
```
<STRING> env:getUserAgentProperty(<STRING> user.agent, <STRING> property.name)
```

<span id="query-parameters" class="md-typeset" style="display: block; color: rgba(0, 0, 0, 0.54); font-size: 12.8px; font-weight: bold;">QUERY PARAMETERS</span>
<table>
    <tr>
        <th>Name</th>
        <th style="min-width: 20em">Description</th>
        <th>Default Value</th>
        <th>Possible Data Types</th>
        <th>Optional</th>
        <th>Dynamic</th>
    </tr>
    <tr>
        <td style="vertical-align: top">user.agent</td>
        <td style="vertical-align: top; word-wrap: break-word">This specifies the user agent from which property will be extracted.</td>
        <td style="vertical-align: top"></td>
        <td style="vertical-align: top">STRING</td>
        <td style="vertical-align: top">No</td>
        <td style="vertical-align: top">No</td>
    </tr>
    <tr>
        <td style="vertical-align: top">property.name</td>
        <td style="vertical-align: top; word-wrap: break-word">This specifies property name which should be extracted. Supported property names are <code>'browser'</code>, <code>'os'</code> and <code>'device'</code>.</td>
        <td style="vertical-align: top"></td>
        <td style="vertical-align: top">STRING</td>
        <td style="vertical-align: top">No</td>
        <td style="vertical-align: top">No</td>
    </tr>
</table>

<span id="system-parameters" class="md-typeset" style="display: block; font-weight: bold;">System Parameters</span>
<table>
    <tr>
        <th>Name</th>
        <th style="min-width: 20em">Description</th>
        <th>Default Value</th>
        <th>Possible Parameters</th>
    </tr>
    <tr>
        <td style="vertical-align: top">regexFilePath</td>
        <td style="vertical-align: top; word-wrap: break-word">Location of the yaml file which contains the regex to process the user agent. </td>
        <td style="vertical-align: top">Default regexes included in the ua_parser library</td>
        <td style="vertical-align: top">N/A</td>
    </tr>
</table>

<span id="examples" class="md-typeset" style="display: block; font-weight: bold;">Examples</span>
<span id="example-1" class="md-typeset" style="display: block; color: rgba(0, 0, 0, 0.54); font-size: 12.8px; font-weight: bold;">EXAMPLE 1</span>
```
define stream UserAgentStream (userAgent string);
from UserAgentStream 
select env:getUserAgentProperty(userAgent, "browser") as functionOutput 
insert into OutputStream;
```
<p style="word-wrap: break-word">This query returns browser name of the userAgent from UserAgentStream as <code>functionOutput</code> to the OutputStream</p>

### getYAMLProperty *<a target="_blank" href="https://wso2.github.io/siddhi/documentation/siddhi-4.0/#function">(Function)</a>*

<p style="word-wrap: break-word">This function returns the YAML property requested or the default values specified if such avariable is not available in the deployment.yaml</p>

<span id="syntax" class="md-typeset" style="display: block; font-weight: bold;">Syntax</span>
```
<INT|LONG|DOUBLE|FLOAT|STRING|BOOL> env:getYAMLProperty(<STRING> key, <STRING> data.type, <INT|LONG|DOUBLE|FLOAT|STRING|BOOL> default.value)
```

<span id="query-parameters" class="md-typeset" style="display: block; color: rgba(0, 0, 0, 0.54); font-size: 12.8px; font-weight: bold;">QUERY PARAMETERS</span>
<table>
    <tr>
        <th>Name</th>
        <th style="min-width: 20em">Description</th>
        <th>Default Value</th>
        <th>Possible Data Types</th>
        <th>Optional</th>
        <th>Dynamic</th>
    </tr>
    <tr>
        <td style="vertical-align: top">key</td>
        <td style="vertical-align: top; word-wrap: break-word">This specifies Key of the property to be read.</td>
        <td style="vertical-align: top"></td>
        <td style="vertical-align: top">STRING</td>
        <td style="vertical-align: top">No</td>
        <td style="vertical-align: top">No</td>
    </tr>
    <tr>
        <td style="vertical-align: top">data.type</td>
        <td style="vertical-align: top; word-wrap: break-word">A string constant parameter expressing the data type of the propertyusing one of the following string values: int, long, float, double, string, bool.</td>
        <td style="vertical-align: top">string</td>
        <td style="vertical-align: top">STRING</td>
        <td style="vertical-align: top">No</td>
        <td style="vertical-align: top">No</td>
    </tr>
    <tr>
        <td style="vertical-align: top">default.value</td>
        <td style="vertical-align: top; word-wrap: break-word">This specifies the default Value to be returned if the property value is not available.</td>
        <td style="vertical-align: top">null</td>
        <td style="vertical-align: top">INT<br>LONG<br>DOUBLE<br>FLOAT<br>STRING<br>BOOL</td>
        <td style="vertical-align: top">Yes</td>
        <td style="vertical-align: top">No</td>
    </tr>
</table>

<span id="examples" class="md-typeset" style="display: block; font-weight: bold;">Examples</span>
<span id="example-1" class="md-typeset" style="display: block; color: rgba(0, 0, 0, 0.54); font-size: 12.8px; font-weight: bold;">EXAMPLE 1</span>
```
define stream keyStream (key string);
from keyStream  env:getYAMLProperty(key) as FunctionOutput 
insert into outputStream;
```
<p style="word-wrap: break-word">This query returns corresponding YAML property for the corresponding key from keyStream as FunctionOutput to the outputStream</p>

