/**
 * @file    IoTHubDeviceManager.java
 * @brief   MS IoTHub Device Manager for the MS IoTHub Peer Processor
 * @author  Doug Anson
 * @version 1.0
 * @see
 *
 * Copyright 2015. ARM Ltd. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * 
 */
package com.arm.connector.bridge.coordinator.processors.ms;

import com.arm.connector.bridge.coordinator.Orchestrator;
import com.arm.connector.bridge.core.BaseClass;
import com.arm.connector.bridge.core.ErrorLogger;
import com.arm.connector.bridge.core.Utils;
import com.arm.connector.bridge.preferences.PreferenceManager;
import com.arm.connector.bridge.transport.HttpTransport;
import java.util.HashMap;
import java.util.Map;

/**
 * This class defines the required REST functions to manage IoTHub devices via the gateway mapping
 * @author Doug Anson
 */
public class IoTHubDeviceManager extends BaseClass {
    private HttpTransport                           m_http = null;
    private Orchestrator                            m_orchestrator = null;
    private String                                  m_suffix = null;
    private HashMap<String,HashMap<String,String>>  m_endpoint_details = null;
    private String                                  m_device_id_url_template = null;
    private String                                  m_api_version = null;
    private String                                  m_iot_event_hub_name = null;
    private String                                  m_iot_event_hub_add_device_json = null;
    private String                                  m_iot_event_hub_sas_token = null;
    private String                                  m_iot_event_hub_auth_qualifier = "SharedAccessSignature";
    
     // constructor
    public IoTHubDeviceManager(ErrorLogger logger,PreferenceManager preferences,HttpTransport http,Orchestrator orchestrator) {
        this(logger,preferences,null,http,orchestrator);
    }
    
    // constructor
    public IoTHubDeviceManager(ErrorLogger logger,PreferenceManager preferences,String suffix,HttpTransport http,Orchestrator orchestrator) {
        super(logger,preferences);
        
        // HTTP and suffix support
        this.m_http = http;
        this.m_suffix = suffix;  
        this.m_orchestrator = orchestrator; 
       
        // initialize the endpoint keys map
        this.m_endpoint_details = new HashMap<>();
        
        // IoTHub Name
        this.m_iot_event_hub_name = this.preferences().valueOf("iot_event_hub_name",this.m_suffix);
        
        // IoTHub REST API Version
        this.m_api_version = this.preferences().valueOf("iot_event_hub_api_version",this.m_suffix);
        
        // IoTHub DeviceID REST URL Template
        this.m_device_id_url_template = this.preferences().valueOf("iot_event_hub_device_id_url",this.m_suffix).replace("__IOT_EVENT_HUB__",this.m_iot_event_hub_name).replace("__API_VERSION__", this.m_api_version);
    
        // Add device JSON template
        this.m_iot_event_hub_add_device_json = this.preferences().valueOf("iot_event_hub_add_device_json",this.m_suffix);
        
        // IoTHub SAS Token (take out the qualifier if present...)
        this.m_iot_event_hub_sas_token = this.preferences().valueOf("iot_event_hub_sas_token",this.m_suffix).replace("SharedAccessSignature ", "").trim();
    }
    
    // get the orchestrator
    private Orchestrator orchestrator() { return this.m_orchestrator; }
    
    // process new device registration
    public boolean registerNewDevice(Map message) {
        boolean status = false;
        
        // get the device details
        String device_type = (String)message.get("ept");
        String device = (String)message.get("ep");
        
        // see if we already have a device...
        HashMap<String,String> ep = this.getDeviceDetails(device);
        if (ep != null) {
            // save off this device 
            this.saveDeviceDetails(device, ep);
            
            // we are good
            status = true;
        }
        else {
            // device is not registered... so create/register it
            status = this.createAndRegisterNewDevice(message);
        }
        
        // return our status
        return status;
    }
    
    // create and register a new device
    private boolean createAndRegisterNewDevice(Map message) {
        Boolean status = false; 
        
        // create the new device type
        String device_type = (String)message.get("ept");
        String device = (String)message.get("ep");
        
        // create the URL
        String url = this.m_device_id_url_template.replace("__EPNAME__", device);
                 
        // build out the PUT payload
        String payload = this.m_iot_event_hub_add_device_json.replace("__EPNAME__", device);
        
        // DEBUG
        this.errorLogger().info("IoTHub: registerNewDevice: URL: " + url + " DATA: " + payload);
        
        // dispatch and look for the result
        String result = this.put(url,payload);
        
        // check the result
        int http_code = this.m_http.getLastResponseCode();
        if (Utils.httpResponseCodeOK(http_code)) {
            // DEBUG
            this.errorLogger().info("IoTHub: registerNewDevice: SUCCESS. RESULT: " + result);
            status = true;
            
            // DEBUG
            this.errorLogger().info("IoTHub: registerNewDevice: saving off device details...");
            
            // save off device details...
            this.saveAddDeviceDetails(device,device_type,result);
        }
        else if (http_code == 409) {
             // DEBUG
            this.errorLogger().info("IoTHub: registerNewDevice: SUCCESS (already registered)");
            status = true;
            
            // save off device details...
            this.saveAddDeviceDetails(device,device_type,result);
        }
        else {
            // DEBUG
            this.errorLogger().warning("IoTHub: registerNewDevice: FAILURE: " + this.m_http.getLastResponseCode() + " RESULT: " + result);
        }
           
        // return our status
        return status;
    } 
    
    // process device de-registration
    public Boolean deregisterDevice(String device) { 
        Boolean status = false;
        
        // create the URL
        String url = this.m_device_id_url_template.replace("__EPNAME__", device);
        
        // Get the ETag
        String etag = this.getETagForDevice(device);
        
        // DEBUG
        this.errorLogger().info("IoTHub: deregisterDevice: URL: " + url);
        
        // dispatch and look for the result
        String result = this.delete(url,etag);
        
        // check the result
        if (Utils.httpResponseCodeOK(this.m_http.getLastResponseCode())) {
            // DEBUG
            this.errorLogger().info("IoTHub: deregisterDevice: SUCCESS. RESULT: " + result);
        }
        else {
            // DEBUG
            this.errorLogger().warning("IoTHub: deregisterDevice: FAILURE: " + this.m_http.getLastResponseCode() + " RESULT: " + result);
        }
        
        // remove the endpoint details
        this.m_endpoint_details.remove(device);
        
        // return our status
        status = true;
        return status;
    }
    
    // get a given device's details...
    private HashMap<String,String> getDeviceDetails(String device) {
        HashMap<String,String> ep = null;
        Boolean status = false;
        
        // create the URL
        String url = this.m_device_id_url_template.replace("__EPNAME__", device);
        
        // DEBUG
        this.errorLogger().info("IoTHub: getDeviceDetails: URL: " + url);
        
        // dispatch and look for the result
        String result = this.get(url);
                
        // check the result
        if (Utils.httpResponseCodeOK(this.m_http.getLastResponseCode())) {
            // DEBUG
            this.errorLogger().info("IoTHub: getDeviceDetails: SUCCESS. RESULT: " + result);
            status = true;
        }
        else {
            // DEBUG
            this.errorLogger().warning("IoTHub: getDeviceDetails: FAILURE: " + this.m_http.getLastResponseCode() + " RESULT: " + result);
        }
        
        // parse our result...
        if (status == true) {
            ep = this.parseDeviceDetails(device,result);
        }
        
        // return our endpoint details
        return ep;
    }
    
    // GET specific data to a given URL 
    private String get(String url) {
        this.m_http.setAuthorizationQualifier(this.m_iot_event_hub_auth_qualifier);
        String result = this.m_http.httpsGetApiTokenAuth(url,this.m_iot_event_hub_sas_token,null,"application/json",null);
        return result;
    }
    
    // PUT specific data to a given URL (with data)
    private String put(String url,String payload) {
        this.m_http.setAuthorizationQualifier(this.m_iot_event_hub_auth_qualifier);
        String result = this.m_http.httpsPutApiTokenAuth(url,this.m_iot_event_hub_sas_token,payload,"application/json",null);
        return result;
    }
    
    // DELETE specific data to a given URL (with data)
    private String delete(String url,String etag) { return this.delete(url,etag,null); }
    private String delete(String url,String etag,String payload) {
        this.m_http.setAuthorizationQualifier(this.m_iot_event_hub_auth_qualifier);
        this.m_http.setETagValue(etag);             // ETag header required...
        this.m_http.setIfMatchValue("*");           // If-Match header required... 
        String result = this.m_http.httpsDeleteApiTokenAuth(url,this.m_iot_event_hub_sas_token,payload,"application/json",null);
        return result;
    }
    
    // Get the ETag value for the device
    private String getETagForDevice(String ep_name) {
        HashMap<String,String> ep = this.getEndpointDetails(ep_name);
        if (ep != null) 
            return ep.get("etag");
        return null;
    }
    
    // get the endpoint key
    public String getEndpointKey(String ep_name) { return this.getEndpointKey(ep_name,"primary_key"); }
    private String getEndpointKey(String ep_name,String id) {
        HashMap<String,String> ep = this.getEndpointDetails(ep_name);
        if (ep != null) 
            return ep.get(id);
        return null;
    }
    
    // get the endpoint details
    public HashMap<String,String> getEndpointDetails(String ep_name) {
        return this.m_endpoint_details.get(ep_name);
    }
    
    // Help the JSON parser with null strings... ugh
    private String helpJSONParser(String json) {
        if (json != null && json.length() > 0) return json.replace(":null",":\"none\"").replace(":\"\"",":\"none\"");
        return json;
    }
    
    // parse our device details
    private HashMap<String,String> parseDeviceDetails(String device,String json) { return this.parseDeviceDetails(device,"",json); }
    private HashMap<String,String> parseDeviceDetails(String device,String device_type,String json) {
        HashMap<String,String> ep = null;
        
        // check the input json
        if (json != null) {
            try {
                if (json.contains("ErrorCode:DeviceNotFound;") == false) {
                    // fix up empty values
                    json = this.helpJSONParser(json);

                    // Parse the JSON...
                    Map parsed = this.orchestrator().getJSONParser().parseJson(json);

                    // Device Details
                    ep = new HashMap<>();

                    // Device Keys
                    Map authentication = (Map)parsed.get("authentication");
                    Map symmetric_key = (Map)authentication.get("symmetricKey");
                    ep.put("primary_key",(String)symmetric_key.get("primaryKey"));
                    ep.put("secondary_key",(String)symmetric_key.get("secondaryKey"));

                    // ETag for device
                    ep.put("etag",(String)parsed.get("etag"));

                    // Device Name
                    ep.put("deviceID",(String)parsed.get("deviceId"));
                    ep.put("ep_name",device);
                    ep.put("ep_type",device_type);

                    // record the entire record for later...
                    ep.put("json_record",json);

                    // DEBUG
                    //this.errorLogger().info("parseDeviceDetails for " + device + ": " + ep);
                }
                else {
                    // device is not found
                    this.errorLogger().warning("IoTHub: parseDeviceDetails: device " + device + " is not a registered device (OK)");
                    ep = null;
                }
            }
            catch (Exception ex) {
                // exception in parsing... so nullify...
                this.errorLogger().warning("IoTHub: parseDeviceDetails: exception while parsing device " + device + " JSON: " + json,ex);
                if (ep != null) {
                    this.errorLogger().warning("IoTHub: parseDeviceDetails: last known ep contents: " + ep);
                }
                else {
                    this.errorLogger().warning("IoTHub: parseDeviceDetails: last known ep contents: EMPTY");
                }
                ep = null;
            }
        }
        else {
            this.errorLogger().warning("IoTHub: parseDeviceDetails: input JSON is EMPTY");
            ep = null;
        }
        
        // return our endpoint details
        return ep;
    }
    
    // Parse the AddDevice result and capture key elements 
    private void saveAddDeviceDetails(String device,String device_type,String json) {
        // parse our device details into structure
        HashMap<String,String> ep = this.parseDeviceDetails(device, device_type, json);
        if (ep != null) {
            // save off the details
            this.saveDeviceDetails(device, ep);
        }
        else {
            // unable to parse details
            this.errorLogger().warning("IoTHub: saveAddDeviceDetails: ERROR: unable to parse device " + device + " details JSON: " + json);
        }
    }
    
    // save device details
    public void saveDeviceDetails(String device,HashMap<String,String> entry) {
        // don't overwrite an existing entry..
        if (this.m_endpoint_details.get(device) == null) {
            // DEBUG
            this.errorLogger().info("IoTHub: saveDeviceDetails: saving " + device + ": " + entry);

            // save off the endpoint details
            this.m_endpoint_details.put(device,entry);
        }
    }
    
    // create a MQTT Password for a given device
    public String createMQTTPassword(String device) {
        // use the IoTHub SAS Token + the original signature qualifier
        return this.m_iot_event_hub_auth_qualifier + " " + this.m_iot_event_hub_sas_token;
    }
}
