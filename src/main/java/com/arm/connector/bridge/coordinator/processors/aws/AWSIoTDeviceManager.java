/**
 * @file    AWSIoTDeviceManager.java
 * @brief   MS AWSIoT Device Manager for the MS AWSIoT Peer Processor
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
package com.arm.connector.bridge.coordinator.processors.aws;

import com.arm.connector.bridge.coordinator.Orchestrator;
import com.arm.connector.bridge.core.BaseClass;
import com.arm.connector.bridge.core.ErrorLogger;
import com.arm.connector.bridge.core.Utils;
import com.arm.connector.bridge.preferences.PreferenceManager;
import com.arm.connector.bridge.transport.HttpTransport;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * This class defines the required REST functions to manage AWSIoT devices via the gateway mapping
 * @author Doug Anson
 */
public class AWSIoTDeviceManager extends BaseClass {
    private HttpTransport                           m_http = null;
    private Orchestrator                            m_orchestrator = null;
    private String                                  m_suffix = null;
    private HashMap<String,HashMap<String,String>>  m_endpoint_details = null;
    
    private ArrayList<String>                       m_keys_cert_ids = null;
    
    // XXX make configurable
    private String                                  m_policy_name = null;
    private String                                  m_policy_document = null;
    
     // constructor
    public AWSIoTDeviceManager(ErrorLogger logger,PreferenceManager preferences,HttpTransport http,Orchestrator orchestrator) {
        this(logger,preferences,null,http,orchestrator);
    }
    
    // constructor
    public AWSIoTDeviceManager(ErrorLogger logger,PreferenceManager preferences,String suffix,HttpTransport http,Orchestrator orchestrator) {
        super(logger,preferences);
        
        // HTTP and suffix support
        this.m_http = http;
        this.m_suffix = suffix;  
        this.m_orchestrator = orchestrator; 
       
        // initialize the endpoint keys map
        this.m_endpoint_details = new HashMap<>();   
        
        // initialize the keys/cert id cache
        this.m_keys_cert_ids = new ArrayList<>();
        
        // get configuration params
        this.m_policy_name = this.orchestrator().preferences().valueOf("aws_iot_policy_name",this.m_suffix);
        this.m_policy_document = this.orchestrator().preferences().valueOf("aws_iot_policy_document",this.m_suffix);
    }
    
    // get the orchestrator
    private Orchestrator orchestrator() { return this.m_orchestrator; }
    
    // get our endpoint details
    public HashMap<String,String> getEndpointDetails(String ep_name) {
        return this.m_endpoint_details.get(ep_name);
    }
    
    // process new device registration
    public boolean registerNewDevice(Map message) {
        boolean status = false;
        
        // get the device details
        String device_type = (String)message.get("ept");
        String device = (String)message.get("ep");
        
        // see if we already have a device...
        HashMap<String,String> ep = this.getDeviceDetails(device);
        if (ep != null) {
            // complete the details
            this.completeDeviceDetails(ep);
            
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
        // create the new device type
        String device_type = (String)message.get("ept");
        String device = (String)message.get("ep");
        
        // invoke AWS CLI to create a new device
        String args = "iot create-thing --thing-name=" + device;
        String result = Utils.awsCLI(this.errorLogger(), args);
        
        // DEBUG
        this.errorLogger().info("AWSIoT: registerNewDevice: RESULT: " + result);
        
        // return our status
        Boolean status = (result != null && result.length() > 0);
        
        // If OK, save the result
        if (status == true) {
            // DEBUG
            this.errorLogger().info("AWSIoT: registerNewDevice: saving off device details...");
            
            // save off device details...
            this.saveAddDeviceDetails(device,device_type,result);
        }
                
        // return our status
        return status;
    } 
    
    // unlink the certificate from the Thing Record
    private void unlinkCertificateFromThing(HashMap<String,String> ep) {
        this.unlinkCertificateFromThing((String)ep.get("thingName"),(String)ep.get("certificateArn"));
    }
    
    // unlink the certificate from the Thing Record
    private void unlinkCertificateFromThing(String ep_name,String arn) {
        String ep_qual = "--thing-name=" + ep_name;
        if (ep_name == null) ep_qual = "";
        
        String args = "iot detach-thing-principal " + ep_qual + " --principal=" + arn;
        Utils.awsCLI(this.errorLogger(), args);
    }
    
    // unlink the certificate from the Policy
    private void unlinkCertificateFromPolicy(HashMap<String,String> ep) {
        this.unlinkCertificateFromPolicy((String)ep.get("certificateArn"));
    }
    
    // unlink the certificate from the Policy
    private void unlinkCertificateFromPolicy(String arn) {
        String args = "iot detach-principal-policy --policy-name=" + this.m_policy_name + " --principal=" + arn;
        Utils.awsCLI(this.errorLogger(), args);
    }
    
    // inactivate the Certificate
    private void inactivateCertificate(HashMap<String,String> ep) {
        this.inactivateCertificate((String)ep.get("certificateId"));
    }
    
    // inactivate the Certificate
    private void inactivateCertificate(String id) {
        String args = "iot update-certificate --certificate-id=" + id + " --new-status=INACTIVE";
        Utils.awsCLI(this.errorLogger(), args);
    }
    
    // delete the Certificate
    private void deleteCertificate(HashMap<String,String> ep) {
        this.deleteCertificate((String)ep.get("certificateId"));
    }
    
    // delete the Certificate
    private void deleteCertificate(String id) {
        String args = "iot delete-certificate --certificate-id=" + id;
        Utils.awsCLI(this.errorLogger(), args);
    }
    
    // get the key and cert index 
    private int getKeyAndCertIndex(String id) {
        int index = -1;
        
        for(int i=0;i<this.m_keys_cert_ids.size() && index < 0;++i) {
            if (id.equalsIgnoreCase(this.m_keys_cert_ids.get(i)) == true) {
                index = i;
            }
        }
        
        return index;
    }
    
    // unlink a certificate from the policy and thing record, then deactivate it
    private void removeCertificate(String device) {
        // Get our record 
        HashMap<String,String> ep = this.getEndpointDetails(device);
        if (ep != null) {
            // unlink the certificate from the thing record
            this.unlinkCertificateFromThing(ep);
            
            // unlink the certificate from the thing record
            this.unlinkCertificateFromPolicy(ep);
            
            // inactivate the certificate
            this.inactivateCertificate(ep);
            
            // delete the certificate
            this.deleteCertificate(ep);
        }
        
        // remove locally as well
        int index = this.getKeyAndCertIndex(device);
        if (index >= 0) {
            this.m_keys_cert_ids.remove(index);
        }
    }
    
    // process device de-registration
    public boolean deregisterDevice(String device) { 
        // first we unlink the certificate and deactivate it
        this.removeCertificate(device);
        
        // invoke AWS CLI to create a new device
        String args = "iot delete-thing --thing-name=" + device;
        String result = Utils.awsCLI(this.errorLogger(), args);
        
        // DEBUG
        this.errorLogger().info("deregisterDevice: RESULT: " + result);
        
        // remove the endpoint details
        this.m_endpoint_details.remove(device);
        
        // return our status
        boolean status = true;
        return status;
    }
    
    // complete the device details
    private void completeDeviceDetails(HashMap<String,String> ep) {    
        // add the endpoint address details
        if (ep.get("endpointAddress") == null) {
            this.captureEndpointAddress(ep);
        }

        // add the policy details
        if (ep.get("policyArn") == null) {
            ep.put("policyName", this.m_policy_name);
            this.capturePolicyDetails(ep);
        }

        // create and add the certificate details
        if (ep.get("certificateId") == null) {
            this.createKeysAndCertsAndLinkToPolicyAndThing(ep);
        }
    }
    
    // get a given device's details...
    private HashMap<String,String> getDeviceDetails(String device) {
        HashMap<String,String> ep = this.getEndpointDetails(device);
        
        // if we dont already have it, go get it... 
        if (ep == null) {
            // invoke AWS CLI to create a new device
            String args = "iot describe-thing --thing-name=" + device;
            String result = Utils.awsCLI(this.errorLogger(), args);

            // DEBUG
            //this.errorLogger().info("getDeviceDetails: RESULT: " + result);

            // parse our result...
            if (result != null && result.length() > 0) {
                ep = this.parseDeviceDetails(device,result);
            }
        }
        
        // return our endpoint details
        return ep;
    }
    
    // Help the JSON parser with null strings... ugh
    private String helpJSONParser(String json) {
        if (json != null && json.length() > 0) return json.replace(":null",":\"none\"").replace(":\"\"",":\"none\"").replace("\"attributes\": {},","");
        return json;
    }
    
    // parse our device details
    private HashMap<String,String> parseDeviceDetails(String device,String json) { return this.parseDeviceDetails(device,"",json); }
    private HashMap<String,String> parseDeviceDetails(String device,String device_type,String json) {
        HashMap<String,String> ep = null;
        
        // check the input json
        if (json != null) {
            try {
                if (json.contains("ResourceNotFoundException") == false) {
                    // fix up empty values
                    json = this.helpJSONParser(json);

                    // Parse the JSON...
                    Map parsed = this.orchestrator().getJSONParser().parseJson(json);

                    // Device Details
                    ep = new HashMap<>();

                    // Device Name
                    ep.put("thingName",(String)parsed.get("thingName"));
                    ep.put("defaultClientId",(String)parsed.get("defaultClientId"));
                    ep.put("attributes",(String)parsed.get("thingName"));
                    ep.put("ep_name",device);
                    ep.put("ep_type",device_type);

                    // record the entire record for later...
                    ep.put("json_record",json);

                    // DEBUG
                    this.errorLogger().info("AWSIoT: parseDeviceDetails for " + device + ": " + ep);
                }
                else {
                    // device is not found
                    this.errorLogger().warning("AWSIoT: parseDeviceDetails: device " + device + " is not a registered device (OK)");
                    ep = null;
                }
            }
            catch (Exception ex) {
                // exception in parsing... so nullify...
                this.errorLogger().warning("AWSIoT: parseDeviceDetails: exception while parsing device " + device + " JSON: " + json,ex);
                if (ep != null) {
                    this.errorLogger().warning("AWSIoT: parseDeviceDetails: last known ep contents: " + ep);
                }
                else {
                    this.errorLogger().warning("AWSIoT: parseDeviceDetails: last known ep contents: EMPTY");
                }
                ep = null;
            }
        }
        else {
            this.errorLogger().warning("AWSIoT: parseDeviceDetails: input JSON is EMPTY");
            ep = null;
        }
        
        // return our endpoint details
        return ep;
    }
    
    // generate keys and certs
    private void createKeysAndCerts(HashMap<String,String> ep) {
        // AWS IOT CLI to create the keys and certificates
        String args = "iot create-keys-and-certificate --set-as-active";
        String result = Utils.awsCLI(this.errorLogger(), args);
         
        // DEBUG
        //this.errorLogger().info("createKeysAndCerts: RESULT: " + result);
        
        // parse if we have something to parse
        if (result != null && result.length() > 2) {
            // parse it
            Map parsed = this.m_orchestrator.getJSONParser().parseJson(result);
            
            // put the key items into our map
            ep.put("certificateArn",(String)parsed.get("certificateArn"));
            ep.put("certificatePem",(String)parsed.get("certificatePem"));
            ep.put("certificateId",(String)parsed.get("certificateId"));
            
            // Keys are also available
            Map keys = (Map)parsed.get("keyPair");
            
            // install the keys
            ep.put("PrivateKey",(String)keys.get("PrivateKey"));
            ep.put("PublicKey",(String)keys.get("PublicKey"));
            
            // save off to the key store
            this.m_keys_cert_ids.add((String)parsed.get("certificateId"));
        }
        
        // DEBUG
        //this.errorLogger().info("createKeysAndCerts: EP: " + ep);
    } 
    
    // get our current defaulted policy
    private String getDefaultPolicy() {
        // AWS CLI invocation...
        String args = "iot get-policy --policy-name=" + this.m_policy_name;
        return Utils.awsCLI(this.errorLogger(), args);
    }
    
    // save off the default policy
    private void saveDefaultPolicy(HashMap<String,String> ep,String json) {
        Map parsed = this.m_orchestrator.getJSONParser().parseJson(json);
        ep.put("policyName", (String)parsed.get("policyName"));
        ep.put("policyArn", (String)parsed.get("policyArn"));
        ep.put("policyVersionId", (String)parsed.get("policyVersionId"));
        ep.put("policyDocument", (String)parsed.get("policyDocument"));
    }
    
    // create and save the defaulted policy
    private void checkAndCreateDefaultPolicy(HashMap<String,String> ep) { 
        // Our default policy
        String policy_json = this.getDefaultPolicy();
        
        // if we dont have it, create it...
        if (policy_json == null || policy_json.length() == 0) {
            // AWS CLI invocation...
            String args = "iot create-policy --policy-name=" + this.m_policy_name + " --policy-document=" + this.m_policy_document;
            Utils.awsCLI(this.errorLogger(), args);
            policy_json = this.getDefaultPolicy();
        }
         
        // save off
        this.saveDefaultPolicy(ep, policy_json);
    }
    
    // link the certificate to the thing record and the default policy
    private void linkCertificateToThingAndPolicy(HashMap<String,String> ep) {
        // AWS CLI invocation - link policy to certficate ARN
        String args = "iot attach-principal-policy --policy-name=" + this.m_policy_name + " --principal=" + (String)ep.get("certificateArn");
        Utils.awsCLI(this.errorLogger(), args);
        
        // AWS CLI invocation - link thing record to certificate
        args = "iot attach-thing-principal --thing-name=" + (String)ep.get("thingName")+ " --principal=" + (String)ep.get("certificateArn");
        Utils.awsCLI(this.errorLogger(), args);
    }
    
    // capture the endpoint address
    private void captureEndpointAddress(HashMap<String,String> ep) {
        // AWS CLI invocation - link policy to certficate ARN
        String args = "iot describe-endpoint";
        String json = Utils.awsCLI(this.errorLogger(), args);
        if (json != null && json.length() > 0) {
            Map parsed = this.m_orchestrator.getJSONParser().parseJson(json);
            ep.put("endpointAddress",(String)parsed.get("endpointAddress"));
        }
    }
    
    // capture the certificate details
    private void captureCertificateDetails(HashMap<String,String> ep) {
        // AWS CLI invocation - link policy to certficate ARN
        String args = "iot describe-endpoint";
        String json = Utils.awsCLI(this.errorLogger(), args);
        if (json != null && json.length() > 0) {
            Map parsed = this.m_orchestrator.getJSONParser().parseJson(json);
            ep.put("endpointAddress",(String)parsed.get("endpointAddress"));
        }
    }
    
    // capture the policy details
    private void capturePolicyDetails(HashMap<String,String> ep) {
        String json = this.getDefaultPolicy();
        if (json != null && json.length() > 0) {
            this.saveDefaultPolicy(ep, json);
        }
    }
    
    // create keys and certs and link to the device 
    void createKeysAndCertsAndLinkToPolicyAndThing(HashMap<String,String> ep) {
        // add the certificates and keys
        this.createKeysAndCerts(ep);            

        // link the thing record and default policy to the certificate
        this.linkCertificateToThingAndPolicy(ep);
    }
    
    // Parse the AddDevice result and capture key elements 
    private void saveAddDeviceDetails(String device,String device_type,String json) {
        // parse our device details into structure
        HashMap<String,String> ep = this.parseDeviceDetails(device, device_type, json);
        if (ep != null) {
            // create the default policy
            this.checkAndCreateDefaultPolicy(ep);
            
            // create the keys/certs and link it to the default policy and device
            this.createKeysAndCertsAndLinkToPolicyAndThing(ep);
            
            // capture the endpoint address
            this.captureEndpointAddress(ep);
            
            // save off the details
            this.saveDeviceDetails(device, ep);
        }
        else {
            // unable to parse details
            this.errorLogger().warning("AWSIoT: saveAddDeviceDetails: ERROR: unable to parse device " + device + " details JSON: " + json);
        }
    }
  
    // save device details
    public void saveDeviceDetails(String device,HashMap<String,String> entry) {
        // don't overwrite an existing entry..
        if (this.getEndpointDetails(device) == null) {
            // save off the endpoint details
            this.m_endpoint_details.put(device,entry);
        }
    }
    
    // is this cert_id used by one of the existing devices?
    private boolean isUsedCert(String id) {
        boolean found = false;
        for(int i=0;i<this.m_keys_cert_ids.size() && !found && id != null && id.length() > 0;++i) {
            if (id.equalsIgnoreCase(this.m_keys_cert_ids.get(i))) {
                found = true;
            }
        }
        return found;
    }
    
    // get our list of certificates
    private ArrayList<HashMap<String,String>> getRegisteredCertificates() {
        ArrayList<HashMap<String,String>> list = new ArrayList<>();
        
        // AWS IoT CLI
        String args = "iot list-certificates";
        String json = Utils.awsCLI(this.errorLogger(), args);
        
        // parse and process the result
        if (json != null && json.length() > 0) {
            Map parsed = this.m_orchestrator.getJSONParser().parseJson(json);
            List cert_list = (List)parsed.get("certificates");
            for(int i=0;cert_list != null && i<cert_list.size();++i) {
                Map entry = (Map)cert_list.get(i);
                HashMap<String,String> cert = new HashMap<>();
                cert.put("certificateId",(String)entry.get("certificateId"));
                cert.put("certificateArn",(String)entry.get("certificateArn"));
                list.add(cert);
            }
        }
        
        // DEBUG
        this.errorLogger().info("AWSIoT: getRegisteredCertificates(" + list.size() + "): " + list);
        
        // return the list
        return list;
    }
    
    // clear out any stale Keys and Certs
    public void clearOrhpanedKeysAndCerts() {
        ArrayList<HashMap<String,String>> cert_list = this.getRegisteredCertificates();
        for(int i=0;i<cert_list.size();++i) {
            // get the certificate ID
            HashMap<String,String> cert = cert_list.get(i);
            String cert_id = cert.get("certificateId");
            String cert_arn = cert.get("certificateArn");
            
            // get the device
            
            // if NOT present in any of the device entries... kill it
            if (this.isUsedCert(cert_id) == false) {
                // unlink the certificate from the thing record
                this.unlinkCertificateFromThing(null,cert_arn);
            
                // unlink the certificate from the thing record
                this.unlinkCertificateFromPolicy(cert_arn);
            
                // inactivate
                this.inactivateCertificate(cert_id);
                
                // delete
                this.deleteCertificate(cert_id);
                
                // purge the certificate from the cache
                this.m_keys_cert_ids.remove(this.getKeyAndCertIndex(cert_id));
            }
        }
    }
}
