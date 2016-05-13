/**
 * @file    AsyncResponseManager.java
 * @brief   async response manager for mDS async response handling in connector bridge
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

package com.arm.connector.bridge.coordinator.processors.core;

import com.arm.connector.bridge.coordinator.Orchestrator;
import com.arm.connector.bridge.coordinator.processors.arm.GenericMQTTProcessor;
import com.arm.connector.bridge.core.ErrorLogger;
import com.arm.connector.bridge.json.JSONParser;
import com.arm.connector.bridge.transport.MQTTTransport;
import java.util.HashMap;
import java.util.Map;

/**
 * async response manager handling async responses from mDS for connector-bridge
 * @author Doug Anson
 */
public class AsyncResponseManager {
    private HashMap<String,HashMap<String,Object>>  m_responses;
    private Orchestrator                            m_manager;
    
    public AsyncResponseManager(Orchestrator manager) {
        this.m_manager = manager;
        this.m_responses = new HashMap<>();
    }
    
    // get the error logger
    private Orchestrator manager() {
        return this.m_manager;
    }
    
    // get the error logger
    private ErrorLogger errorLogger() {
        return this.manager().errorLogger();
    }
    
    // get the async response ID
    private String id(Map response) {
        return (String)response.get("id");
    }
        
    // do we have a recording for a given AsyncResponse?
    private boolean haveRecordForAsyncResponse(String id) {
        return (this.m_responses.containsKey(id) == true);
    }
    
    // record an AsyncResponse
    public void recordAsyncResponse(String response,String coap_verb,MQTTTransport mqtt,GenericMQTTProcessor proc,String response_topic,String reply_topic,String message, String ep_name, String uri) {
        // create a new AsyncResponse record
        HashMap<String,Object> record = new HashMap<>();
       
        // fill it... 
        record.put("verb", coap_verb);
        record.put("response",response);
        record.put("mqtt",mqtt);
        record.put("proc",proc);
        record.put("response_topic",response_topic);
        record.put("reply_topic",reply_topic);
        record.put("message",message);
        record.put("ep_name",ep_name);
        record.put("uri",uri);
        
        // parse the
        JSONParser parser = this.manager().getJSONParser();
        Map parsed = parser.parseJson(response);
        
        // add it to the record too
        record.put("response_map",parsed);
        
        // add the record to our list
        this.m_responses.put((String)parsed.get("async-response-id"), record);
        
        // DEBUG
        this.errorLogger().info("recordAsyncResponse: Adding Record: ID:" + (String)parsed.get("async-response-id") + " RECORD: " + record);
    }

    // process AsyncResponse
    @SuppressWarnings("empty-statement")
    public void processAsyncResponse(Map response) {
        // get our AsyncResponse ID
        String id = this.id(response);
        
        // do we have a record for this AsyncResponse?
        if (this.haveRecordForAsyncResponse(id) == true) {
            // Get the record
            HashMap<String,Object> record = this.m_responses.get(id);
            
            // pull the requisite elements from the record
            MQTTTransport mqtt = (MQTTTransport)record.get("mqtt");
            String response_topic = (String)record.get("response_topic");
            String verb = (String)record.get("verb");
            GenericMQTTProcessor proc = (GenericMQTTProcessor)record.get("proc");
            
            // construct the reply message value
            String reply = proc.formatAsyncResponseAsReply(response,verb);
            if (reply != null) {
                // Get the reply MQTT topic...default is the response topic
                String target_topic = response_topic;
                
                // If the reply topic is different that the response topic... it takes preference...
                if (record.get("reply_topic") != null) {
                    target_topic = (String)record.get("reply_topic");
                }
                
                // DEBUG
                this.errorLogger().info("processAsyncResponse: sending reply(" + verb + ") to AsyncResponse: ID: " + id + " Topic: " + target_topic + " Message: " + reply);

                // send the reply...
                mqtt.sendMessage(target_topic, reply);
            }
            else {
                // DEBUG
                this.errorLogger().info("processAsyncResponse: not sending reply(" + verb + ") to AsyncResponse: ID: " + id + " (OK).");
            }
            
            // DEBUG
            this.errorLogger().info("processAsyncResponse: Removing record for AsyncResponse: ID: " + id);
                    
            // finally delete the record
            this.m_responses.remove(id);
        }
        else {
            // processing something we have no record on...
            ;
            
            // DEBUG
            //this.errorLogger().info("processAsyncResponse: No AsyncResponse record for ID: " + id + " Ignoring: " + response.toString());
        }
    }
}
