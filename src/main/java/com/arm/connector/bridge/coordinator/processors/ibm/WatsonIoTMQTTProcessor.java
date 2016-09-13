/**
 * @file    WatsonIoTMQTTProcessor.java
 * @brief   IBM WatsonIoT MQTT Peer Processor
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

package com.arm.connector.bridge.coordinator.processors.ibm;

import com.arm.connector.bridge.coordinator.processors.arm.GenericMQTTProcessor;
import com.arm.connector.bridge.coordinator.Orchestrator;
import com.arm.connector.bridge.coordinator.processors.interfaces.AsyncResponseProcessor;
import com.arm.connector.bridge.coordinator.processors.interfaces.PeerInterface;
import com.arm.connector.bridge.core.Utils;
import com.arm.connector.bridge.transport.HttpTransport;
import com.arm.connector.bridge.transport.MQTTTransport;
import com.arm.connector.bridge.core.Transport;
import com.arm.connector.bridge.json.JSONParser;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.commons.codec.binary.Base64;
import org.fusesource.mqtt.client.QoS;
import org.fusesource.mqtt.client.Topic;

/**
 * IBM WatsonIoT peer processor based on MQTT with MessageSight
 * @author Doug Anson
 */
public class WatsonIoTMQTTProcessor extends GenericMQTTProcessor implements Transport.ReceiveListener, PeerInterface, AsyncResponseProcessor {
    public static int               NUM_COAP_VERBS = 4;                                   // GET, PUT, POST, DELETE
    
    private String                  m_observation_type = "observation";
    private String                  m_async_response_type = "cmd-response";
    
    private String                  m_mqtt_ip_address = null;
    private String                  m_watson_iot_observe_notification_topic = null;
    private String                  m_watson_iot_coap_cmd_topic_get = null;
    private String                  m_watson_iot_coap_cmd_topic_put = null;
    private String                  m_watson_iot_coap_cmd_topic_post = null;
    private String                  m_watson_iot_coap_cmd_topic_delete = null;
    private HashMap<String,Object>  m_watson_iot_endpoints = null;
    private String                  m_watson_iot_org_id = null;
    private String                  m_watson_iot_org_key = null;
    private String                  m_client_id_template = null;
    private String                  m_watson_iot_device_data_key = null;
    
    // WatsonIoT bindings
    private String                  m_watson_iot_api_key = null;
    private String                  m_watson_iot_auth_token = null;
    
    // WatsonIoT Device Manager
    private WatsonIoTDeviceManager  m_watson_iot_device_manager = null;
        
    // constructor (singleton)
    public WatsonIoTMQTTProcessor(Orchestrator manager,MQTTTransport mqtt,HttpTransport http) {
        this(manager,mqtt,null,http);
    }
    
    // constructor (with suffix for preferences)
    public WatsonIoTMQTTProcessor(Orchestrator manager,MQTTTransport mqtt,String suffix,HttpTransport http) {
        super(manager,mqtt,suffix,http);
        
        // WatsonIoT Processor Announce
        this.errorLogger().info("IBM Watson IoT Processor ENABLED.");
        
        // initialize the endpoint map
        this.m_watson_iot_endpoints = new HashMap<>();
                        
        // get our defaults
        this.m_watson_iot_org_id = this.orchestrator().preferences().valueOf("iotf_org_id",this.m_suffix);
        this.m_watson_iot_org_key = this.orchestrator().preferences().valueOf("iotf_org_key",this.m_suffix);
        this.m_mqtt_ip_address = this.orchestrator().preferences().valueOf("iotf_mqtt_ip_address",this.m_suffix);
        this.m_mqtt_port = this.orchestrator().preferences().intValueOf("iotf_mqtt_port",this.m_suffix);
        
        // get our configured device data key 
        this.m_watson_iot_device_data_key = this.orchestrator().preferences().valueOf("iotf_device_data_key",this.m_suffix);
        if (this.m_watson_iot_device_data_key == null || this.m_watson_iot_device_data_key.length() <= 0) {
            // default
            this.m_watson_iot_device_data_key = "coap";
        }
        
        // Observation notifications
        this.m_watson_iot_observe_notification_topic = this.orchestrator().preferences().valueOf("iotf_observe_notification_topic",this.m_suffix).replace("__EVENT_TYPE__",this.m_observation_type); 
        
        // Send CoAP commands back through mDS into the endpoint via these Topics... 
        this.m_watson_iot_coap_cmd_topic_get = this.orchestrator().preferences().valueOf("iotf_coap_cmd_topic",this.m_suffix).replace("__COMMAND_TYPE__","get");
        this.m_watson_iot_coap_cmd_topic_put = this.orchestrator().preferences().valueOf("iotf_coap_cmd_topic",this.m_suffix).replace("__COMMAND_TYPE__","put");
        this.m_watson_iot_coap_cmd_topic_post = this.orchestrator().preferences().valueOf("iotf_coap_cmd_topic",this.m_suffix).replace("__COMMAND_TYPE__","post");
        this.m_watson_iot_coap_cmd_topic_delete = this.orchestrator().preferences().valueOf("iotf_coap_cmd_topic",this.m_suffix).replace("__COMMAND_TYPE__","delete");
        
        // establish default bindings
        this.m_watson_iot_api_key = this.orchestrator().preferences().valueOf("iotf_api_key",this.m_suffix).replace("__ORG_ID__",this.m_watson_iot_org_id).replace("__ORG_KEY__",this.m_watson_iot_org_key);
        this.m_watson_iot_auth_token = this.orchestrator().preferences().valueOf("iotf_auth_token",this.m_suffix);
        
        // resync org_id and m_watson_iot_org_key
        this.parseWatsonIoTUsername();
        
        // create the client ID
        this.m_client_id_template = this.orchestrator().preferences().valueOf("iotf_client_id_template",this.m_suffix).replace("__ORG_ID__",this.m_watson_iot_org_id);
        this.m_client_id = this.createWatsonIoTClientID(this.m_mds_domain);
        
        // Watson IoT Device Manager - will initialize and update our WatsonIoT bindings/metadata
        this.m_watson_iot_device_manager = new WatsonIoTDeviceManager(this.orchestrator().errorLogger(),this.orchestrator().preferences(),this.m_suffix,http);
        this.m_watson_iot_device_manager.updateWatsonIoTBindings(this.m_watson_iot_org_id, this.m_watson_iot_org_key);
        this.m_watson_iot_api_key = this.m_watson_iot_device_manager.updateUsernameBinding(this.m_watson_iot_api_key);
        this.m_watson_iot_auth_token = this.m_watson_iot_device_manager.updatePasswordBinding(this.m_watson_iot_auth_token);
        this.m_client_id = this.m_watson_iot_device_manager.updateClientIDBinding(this.m_client_id);
        this.m_mqtt_ip_address = this.m_watson_iot_device_manager.updateHostnameBinding(this.m_mqtt_ip_address);
        
        // RESET in case we want to just connect as an WatsonIoT Application
        if (this.orchestrator().preferences().booleanValueOf("iotf_force_app_binding",this.m_suffix) == true) {
            // DEBUG
            this.errorLogger().warning("WatsonIoT Processor: FORCED binding as WatsonIoT Application - ENABLED");
            
            // override - simply bind as a WatsonIoT applciation
            this.m_watson_iot_api_key = this.orchestrator().preferences().valueOf("iotf_api_key",this.m_suffix).replace("__ORG_ID__",this.m_watson_iot_org_id).replace("__ORG_KEY__",this.m_watson_iot_org_key);
            this.m_watson_iot_auth_token = this.orchestrator().preferences().valueOf("iotf_auth_token",this.m_suffix);
            
            // resync org_id and m_watson_iot_org_key
            this.parseWatsonIoTUsername();

            // create the client ID
            this.m_client_id_template = this.orchestrator().preferences().valueOf("iotf_client_id_template",this.m_suffix).replace("__ORG_ID__",this.m_watson_iot_org_id);
            this.m_client_id = this.createWatsonIoTClientID(this.m_mds_domain);
        }
        
        // create the transport
        mqtt.setUsername(this.m_watson_iot_api_key);
        mqtt.setPassword(this.m_watson_iot_auth_token);
                
        // add the transport
        this.initMQTTTransportList();
        this.addMQTTTransport(this.m_client_id, mqtt);
            
        // DEBUG
        //this.errorLogger().info("WatsonIoT Credentials: Username: " + this.m_mqtt.getUsername() + " PW: " + this.m_mqtt.getPassword());
    }
    
    // get our defaulted reply topic
    @Override
    public String getReplyTopic(String ep_name,String ep_type,String def) {
        return this.customizeTopic(this.m_watson_iot_observe_notification_topic, ep_name, ep_type).replace(this.m_observation_type, this.m_async_response_type);
    }
    
    // parse the WatsonIoT Username
    private void parseWatsonIoTUsername() {
        String[] elements = this.m_watson_iot_api_key.replace("-"," ").split(" ");
        if (elements != null && elements.length >= 3) {
            this.m_watson_iot_org_id = elements[1];
            this.m_watson_iot_org_key = elements[2];
            //this.errorLogger().info("WatsonIoT: org_id: " + elements[1] + " apikey: " + elements[2]);
        }
        else {
            this.errorLogger().info("Watson IoT: unable to parse WatsonIoT Username: " + this.m_watson_iot_api_key);
        }
    }
    
    // OVERRIDE: Connection to WatsonIoT vs. stock MQTT...
    @Override
    protected boolean connectMQTT() {        
        // if not connected attempt
        if (!this.isConnected()) {
            if (this.mqtt().connect(this.m_mqtt_ip_address,this.m_mqtt_port,this.m_client_id,this.m_use_clean_session)) {
                this.orchestrator().errorLogger().info("Watson IoT: Setting CoAP command listener...");
                this.mqtt().setOnReceiveListener(this);
                this.orchestrator().errorLogger().info("Watson IoT: connection completed successfully");
            }
        }
        else {
            // already connected
            this.orchestrator().errorLogger().info("Watson IoT: Already connected (OK)...");
        }
        
        // return our connection status
        this.orchestrator().errorLogger().info("Watson IoT: Connection status: " + this.isConnected());
        return this.isConnected();
    }
    
    // OVERRIDE: (Listening) Topics for WatsonIoT vs. stock MQTT...
    @Override
    @SuppressWarnings("empty-statement")
    protected void subscribeToMQTTTopics() {
        // do nothing... WatsonIoT will have "listenable" topics for the CoAP verbs via the CMD event type...
        ;
    }
    
    // OVERRIDE: process a mDS notification for WatsonIoT
    @Override
    public void processNotification(Map data) {
        // DEBUG
        //this.errorLogger().info("processNotification(WatsonIoT)...");
        
        // get the list of parsed notifications
        List notifications = (List)data.get("notifications");
        for(int i=0;notifications != null && i<notifications.size();++i) {
            // we have to process the payload... this may be dependent on being a string core type... 
            Map notification = (Map)notifications.get(i);
            
            // decode the Payload...
            String b64_coap_payload = (String)notification.get("payload");
            String decoded_coap_payload = Utils.decodeCoAPPayload(b64_coap_payload);
            
            // DEBUG
            //this.errorLogger().info("Watson IoT: Decoded Payload: " + decoded_coap_payload);
            
            // Try a JSON parse... if it succeeds, assume the payload is a composite JSON value...
            Map json_parsed = this.tryJSONParse(decoded_coap_payload);
            if (json_parsed != null && json_parsed.isEmpty() == false) {
                // add in a JSON object payload value directly... 
                notification.put("value", Utils.retypeMap(json_parsed,this.fundamentalTypeDecoder()));             // its JSON (flat...)                                                   // its JSON 
            }
            else {
                // add in a decoded payload value as a fundamental type...
                notification.put("value",this.fundamentalTypeDecoder().getFundamentalValue(decoded_coap_payload)); // its a Float, Integer, or String
            }
                                                
            // we will send the raw CoAP JSON... WatsonIoT can parse that... 
            String coap_raw_json = this.jsonGenerator().generateJson(notification);
            
            // strip off []...
            String coap_json_stripped = this.stripArrayChars(coap_raw_json);
                        
            // get our endpoint name
            String ep_name = (String)notification.get("ep");
            
            // encapsulate into a coap/device packet...
            String iotf_coap_json = coap_json_stripped;
            if (this.m_watson_iot_device_data_key != null && this.m_watson_iot_device_data_key.length() > 0) {
                iotf_coap_json = "{ \"" + this.m_watson_iot_device_data_key + "\":" + coap_json_stripped + "}";
            }
                                    
            // DEBUG
            this.errorLogger().info("Watson IoT: CoAP notification: " + iotf_coap_json);
            
            // send to WatsonIoT...
            if (this.mqtt() != null) {
                boolean status = this.mqtt().sendMessage(this.customizeTopic(this.m_watson_iot_observe_notification_topic,ep_name,this.m_watson_iot_device_manager.getDeviceType(ep_name)),iotf_coap_json,QoS.AT_MOST_ONCE);            
                if (status == true) {
                    // not connected
                    this.errorLogger().info("Watson IoT: CoAP notification sent. SUCCESS");
                }
                else {
                    // send failed
                    this.errorLogger().warning("Watson IoT: CoAP notification not sent. SEND FAILED");
                }
            }
            else {
                // not connected
                this.errorLogger().warning("Watson IoT: CoAP notification not sent. NOT CONNECTED");
            }
        }
    }
    
    // OVERRIDE: process a re-registration in WatsonIoT
    @Override
    public void processReRegistration(Map data) {
        List notifications = (List)data.get("reg-updates");
        for(int i=0;notifications != null && i<notifications.size();++i) {
            Map entry = (Map)notifications.get(i);
            // DEBUG
            // this.errorLogger().info("WatsonIoT: CoAP re-registration: " + entry);
            if (this.hasSubscriptions((String)entry.get("ep")) == false) {
                // no subscriptions - so process as a new registration
                this.errorLogger().info("Watson IoT : CoAP re-registration: no subscriptions.. processing as new registration...");
                this.processRegistration(data,"reg-updates");
                
                /*
                boolean do_register = this.unsubscribe((String)entry.get("ep"));
                if (do_register == true) {
                    this.processRegistration(data,"reg-updates");
                }
                else {
                    this.subscribe((String)entry.get("ep"),(String)entry.get("ept"));
                }
                */
            }
            else {
                // already subscribed (OK)
                this.errorLogger().info("Watson IoT : CoAP re-registration: already subscribed (OK)");
            }
        }
    }
    
    // OVERRIDE: handle de-registrations for WatsonIoT
    @Override
    public String[] processDeregistrations(Map parsed) {
        String[] deregistration = super.processDeregistrations(parsed);
        for(int i=0;deregistration != null && i<deregistration.length;++i) {
            // DEBUG
            this.errorLogger().info("Watson IoT : CoAP de-registration: " + deregistration[i]);
            
            // WatsonIoT add-on... 
            this.unsubscribe(deregistration[i]);
            
            // Remove from WatsonIoT
            this.deregisterDevice(deregistration[i]);
        }
        return deregistration;
    }
    
    // OVERRIDE: process mds registrations-expired messages 
    @Override
    public void processRegistrationsExpired(Map parsed) {
        this.processDeregistrations(parsed);
    }
    
    // OVERRIDE: process a received new registration for WatsonIoT
    @Override
    public void processNewRegistration(Map data) {
        this.processRegistration(data,"registrations");
    }
    
    // OVERRIDE: process a received new registration for WatsonIoT
    @Override
    protected void processRegistration(Map data,String key) {  
        List endpoints = (List)data.get(key);
        for(int i=0;endpoints != null && i<endpoints.size();++i) {
            Map endpoint = (Map)endpoints.get(i);            
            List resources = (List)endpoint.get("resources");
            for(int j=0;resources != null && j<resources.size();++j) {
                Map resource = (Map)resources.get(j); 
                
                // re-subscribe
                if (this.m_subscriptions.containsSubscription(this.m_mds_domain,(String)endpoint.get("ep"),(String)endpoint.get("ept"),(String)resource.get("path"))) {
                    // re-subscribe to this resource
                    this.orchestrator().subscribeToEndpointResource((String)endpoint.get("ep"),(String)resource.get("path"),false);
                    
                    // SYNC: here we dont have to worry about Sync options - we simply dispatch the subscription to mDS and setup for it...
                    this.m_subscriptions.removeSubscription(this.m_mds_domain,(String)endpoint.get("ep"),(String)endpoint.get("ept"),(String)resource.get("path"));
                    this.m_subscriptions.addSubscription(this.m_mds_domain,(String)endpoint.get("ep"),(String)endpoint.get("ept"),(String)resource.get("path"));
                }
                
                // auto-subscribe
                else if (this.isObservableResource(resource) && this.m_auto_subscribe_to_obs_resources == true) {
                    // auto-subscribe to observable resources... if enabled.
                    this.orchestrator().subscribeToEndpointResource((String)endpoint.get("ep"),(String)resource.get("path"),false);
                    
                    // SYNC: here we dont have to worry about Sync options - we simply dispatch the subscription to mDS and setup for it...
                    this.m_subscriptions.removeSubscription(this.m_mds_domain,(String)endpoint.get("ep"),(String)endpoint.get("ept"),(String)resource.get("path"));
                    this.m_subscriptions.addSubscription(this.m_mds_domain,(String)endpoint.get("ep"),(String)endpoint.get("ept"),(String)resource.get("path"));
                }
            }    
                        
            // invoke a GET to get the resource information for this endpoint... we will update the Metadata when it arrives
            this.retrieveEndpointAttributes(endpoint);
        }
    }
    
    // create the WatsonIoT clientID
    private String createWatsonIoTClientID(String domain) {
        int length = 12;
        if (domain == null) domain = this.prefValue("mds_def_domain",this.m_suffix);
        if (domain.length() < 12) length = domain.length();
        return this.m_client_id_template + domain.substring(0,length);  // 12 digits only of the domain
    }
    
    // create the endpoint WatsonIoT topic data
    private HashMap<String,Object> createEndpointTopicData(String ep_name,String ep_type) {
        HashMap<String,Object> topic_data = null;
        if (this.m_watson_iot_coap_cmd_topic_get != null) {
            Topic[] list = new Topic[NUM_COAP_VERBS];
            String[] topic_string_list = new String[NUM_COAP_VERBS];
            topic_string_list[0] = this.customizeTopic(this.m_watson_iot_coap_cmd_topic_get,ep_name,ep_type);
            topic_string_list[1] = this.customizeTopic(this.m_watson_iot_coap_cmd_topic_put,ep_name,ep_type);
            topic_string_list[2] = this.customizeTopic(this.m_watson_iot_coap_cmd_topic_post,ep_name,ep_type);
            topic_string_list[3] = this.customizeTopic(this.m_watson_iot_coap_cmd_topic_delete,ep_name,ep_type);
            for(int i=0;i<NUM_COAP_VERBS;++i) {
                list[i] = new Topic(topic_string_list[i],QoS.AT_LEAST_ONCE);
            }
            topic_data = new HashMap<>();
            topic_data.put("topic_list",list);
            topic_data.put("topic_string_list",topic_string_list);
        }
        return topic_data;
    }
    
    // final customization of a MQTT Topic...
    private String customizeTopic(String topic,String ep_name,String ep_type) {
        String cust_topic = topic.replace("__EPNAME__", ep_name);
        if (ep_type != null) cust_topic = cust_topic.replace("__DEVICE_TYPE__", ep_type);
        this.errorLogger().info("Watson IoT:  Customized Topic: " + cust_topic); 
        return cust_topic;
    }
    
    // disconnect
    private void disconnect() {
        if (this.isConnected()) {
            this.mqtt().disconnect();
        }
    }
    
    // are we connected
    private boolean isConnected() {
        if (this.mqtt() != null) return this.mqtt().isConnected();
        return false;
    }
    
    // subscribe to the WatsonIoT MQTT topics
    private void subscribe_to_topics(Topic topics[]) {
        // (4/7/16): OFF
        // this.mqtt().subscribe(topics);
        
        // (4/7/16): subscribe to each topic individually
        for(int i=0;i<topics.length;++i) {
            Topic[] single_topic = new Topic[1];
            single_topic[0] = topics[i];
            this.mqtt().subscribe(single_topic);
        }
    }
    
    // does this endpoint already have registered subscriptions?
    private boolean hasSubscriptions(String ep_name) {
        try {
            if (this.m_watson_iot_endpoints.get(ep_name) != null) {
                HashMap<String,Object> topic_data = (HashMap<String,Object>)this.m_watson_iot_endpoints.get(ep_name);
                if (topic_data != null && topic_data.size() > 0) {
                    return true;
                }
            }
        }
        catch (Exception ex) {
            //silent
        }
        return false;
    }
    
    // register topics for CoAP commands
    private void subscribe(String ep_name,String ep_type) {
        if (ep_name != null) {
            // DEBUG
            this.orchestrator().errorLogger().info("Watson IoT: Subscribing to CoAP command topics for endpoint: " + ep_name);
            try {
                HashMap<String,Object> topic_data = this.createEndpointTopicData(ep_name,ep_type);
                if (topic_data != null) {
                    // get,put,post,delete enablement
                    this.m_watson_iot_endpoints.put(ep_name, topic_data);
                    this.subscribe_to_topics((Topic[])topic_data.get("topic_list"));
                }
                else {
                    this.orchestrator().errorLogger().warning("Watson IoT: GET/PUT/POST/DELETE topic data NULL. GET/PUT/POST/DELETE disabled");
                }
            }
            catch (Exception ex) {
                this.orchestrator().errorLogger().info("Watson IoT: Exception in subscribe for " + ep_name + " : " + ex.getMessage());
            }
        }
        else {
            this.orchestrator().errorLogger().info("Watson IoT: NULL Endpoint name in subscribe()... ignoring...");
        }
    }
    
    // un-register topics for CoAP commands
    private boolean unsubscribe(String ep_name) {
        boolean do_register = false;
        if (ep_name != null) {
            // DEBUG
            this.orchestrator().errorLogger().info("Watson IoT: Un-Subscribing to CoAP command topics for endpoint: " + ep_name);
            try {
                HashMap<String,Object> topic_data = (HashMap<String,Object>)this.m_watson_iot_endpoints.get(ep_name);
                if (topic_data != null) {
                    // unsubscribe...
                    this.mqtt().unsubscribe((String[])topic_data.get("topic_string_list"));
                } 
                else {
                    // not in subscription list (OK)
                    this.orchestrator().errorLogger().info("Watson IoT: Endpoint: " + ep_name + " not in subscription list (OK).");
                    do_register = true;
                }
            }
            catch (Exception ex) {
                this.orchestrator().errorLogger().info("Watson IoT: Exception in unsubscribe for " + ep_name + " : " + ex.getMessage());
            }
        }
        else {
            this.orchestrator().errorLogger().info("Watson IoT: NULL Endpoint name in unsubscribe()... ignoring...");
        }
        
        // clean up
        if (ep_name != null) this.m_watson_iot_endpoints.remove(ep_name);
        
        // return the unsubscribe status
        return do_register;
    }
    
    // retrieve a specific element from the topic structure
    private String getTopicElement(String topic,int index) {
        String element = "";
        String[] parsed = topic.split("/");
        if (parsed != null && parsed.length > index) 
            element = parsed[index];
        return element;
    }
    
     // get the endpoint name from the MQTT topic
    private String getEndpointNameFromTopic(String topic) {
        // format: iot-2/type/mbed/id/mbed-eth-observe/cmd/put/fmt/json
        return this.getTopicElement(topic,4);
    }
    
    // get the CoAP verb from the MQTT topic
    private String getCoAPVerbFromTopic(String topic) {
        // format: iot-2/type/mbed/id/mbed-eth-observe/cmd/put/fmt/json
        return this.getTopicElement(topic, 6);
    }
    
    // get the resource URI from the message
    private String getCoAPURI(String message) {
        // expected format: { "path":"/303/0/5850", "new_value":"0", "ep":"mbed-eth-observe", "coap_verb": "get" }
        //this.errorLogger().info("getCoAPURI: payload: " + message);
        JSONParser parser = this.orchestrator().getJSONParser();
        Map parsed = this.tryJSONParse(message);
        String val =  (String)parsed.get("path");
        if (val == null || val.length() == 0) val = (String)parsed.get("resourceId");
        return val;
    }
    
    // get the resource value from the message
    private String getCoAPValue(String message) {
        // expected format: { "path":"/303/0/5850", "new_value":"0", "ep":"mbed-eth-observe", "coap_verb": "get" }
        //this.errorLogger().info("getCoAPValue: payload: " + message);
        JSONParser parser = this.orchestrator().getJSONParser();
        Map parsed = this.tryJSONParse(message);
        String val =  (String)parsed.get("new_value");
        if (val == null || val.length() == 0) {
            val = (String)parsed.get("payload");
            if (val != null) {
                // see if the value is Base64 encoded
                if (val.contains("==")) {
                    // value appears to be Base64 encoded... so decode... 
                    try {
                        val = new String(Base64.decodeBase64(val));
                    }
                    catch (Exception ex) {
                        // just use the value itself...
                        this.errorLogger().info("getCoAPValue: Exception in base64 decode",ex);
                    }
                }
            }
        }
        return val;
    }
    
    // pull the EndpointName from the message
    private String getCoAPEndpointName(String message) {
        // expected format: { "path":"/303/0/5850", "new_value":"0", "ep":"mbed-eth-observe", "coap_verb": "get" }
        //this.errorLogger().info("getCoAPValue: payload: " + message);
        JSONParser parser = this.orchestrator().getJSONParser();
        Map parsed = this.tryJSONParse(message);
        String val =  (String)parsed.get("ep");
        if (val == null || val.length() == 0) val = (String)parsed.get("deviceId");
        return val;
    }
    
    // pull the CoAP verb from the message
    private String getCoAPVerb(String message) {
        // expected format: { "path":"/303/0/5850", "new_value":"0", "ep":"mbed-eth-observe", "coap_verb": "get" }
        //this.errorLogger().info("getCoAPValue: payload: " + message);
        JSONParser parser = this.orchestrator().getJSONParser();
        Map parsed = this.tryJSONParse(message);
        String val =  (String)parsed.get("coap_verb");
        if (val == null || val.length() == 0) val = (String)parsed.get("method");
        return val;
    }
    
    // pull any mDC/mDS REST options from the message (optional)
    private String getRESTOptions(String message) {
        // expected format: { "path":"/303/0/5850", "new_value":"0", "ep":"mbed-eth-observe", "coap_verb": "get", "options":"noResp=true" }
        //this.errorLogger().info("getCoAPValue: payload: " + message);
        JSONParser parser = this.orchestrator().getJSONParser();
        Map parsed = this.tryJSONParse(message);
        return (String)parsed.get("options");
    }
    
    // CoAP command handler - processes CoAP commands coming over MQTT channel
    @Override
    public void onMessageReceive(String topic, String message) {
        // DEBUG
        this.errorLogger().info("Watson IoT(CoAP Command): Topic: " + topic + " message: " + message);
        
        // parse the topic to get the endpoint and CoAP verb
        // format: iot-2/type/mbed/id/mbed-eth-observe/cmd/put/fmt/json
        String ep_name = this.getEndpointNameFromTopic(topic);
        
        // pull the CoAP URI and Payload from the message itself... its JSON... 
        // format: { "path":"/303/0/5850", "new_value":"0", "ep":"mbed-eth-observe", "coap_verb": "get" }
        String uri = this.getCoAPURI(message);
        String value = this.getCoAPValue(message);
        
        // pull the CoAP verb from the message itself... its JSON... (PRIMARY)
        // format: { "path":"/303/0/5850", "new_value":"0", "ep":"mbed-eth-observe", "coap_verb": "get" }
        String coap_verb = this.getCoAPVerb(message);
        if (coap_verb == null || coap_verb.length() == 0) {
            // optionally pull the CoAP verb from the MQTT Topic (SECONDARY)
            coap_verb = this.getCoAPVerbFromTopic(topic);
        }
        
        // if the ep_name is wildcarded... get the endpoint name from the JSON payload
        // format: { "path":"/303/0/5850", "new_value":"0", "ep":"mbed-eth-observe", "coap_verb": "get" }
        if (ep_name == null || ep_name.length() <= 0 || ep_name.equalsIgnoreCase("+")) {
            ep_name = this.getCoAPEndpointName(message);
        }
        
        // if there are mDC/mDS REST options... lets add them
        // format: { "path":"/303/0/5850", "new_value":"0", "ep":"mbed-eth-observe", "coap_verb": "get", "options":"noResp=true" }
        String options = this.getRESTOptions(message);
        
        // dispatch the coap resource operation request (GET,PUT,POST,DELETE handled here)
        String response = this.orchestrator().processEndpointResourceOperation(coap_verb,ep_name,uri,value,options);
        
        // examine the response
        if (response != null && response.length() > 0) {
            // SYNC: We only process AsyncResponses from GET verbs... we dont sent HTTP status back through WatsonIoT.
            this.errorLogger().info("Watson IoT(CoAP Command): Response: " + response);
            
            // AsyncResponse detection and recording...
            if (this.isAsyncResponse(response) == true) {
                // CoAP GET and PUT provides AsyncResponses...
                if (coap_verb.equalsIgnoreCase("get") == true || coap_verb.equalsIgnoreCase("put") == true) {
                    // its an AsyncResponse.. so record it...
                    this.recordAsyncResponse(response,coap_verb,this.mqtt(),this,topic,message,ep_name,uri);
                }
                else {
                    // we ignore AsyncResponses to PUT,POST,DELETE
                    this.errorLogger().info("Watson IoT(CoAP Command): Ignoring AsyncResponse for " + coap_verb + " (OK).");
                }
            }
            else if (coap_verb.equalsIgnoreCase("get")) {
                // not an AsyncResponse... so just emit it immediately... only for GET...
                this.errorLogger().info("Watson IoT(CoAP Command): Response: " + response + " from GET... creating observation...");
                
                // we have to format as an observation...
                String observation = this.createObservation(coap_verb,ep_name,uri,response);
                
                // DEBUG
                this.errorLogger().info("Watson IoT(CoAP Command): Sending Observation(GET): " + observation);
                
                // send the observation (GET reply)...
                if (this.mqtt() != null) {
                    String reply_topic = this.customizeTopic(this.m_watson_iot_observe_notification_topic,ep_name,this.m_watson_iot_device_manager.getDeviceType(ep_name));
                    reply_topic = reply_topic.replace(this.m_observation_type,this.m_async_response_type);
                    boolean status = this.mqtt().sendMessage(reply_topic,observation,QoS.AT_MOST_ONCE); 
                    if (status == true) {
                        // success
                        this.errorLogger().info("Watson IoT(CoAP Command): CoAP observation(get) sent. SUCCESS");
                    }
                    else {
                        // send failed
                        this.errorLogger().warning("Watson IoT(CoAP Command): CoAP observation(get) not sent. SEND FAILED");
                    }
                }
                else {
                    // not connected
                    this.errorLogger().warning("Watson IoT(CoAP Command): CoAP observation(get) not sent. NOT CONNECTED");
                }
            }
        }
    }
    
    // create an observation JSON as a response to a GET request...
    private String createObservation(String verb, String ep_name, String uri, String value) {
        Map notification = new HashMap<>();
        
        // needs to look like this:  {"d":{"path":"/303/0/5700","payload":"MjkuNzU\u003d","max-age":"60","ep":"350e67be-9270-406b-8802-dd5e5f20ansond","value":"29.75"}}    
        notification.put("value", value);
        notification.put("path", uri);
        notification.put("ep",ep_name);
        
        // add a new field to denote its a GET
        notification.put("coap_verb",verb);

        // we will send the raw CoAP JSON... WatsonIoT can parse that... 
        String coap_raw_json = this.jsonGenerator().generateJson(notification);

        // strip off []...
        String coap_json_stripped = this.stripArrayChars(coap_raw_json);

        // encapsulate into a coap/device packet...
        String iotf_coap_json = coap_json_stripped;
        if (this.m_watson_iot_device_data_key != null && this.m_watson_iot_device_data_key.length() > 0) {
            iotf_coap_json = "{ \"" + this.m_watson_iot_device_data_key + "\":" + coap_json_stripped + "}";
        }

        // DEBUG
        this.errorLogger().info("Watson IoT: CoAP notification(" + verb + " REPLY): " + iotf_coap_json);
        
        // return the WatsonIoT-specific observation JSON...
        return iotf_coap_json;
    }
    
    // default formatter for AsyncResponse replies
    @Override
    public String formatAsyncResponseAsReply(Map async_response,String verb) {
        // DEBUG
        this.errorLogger().info("Watson IoT(" + verb + ") AsyncResponse: " + async_response);
        
        if (verb != null && verb.equalsIgnoreCase("GET") == true) {           
            try {
                // DEBUG
                this.errorLogger().info("Watson IoT: CoAP AsyncResponse for GET: " + async_response);

                // get the payload from the ith entry
                String payload = (String)async_response.get("payload");
                if (payload != null) {
                    // trim 
                    payload = payload.trim();

                    // parse if present
                    if (payload.length() > 0) {
                        // Base64 decode
                        String value = Utils.decodeCoAPPayload(payload);

                        // build out the response
                        String uri = this.getURIFromAsyncID((String)async_response.get("id"));
                        String ep_name = this.getEndpointNameFromAsyncID((String)async_response.get("id"));

                        // build out the observation
                        String message = this.createObservation(verb, ep_name, uri, value);

                        // DEBUG
                        this.errorLogger().info("Watson IoT: Created(" + verb + ") GET observation: " + message);

                        // return the message
                        return message;
                    }
                }
            }
            catch (Exception ex) {
                // Error in creating the observation message from the AsyncResponse GET reply... 
                this.errorLogger().warning("Watson IoT(GET): Exception in formatAsyncResponseAsReply(): ",ex);
            }
        }
        
        // Handle AsyncReplies that are CoAP PUTs
        if (verb != null && verb.equalsIgnoreCase("PUT") == true) {
            try {    
                // check to see if we have a payload or not... 
                String payload = (String)async_response.get("payload");
                if (payload != null) {
                    // trim 
                    payload = payload.trim();

                    // parse if present
                    if (payload.length() > 0) {
                        // Base64 decode
                        String value = Utils.decodeCoAPPayload(payload);

                        // build out the response
                        String uri = this.getURIFromAsyncID((String)async_response.get("id"));
                        String ep_name = this.getEndpointNameFromAsyncID((String)async_response.get("id"));

                        // build out the observation
                        String message = this.createObservation(verb, ep_name, uri, value);
                        
                        // DEBUG
                        this.errorLogger().info("Watson IoT: Created(" + verb + ") PUT Observation: " + message);

                        // return the message
                        return message;
                    }
                }
                else {
                    // no payload... so we simply return the async-id
                    String value = (String)async_response.get("async-id");
                    
                    // build out the response
                    String uri = this.getURIFromAsyncID((String)async_response.get("id"));
                    String ep_name = this.getEndpointNameFromAsyncID((String)async_response.get("id"));

                    // build out the observation
                    String message = this.createObservation(verb, ep_name, uri, value);
                    
                    // DEBUG
                    this.errorLogger().info("Watson IoT: Created(" + verb + ") PUT Observation: " + message);

                    // return message
                    return message;
                }
            }
            catch (Exception ex) {
                // Error in creating the observation message from the AsyncResponse PUT reply... 
                this.errorLogger().warning("Watson IoT(PUT): Exception in formatAsyncResponseAsReply(): ",ex);
            }
        }
        
        // return null message
        return null;
    }
    
    // process new device registration
    @Override
    protected Boolean registerNewDevice(Map message) {
        if (this.m_watson_iot_device_manager != null) {
            return this.m_watson_iot_device_manager.registerNewDevice(message);
        }
        return false;
    }
    
    // process device de-registration
    @Override
    protected Boolean deregisterDevice(String device) {
        if (this.m_watson_iot_device_manager != null) {
            return this.m_watson_iot_device_manager.deregisterDevice(device);
        }
        return false;
    }
    
    // AsyncResponse response processor
    @Override
    public boolean processAsyncResponse(Map endpoint) { 
        // DEBUG
        this.errorLogger().info("Watson IoT: Completing Creating New Device: " + endpoint);
        
        // with the attributes added, we finally create the device in Watson IoT
        this.completeNewDeviceRegistration(endpoint);
        
        // return our processing status
        return true;
    }
    
    // discover the endpoint attributes
    private void retrieveEndpointAttributes(Map endpoint) {
        // DEBUG
        this.errorLogger().info("Watson IoT: Creating New Device: " + endpoint);
        
        // pre-populate the new endpoint with initial values for registration
        this.orchestrator().pullDeviceMetadata(endpoint,this); 
    }
    
    // complete processing of adding the new device
    private void completeNewDeviceRegistration(Map endpoint) {
        try {
            // create the device in WatsonIoT
            this.errorLogger().info("Watson IoT: completeNewDeviceRegistration: calling registerNewDevice(): " + endpoint);
            this.registerNewDevice(endpoint);
            this.errorLogger().info("Watson IoT: completeNewDeviceRegistration: registerNewDevice() completed");
        }
        catch (Exception ex) {
            this.errorLogger().warning("Watson IoT: completeNewDeviceRegistration: caught exception in registerNewDevice(): " + endpoint,ex); 
        }

        try {
            // subscribe for WatsonIoT as well..
            this.errorLogger().info("Watson IoT: completeNewDeviceRegistration: calling subscribe(): " + endpoint);
            this.subscribe((String)endpoint.get("ep"),(String)endpoint.get("ept"));
            this.errorLogger().info("Watson IoT: completeNewDeviceRegistration: subscribe() completed");
        }
        catch (Exception ex) {
            this.errorLogger().warning("Watson IoT: completeNewDeviceRegistration: caught exception in registerNewDevice(): " + endpoint,ex); 
        }
    }
}
