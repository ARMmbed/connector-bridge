/**
 * @file    GenericMQTTProcessor.java
 * @brief   Generic MQTT peer processor for connector bridge 
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

package com.arm.connector.bridge.coordinator.processors.arm;

import com.arm.connector.bridge.coordinator.processors.core.AsyncResponseManager;
import com.arm.connector.bridge.coordinator.Orchestrator;
import com.arm.connector.bridge.coordinator.processors.core.Processor;
import com.arm.connector.bridge.coordinator.processors.core.SubscriptionList;
import com.arm.connector.bridge.coordinator.processors.interfaces.AsyncResponseProcessor;
import com.arm.connector.bridge.coordinator.processors.interfaces.PeerInterface;
import com.arm.connector.bridge.core.Utils;
import com.arm.connector.bridge.transport.HttpTransport;
import com.arm.connector.bridge.transport.MQTTTransport;
import com.arm.connector.bridge.core.Transport;
import com.arm.connector.bridge.core.TransportReceiveThread;
import com.arm.connector.bridge.core.TypeDecoder;
import com.arm.connector.bridge.json.JSONParser;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.fusesource.mqtt.client.QoS;
import org.fusesource.mqtt.client.Topic;

/**
 * Generic MQTT peer processor
 * @author Doug Anson
 */
public class GenericMQTTProcessor extends Processor implements Transport.ReceiveListener, PeerInterface {
    private String                          m_topic_root = null;
    protected SubscriptionList              m_subscriptions = null;
    protected boolean                       m_auto_subscribe_to_obs_resources = false;
    protected TransportReceiveThread        m_mqtt_thread = null;
    protected String                        m_mqtt_host = null;
    protected int                           m_mqtt_port = 0;
    private String                          m_mds_mqtt_request_tag = null;
    private String                          m_mds_topic_root = null;
    protected String                        m_suffix = null;
    private String                          m_device_data_key = null;
    protected String                        m_client_id = null;
    private HashMap<String,MQTTTransport>   m_mqtt = null;
    private AsyncResponseManager            m_async_response_manager = null;
    private HttpTransport                   m_http = null;
    protected boolean                       m_use_clean_session = false;
    private TypeDecoder                     m_type_decoder = null;
    private String                          m_qualifier = "endpoints";
    
    // constructor (singleton)
    public GenericMQTTProcessor(Orchestrator orchestrator,MQTTTransport mqtt,HttpTransport http) {
        this(orchestrator,mqtt,null,http);
    }
    
    // constructor (suffix for preferences)
    public GenericMQTTProcessor(Orchestrator orchestrator,MQTTTransport mqtt,String suffix,HttpTransport http) {
        super(orchestrator,suffix);
        
        // allocate our TypeDecoder
        this.m_type_decoder = new TypeDecoder(orchestrator.errorLogger(),orchestrator.preferences());
        
        // allocate our AsyncResponse orchestrator
        this.m_async_response_manager = new AsyncResponseManager(orchestrator);
        
        // set our domain
        this.m_mds_domain = orchestrator.getDomain();
        
        // HTTP support if we need it
        this.m_http = http;
        
        // MQTT transport list
        this.m_mqtt = new HashMap<>(); 
        
        // our suffix
        this.m_suffix = suffix;
        
        // Get the device data key if one exists
        this.m_device_data_key = orchestrator.preferences().valueOf("mqtt_device_data_key",this.m_suffix);
        
        // build out our configuration
        this.m_mqtt_host = orchestrator.preferences().valueOf("mqtt_address",this.m_suffix);
        this.m_mqtt_port = orchestrator.preferences().intValueOf("mqtt_port",this.m_suffix);
        
        // clean session
        this.m_use_clean_session = this.orchestrator().preferences().booleanValueOf("mqtt_clean_session",this.m_suffix);
        
        // MDS MQTT Request TAG
        this.m_mds_mqtt_request_tag = orchestrator.preferences().valueOf("mds_mqtt_request_tag",this.m_suffix);
        if (this.m_mds_mqtt_request_tag == null) {
            this.m_mds_mqtt_request_tag = "/request";
        }
        else {
            this.m_mds_mqtt_request_tag = "/" + this.m_mds_mqtt_request_tag;
        }
        
        // MDS topic root
        this.m_mds_topic_root = orchestrator.preferences().valueOf("mqtt_mds_topic_root",this.m_suffix);
        if (this.m_mds_topic_root == null || this.m_mds_topic_root.length() == 0) this.m_mds_topic_root = "";

        // assign our MQTT transport if we have one...
        if (mqtt != null) {
            this.m_client_id = mqtt.getClientID();
            this.addMQTTTransport(this.m_client_id, mqtt);
        }
        
        // initialize subscriptions
        this.m_subscriptions = new SubscriptionList(orchestrator.errorLogger(),orchestrator.preferences());
        
        // initialize the topic root
        this.initTopicRoot("mqtt_mds_topic_root");
        
        // auto-subscribe behavior
        this.m_auto_subscribe_to_obs_resources = orchestrator.preferences().booleanValueOf("mqtt_obs_auto_subscribe",this.m_suffix);
        
        // setup our MQTT listener if we have one...
        if (mqtt != null) {
            // MQTT Processor listener thread setup
            this.m_mqtt_thread = new TransportReceiveThread(this.mqtt());
            this.m_mqtt_thread.setOnReceiveListener(this);
        }
    }
    
    // Factory method for initializing the Sample 3rd Party peer
    public static GenericMQTTProcessor createPeerProcessor(Orchestrator manager,HttpTransport http) {
        return new GenericMQTTProcessor(manager,new MQTTTransport(manager.errorLogger(),manager.preferences()),http);
    }
    
    // get HTTP if needed
    protected HttpTransport http() { 
        return this.m_http;
    }
    
    // get TypeDecoder if needed
    protected TypeDecoder fundamentalTypeDecoder() {
        return this.m_type_decoder;
    }
    
    // attempt a json parse... 
    protected Map tryJSONParse(String payload) {
        HashMap<String,Object> result = new HashMap<>();
        try {
            result = (HashMap<String,Object>)this.orchestrator().getJSONParser().parseJson(payload);
            return result;
        }
        catch (Exception ex) {
            // silent
        }
        return result;
    }
    
    // get the AsyncResponseManager
    protected AsyncResponseManager asyncResponseManager() {
        return this.m_async_response_manager;
    }
    
    // record an async response to process later
    @Override
    public void recordAsyncResponse(String response,String uri,Map ep,AsyncResponseProcessor processor) {
        this.asyncResponseManager().recordAsyncResponse(response, uri, ep, processor);
    }
    
    // get our reply topic (if we specify URI, the build out the full resource response topic)
    public String getReplyTopic(String ep_name,String ep_type,String uri,String def) {
        return this.createResourceResponseTopic(ep_name, uri);
    }
    
    // get our defaulted reply topic (defaulted)
    public String getReplyTopic(String ep_name,String ep_type,String def) {
        return def;
    }
    
    /**
     * add a MQTT transport
     * @param clientID
     * @param mqtt
     */
    protected void addMQTTTransport(String clientID,MQTTTransport mqtt) {
        this.m_mqtt.put(clientID, mqtt);
    }
    
    /**
     * initialize the MQTT transport list
     */
    protected void initMQTTTransportList() {
        this.closeMQTTTransportList();
        this.m_mqtt.clear();
    }
    
    // PROTECTED: get the MQTT transport for the default clientID
    protected MQTTTransport mqtt() {
        return this.mqtt(this.m_client_id);
    }
    
    // PROTECTED: get the MQTT transport for a given clientID
    protected MQTTTransport mqtt(String clientID) {
        return this.m_mqtt.get(clientID);
    }
    
    // PROTECTED: remove MQTT Transport for a given clientID
    protected void remove(String clientID) {
        this.m_mqtt.remove(clientID);
    }
    
    // close the tranports in the list
    @SuppressWarnings("empty-statement")
    private void closeMQTTTransportList() {
        for (String key : this.m_mqtt.keySet()) {
            try {
                MQTTTransport mqtt = this.m_mqtt.get(key);
                if (mqtt != null) {
                    if (mqtt.isConnected()) {
                        mqtt.disconnect(true);
                    } 
                }
            }
            catch (Exception ex) {
                // silent
                ;
            }
        }
    }
    
    @Override
    public String createAuthenticationHash() {
        return this.mqtt().createAuthenticationHash();
    }
    
    // initialize the topic root...
    protected void initTopicRoot(String pref) {
        this.m_topic_root = this.preferences().valueOf(pref,this.m_suffix);
        if (this.m_topic_root == null || this.m_topic_root.length() == 0) this.m_topic_root = "";
    }
    
    protected String getTopicRoot() {
        if (this.m_topic_root == null) return "";
        return this.m_topic_root;
    }
    
    // OVERRIDE: Connection stock MQTT...
    protected boolean connectMQTT() {
        return this.mqtt().connect(this.m_mqtt_host,this.m_mqtt_port,null,true);
    }
    
    // OVERRIDE: Topics for stock MQTT...
    protected void subscribeToMQTTTopics() {
        String request_topic_str = this.getTopicRoot() + this.m_mds_mqtt_request_tag + this.getDomain() + "/#";
        this.errorLogger().info("subscribeToMQTTTopics(MQTT-STD): listening on REQUEST topic: " + request_topic_str);
        Topic request_topic = new Topic(request_topic_str, QoS.AT_LEAST_ONCE);       
        Topic[] topic_list = {request_topic};
        this.mqtt().subscribe(topic_list);
    }
    
    @Override
    public void initListener() {
       // connect and begin listening for requests (wildcard based on request TAG and domain)
        if (this.connectMQTT()) {
            this.subscribeToMQTTTopics();
            if (this.m_mqtt_thread != null) {
                this.m_mqtt_thread.start();
            }
        } 
    }
    
    @Override
    public void stopListener() {
        if (this.mqtt() != null) {
            this.mqtt().disconnect();
        }
    }
   
    // process a mDS notification for generic MQTT peers
    @Override
    public void processNotification(Map data) {
        // DEBUG
        //this.errorLogger().info("processMDSMessage(STD)...");
        
        // get the list of parsed notifications
        List notifications = (List)data.get("notifications");
        for(int i=0;notifications != null && i<notifications.size();++i) {
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
                                   
            // send it as JSON over the observation sub topic
            String topic = this.createObservationTopic(this.createBaseTopic(),ep_name,(String)notification.get("path"));
            
            // encapsulate into a coap/device packet...
            String coap_json = coap_json_stripped;
            if (this.m_device_data_key != null && this.m_device_data_key.length() > 0) {
                coap_json = "{ \"" + this.m_device_data_key + "\":" + coap_json_stripped + "}";
            }
                       
            // DEBUG
            this.errorLogger().info("processNotification(MQTT-STD): CoAP notification: " + coap_json);
            
            // send to MQTT...
            this.mqtt().sendMessage(topic,coap_json);
        }
    }
    
    // strip array values... not needed
    protected String stripArrayChars(String json) {
        return json.replace("[", "").replace("]","");
    }
    
    // process a re-registration
    @Override
    public void processReRegistration(Map data) {
        List notifications = (List)data.get("reg-updates");
        for(int i=0;notifications != null && i<notifications.size();++i) {
            Map endpoint = (Map)notifications.get(i);
            List resources = (List)endpoint.get("resources");
            for(int j=0;resources != null && j<resources.size();++j) {
                Map resource = (Map)resources.get(j); 
                if (this.isObservableResource(resource)) {
                    this.errorLogger().info("MQTTProcessor(MQTT-STD) : CoAP re-registration: " + endpoint + " Resource: " + resource);
                    if (this.m_subscriptions.containsSubscription(this.m_mds_domain,(String)endpoint.get("ep"),(String)endpoint.get("ept"),(String)resource.get("path")) == false) {
                        this.errorLogger().info("MQTTProcessor(MQTT-STD) : CoAP re-registering OBS resources for: " + endpoint + " Resource: " + resource);
                        this.processRegistration(data,"reg-updates");
                        this.m_subscriptions.addSubscription(this.m_mds_domain,(String)endpoint.get("ep"),(String)endpoint.get("ept"),(String)resource.get("path"));
                    }
                }
            }
        }
    }
    
    /**
     * process mDS deregistrations messages
     * @param parsed
     * @return
     */
    @Override
    public String[] processDeregistrations(Map parsed) {
        String[] deregistrations = this.parseDeRegistrationBody(parsed);
        this.orchestrator().processDeregistrations(deregistrations);
        return deregistrations;
    }
    
    // process mds registrations-expired messages 
    @Override
    public void processRegistrationsExpired(Map parsed) {
        this.processDeregistrations(parsed);
    }
    
    // get the observability of a given resource
    protected boolean isObservableResource(Map resource) {
        String obs_str = (String)resource.get("obs");
        return (obs_str != null && obs_str.equalsIgnoreCase("true"));
    }
    
    // process a received new registration
    @Override
    public void processNewRegistration(Map data) {
        this.processRegistration(data,"registrations");
    }
    
    // process a received new registration
    protected void processRegistration(Map data,String key) {
        List endpoints = (List)data.get(key);
        for(int i=0;endpoints != null && i<endpoints.size();++i) {
            Map endpoint = (Map)endpoints.get(i);
            
            // mimick the message that we get from direct discovery...
            String message = "[{\"name\":\"" + endpoint.get("ep") + "\",\"type\":\"" + endpoint.get("ept") + "\",\"status\":\"ACTIVE\"}]";
            String topic = this.createBaseTopic();
            
            // DEBUG
            this.errorLogger().info("processNewRegistration(MQTT-STD) : Publishing new registration topic: " + topic + " message:" + message);
            this.mqtt().sendMessage(topic, message);
            
            // send it also raw... over a subtopic
            topic = this.createNewRegistrationTopic();
            message = this.jsonGenerator().generateJson(endpoint);
            
            // DEBUG
            this.errorLogger().info("processNewRegistration(MQTT-STD) : Publishing new registration topic: " + topic + " message:" + message);
            this.mqtt().sendMessage(topic, message);
            
            // re-subscribe if previously subscribed to observable resources
            List resources = (List)endpoint.get("resources");
            for(int j=0;resources != null && j<resources.size();++j) {
                Map resource = (Map)resources.get(j); 
                if (this.m_subscriptions.containsSubscription(this.m_mds_domain,(String)endpoint.get("ep"),(String)endpoint.get("ept"),(String)resource.get("path"))) {
                    // re-subscribe to this resource
                    this.orchestrator().subscribeToEndpointResource((String)endpoint.get("ep"),(String)resource.get("path"),false);
                    
                    // SYNC: here we dont have to worry about Sync options - we simply dispatch the subscription to mDS and setup for it...
                    this.m_subscriptions.removeSubscription(this.m_mds_domain,(String)endpoint.get("ep"),(String)endpoint.get("ept"),(String)resource.get("path"));
                    this.m_subscriptions.addSubscription(this.m_mds_domain,(String)endpoint.get("ep"),(String)endpoint.get("ept"),(String)resource.get("path"));
                }
                else if (this.isObservableResource(resource) && this.m_auto_subscribe_to_obs_resources == true) {
                    // auto-subscribe to observable resources... if enabled.
                    this.orchestrator().subscribeToEndpointResource((String)endpoint.get("ep"),(String)resource.get("path"),false);
                    
                    // SYNC: here we dont have to worry about Sync options - we simply dispatch the subscription to mDS and setup for it...
                    this.m_subscriptions.removeSubscription(this.m_mds_domain,(String)endpoint.get("ep"),(String)endpoint.get("ept"),(String)resource.get("path"));
                    this.m_subscriptions.addSubscription(this.m_mds_domain,(String)endpoint.get("ep"),(String)endpoint.get("ept"),(String)resource.get("path"));
                }
            }            
        }
    }
    
    // parse the de-registration body
    protected String[] parseDeRegistrationBody(Map body) {
        List list = (List)body.get("de-registrations");
        if (list != null && list.size() > 0) {
            return list.toString().replace("[","").replace("]", "").replace(",", " ").split(" ");
        }
        list = (List)body.get("registrations-expired");
        if (list != null && list.size() > 0) {
            return list.toString().replace("[","").replace("]", "").replace(",", " ").split(" ");
        }
        return new String[0];
    }
    
    // get the resource value from the message
    private String getCoAPValue(String message) {
        // expected format: { "path":"/303/0/5850", "new_value":"0", "ep":"mbed-eth-observe", "coap_verb": "get" }
        //this.errorLogger().info("getCoAPValue: payload: " + message);
        JSONParser parser = this.orchestrator().getJSONParser();
        Map parsed = this.tryJSONParse(message);
        return (String)parsed.get("new_value");
    }
    
    // pull the CoAP verb from the message
    private String getCoAPVerb(String message) {
        // expected format: { "path":"/303/0/5850", "new_value":"0", "ep":"mbed-eth-observe", "coap_verb": "get" }
        //this.errorLogger().info("getCoAPValue: payload: " + message);
        JSONParser parser = this.orchestrator().getJSONParser();
        Map parsed = this.tryJSONParse(message);
        return (String)parsed.get("coap_verb");
    }
    
    // pull the EndpointName from the message
    private String getCoAPEndpointName(String message) {
        // expected format: { "path":"/303/0/5850", "new_value":"0", "ep":"mbed-eth-observe", "coap_verb": "get" }
        //this.errorLogger().info("getCoAPValue: payload: " + message);
        JSONParser parser = this.orchestrator().getJSONParser();
        Map parsed = this.tryJSONParse(message);
        return (String)parsed.get("ep");
    }
    
    // get the resource URI from the message
    private String getCoAPURI(String message) {
        // expected format: { "path":"/303/0/5850", "new_value":"0", "ep":"mbed-eth-observe", "coap_verb": "get" }
        //this.errorLogger().info("getCoAPURI: payload: " + message);
        JSONParser parser = this.orchestrator().getJSONParser();
        Map parsed = this.tryJSONParse(message);
        return (String)parsed.get("path");
    }
    
    // pull any mDC/mDS REST options from the message (optional)
    private String getRESTOptions(String message) {
        // expected format: { "path":"/303/0/5850", "new_value":"0", "ep":"mbed-eth-observe", "coap_verb": "get", "options":"noResp=true" }
        //this.errorLogger().info("getCoAPValue: payload: " + message);
        JSONParser parser = this.orchestrator().getJSONParser();
        Map parsed = this.tryJSONParse(message);
        return (String)parsed.get("options");
    }
    
    // MQTT: messages from MQTT come here and are processed...
    @Override
    public void onMessageReceive(String topic, String message) {
        String verb = "PUT";
        
        // DEBUG
        this.errorLogger().info("GenericMQTT(CoAP Command): Topic: " + topic + " message: " + message);
        
        // Endpoint Discovery....
        if (this.isEndpointDiscovery(topic)) {
            Map options = (Map)this.parseJson(message);
            String json = this.orchestrator().performDeviceDiscovery(options);
            if (json != null && json.length() > 0) {
                this.mqtt().sendMessage(topic + "/mbed_endpoints", json);
            }
        }
        
        // Get/Put/Post Endpoint Resource Value...
        else if (this.isEndpointResourceRequest(topic)) {
            String json = null;
            
            // parse the topic to get the endpoint and CoAP verb
            // format: iot-2/type/mbed/id/mbed-eth-observe/cmd/put/fmt/json
            String ep_name = this.getCoAPEndpointName(message);

            // pull the CoAP URI and Payload from the message itself... its JSON... 
            // format: { "path":"/303/0/5850", "new_value":"0", "ep":"mbed-eth-observe", "coap_verb": "get" }
            String uri = this.getCoAPURI(message);

            // pull the CoAP verb from the message itself... its JSON... (PRIMARY)
            // format: { "path":"/303/0/5850", "new_value":"0", "ep":"mbed-eth-observe", "coap_verb": "get" }
            verb = this.getCoAPVerb(message);

            // get the CoAP value to send
            String value = this.getCoAPValue(message);

            // if there are mDC/mDS REST options... lets add them
            // format: { "path":"/303/0/5850", "new_value":"0", "ep":"mbed-eth-observe", "coap_verb": "get", "options":"noResp=true" }
            String options = this.getRESTOptions(message);

            // perform the operation
            json = this.orchestrator().processEndpointResourceOperation(verb,ep_name,uri,value,options);
            
            // send a response back if we have one...
            if (json != null && json.length() > 0) {
                // Strip the request tag
                String response_topic = this.createBaseTopic() + "/" + ep_name + uri;
                                
                // SYNC: here we have to handle AsyncResponses. if mDS returns an AsyncResponse... handle it
                if (this.isAsyncResponse(json) == true) {
                    if (verb.equalsIgnoreCase("get") == true || verb.equalsIgnoreCase("put") == true) {
                        // DEBUG
                        this.errorLogger().info("onMessageReceive(MQTT-STD): saving async response (" + verb + ") on topic: " + response_topic + " value: " + json);

                        // its an AsyncResponse to a GET or PUT.. so record it... 
                        this.recordAsyncResponse(json,verb,this.mqtt(),this,response_topic,message,ep_name,uri);
                    }
                    else {
                        // we dont process AsyncResponses to POST and DELETE
                        this.errorLogger().info("onMessageReceive(MQTT-STD): AsyncResponse (" + verb + ") ignored (OK).");
                    }
                }
                else {
                    // DEBUG
                    this.errorLogger().info("onMessageReceive(MQTT-STD): sending immediate reply (" + verb + ") on topic: " + response_topic + " value: " + json);

                    // not an AsyncResponse... so just emit it immediately... (GET only)
                    this.mqtt().sendMessage(response_topic,json);
                }
            }
        }
        
        // Endpoint Resource Discovery...
        else if (this.isEndpointResourcesDiscovery(topic)) {
            String json = this.orchestrator().performDeviceResourceDiscovery(this.removeRequestTagFromTopic(topic));
            if (json != null && json.length() > 0) {
                String response_topic = this.removeRequestTagFromTopic(topic);
                this.mqtt().sendMessage(response_topic, json);
            }
        }
        
        // Endpoint Notification Subscriptions
        else if (this.isEndpointNotificationSubscriptionRequest(topic)) {
            // message format (unsubscribe): {"ep":"<endpoint id>","ept":"<endpoint type>","path":"/123/0/1234","verb":"unsubscribe"}
            // message format (unsubscribe): {"ep":"<endpoint id>","ept":"<endpoint type>","path":"/123/0/1234","verb":"subscribe"}
            Map parsed = (Map)this.parseJson(message);
            String json = null;
            String ep_name = (String)parsed.get("ep");
            String uri = (String)parsed.get("path");
            String ep_type = (String)parsed.get("ept");
            verb = (String)parsed.get("verb");
            if (parsed != null && verb.equalsIgnoreCase("unsubscribe") == true) {
                // Unsubscribe 
                this.errorLogger().info("processMessage(MQTT-STD): sending subscription request (remove subscription)");
                json = this.orchestrator().unsubscribeFromEndpointResource(this.createSubscriptionURI(ep_name,uri),parsed);
                
                // remove from the subscription list
                this.errorLogger().info("processMessage(MQTT-STD): removing subscription TOPIC: " + topic + " endpoint: " + ep_name + " type: " + ep_type + " uri: " + uri);
                this.m_subscriptions.removeSubscription(this.m_mds_domain,ep_name,ep_type,uri);
            }
            else if (parsed != null && verb.equalsIgnoreCase("subscribe") == true) {
                // Subscribe
                this.errorLogger().info("processMessage(MQTT-STD): sending subscription request (add subscription)");
                json = this.orchestrator().subscribeToEndpointResource(this.createSubscriptionURI(ep_name,uri),parsed,true);
                
                // add to the subscription list
                this.errorLogger().info("processMessage(MQTT-STD): adding subscription TOPIC: " + topic + " endpoint: " + ep_name + " type: " + ep_type + " uri: " + uri);
                this.m_subscriptions.addSubscription(this.m_mds_domain,ep_name,ep_type,uri);
            }
            else if (parsed != null) {
                // verb not recognized
                this.errorLogger().info("processMessage(MQTT-STD): Unable to process subscription request: unrecognized verb: " + verb);
            }
            else {
                // invalid message
                this.errorLogger().info("processMessage(MQTT-STD): Unable to process subscription request: invalid message: " + message);
            }
        }
    }
    
    // get the endpoint name from the topic (request topic sent) 
    // format: <topic_root>/request/endpoints/<endpoint name>/<URI>
    private String getEndpointNameFromTopic(String topic) {
        String modified_topic = this.removeRequestTagFromTopic(topic); // strips <topic_root>/request/endpoints/ 
        String[] items = modified_topic.split("/");
        if (items.length >= 1 && items[0].trim().length() > 0) {
            return items[0].trim();
        }
        return null;
    }
    
    // get the resource URI from the topic (request topic sent) 
    // format: <topic_root>/request/endpoints/<endpoint name>/<URI>
    private String getResourceURIFromTopic(String topic,String ep_name) {
        String modified_topic = this.removeRequestTagFromTopic(topic); // strips <topic_root>/request/endpoints/ 
        return modified_topic.replace(ep_name,"");
    }
   
    private boolean isNotObservationOrNewRegistration(String topic) {
        if (topic != null) {
            return (topic.contains("observation") == false && topic.contains("new_registration") == false);
        }
        return false;
    }
    
    private String createBaseTopic() {
        return this.createBaseTopic(this.m_qualifier);
    }
    
    private String createBaseTopic(String qualifier) {
        return this.getTopicRoot() + this.getDomain() + "/" + qualifier;
    }
    
    private String createNewRegistrationTopic() {
        return this.createNewRegistrationTopic(this.m_qualifier);
    }
   
    private String createNewRegistrationTopic(String qualifier) {
        return this.createBaseTopic(qualifier) + "/" + "new_registration";
    }
    
    private String createEndpointResourceRequest() {
        return this.createEndpointResourceRequest(this.m_qualifier);
    }
    
    private String createEndpointResourceRequest(String qualifier) {
        return this.createBaseTopic("request") + "/" + qualifier;
    }
    
    private String createObservationTopic(String base,String ep_name,String uri) {
        return this.createBaseTopic("observation") + "/" + ep_name + uri;
    }
    
    private String createResourceResponseTopic(String ep_name,String uri) {
        return this.createResourceResponseTopic(this.createBaseTopic(),ep_name,uri);
    }
    
    private String createResourceResponseTopic(String base,String ep_name,String uri) {
        return this.createBaseTopic("response") + "/" + ep_name + uri;
    }
    
    private String createSubscriptionURI(String ep_name,String uri) {
        return "subscriptions" + "/" + ep_name + uri;
    }
    
    // strip off the request TAG
    // mbed/request/endpoints/<endpoint>/<URI> --> <endpoint>/<URI>
    private String removeRequestTagFromTopic(String topic) {
        if (topic != null) {
            String stripped = topic.replace(this.getTopicRoot() + this.getDomain() + this.m_mds_mqtt_request_tag + "/" + this.m_qualifier + "/", "");
            this.errorLogger().info("removeRequestTagFromTopic: topic: " + topic + " stripped: " + stripped);
            return stripped;
        }
        return null;
    }
    
    // test to check if a topic is requesting endpoint resource itself
    private boolean isEndpointResourceRequest(String topic) {
        boolean is_endpoint_resource_request = false;
        if (this.isEndpointResourcesDiscovery(topic) == true) {
            // get the endpoint name
            String ep_name = this.getEndpointNameFromTopic(topic);
            
            // get the resource URI
            String resource_uri = this.getResourceURIFromTopic(topic,ep_name);
            
            // see what we have
            if (resource_uri != null && resource_uri.length() > 0) {
                if (this.isNotObservationOrNewRegistration(topic) == true) {
                    is_endpoint_resource_request = true;
                
                    // DEBUG
                    this.errorLogger().info("MQTT(STD): Resource Request: Endpoint: " + ep_name + " Resource: " + resource_uri);
                }
            }
        }
        
        // DEBUG
        this.errorLogger().info("isEndpointResourceRequest: topic: " + topic + " is: " + is_endpoint_resource_request);
        return is_endpoint_resource_request;
    }
    
    // test to check if a topic is requesting endpoint resource discovery
    private boolean isEndpointResourcesDiscovery(String topic) {
        boolean is_discovery = false;
        
        String request_topic = this.createEndpointResourceRequest();
        if (topic != null && topic.contains(request_topic) == true) {
            if (this.isNotObservationOrNewRegistration(topic) == true) {
                is_discovery = true;
            }
        }
        
        // DEBUG
        this.errorLogger().info("isEndpointResourcesDiscovery: topic: " + topic + " is: " + is_discovery);
        return is_discovery;
    }
    
    // test to check if a topic is requesting endpoint discovery
    private boolean isEndpointDiscovery(String topic) {
        boolean is_discovery = false;
        
        String request_topic = this.createEndpointResourceRequest();
        if (topic != null && topic.equalsIgnoreCase(request_topic) == true) {
            is_discovery = true;
        }
        
        // DEBUG
        this.errorLogger().info("isEndpointDiscovery: topic: " + topic + " is: " + is_discovery);
        return is_discovery;
    }
    
    // test to check if a topic is requesting endpoint resource subscription actions
    private boolean isEndpointNotificationSubscriptionRequest(String topic) {
        boolean is_endpoint_notification_subscription = false;
        
        // simply check for "request/subscriptions"
        if (topic.contains("request/subscriptions")) {
            is_endpoint_notification_subscription = true;
        }
        
        // DEBUG
        this.errorLogger().info("isEndpointNotificationSubscription: topic: " + topic + " is: " + is_endpoint_notification_subscription);
        return is_endpoint_notification_subscription;
    }
    
    // response is an AsyncResponse?
    protected boolean isAsyncResponse(String response) {
        return (response.contains("\"async-response-id\":") == true);
    }
    
    // record AsyncResponses
    protected void recordAsyncResponse(String response,String coap_verb,MQTTTransport mqtt,GenericMQTTProcessor proc,String response_topic, String message, String ep_name, String uri) {
        this.asyncResponseManager().recordAsyncResponse(response, coap_verb, mqtt, proc, response_topic, this.getReplyTopic(ep_name,this.m_subscriptions.endpointTypeFromEndpointName(ep_name),uri,response_topic), message, ep_name, uri);
    }
    
    // process AsyncResponses
    @Override
    public void processAsyncResponses(Map data) {
        List responses = (List)data.get("async-responses");
        for(int i=0;responses != null && i<responses.size();++i) {
            this.asyncResponseManager().processAsyncResponse((Map)responses.get(i));
        }
    }
    
    // split AsyncID
    private String[] splitAsyncID(String id) {
        String[] parts = null;
        
        if (id != null && id.length() > 0) {
            // copy the string
            String tmp = id;
            
            // loop through and remove key delimiters
            tmp = tmp.replace('#', ' ');
            tmp = tmp.replace('@', ' ');
            tmp = tmp.replace('/', ' ');
            
            // split
            parts = tmp.split(" ");
        }
        
        // return the parsed parts
        return parts;
    }
    
    // extract the URI from the async-id
    // format: 1408956550#cc69e7c5-c24f-43cf-8365-8d23bb01c707@decd06cc-2a32-4e5e-80d0-7a7c65b90e6e/303/0/5700
    protected String getURIFromAsyncID(String id) {
        String uri = null;
        
        // split
        String[] parts = this.splitAsyncID(id);
        
        // re-assemble the URI
        uri = "/";
        for(int i=3;i<parts.length;++i) {
            uri += parts[i];
            if (i < (parts.length-1)) {
                uri += "/";
            }
        }
        
        // DEBUG
        this.errorLogger().info("getURIFromAsyncID: URI: " + uri);
        
        // return the URI
        return uri;
    }
    
    // extract the endpoint name from the async-id
    // format: 1408956550#cc69e7c5-c24f-43cf-8365-8d23bb01c707@decd06cc-2a32-4e5e-80d0-7a7c65b90e6e/303/0/5700
    protected String getEndpointNameFromAsyncID(String id) {
        // split
        String[] parts = this.splitAsyncID(id);
  
        // DEBUG
        this.errorLogger().info("getEndpointNameFromAsyncID: endpoint: " + parts[1]);
        
        // return the endpoint name
        return parts[1];
    }
    
    // create the observation
    private String createObservation(String verb, String ep_name, String uri, String value) {
        Map notification = new HashMap<>();
        
        // needs to look like this:  {"path":"/303/0/5700","payload":"MjkuNzU\u003d","max-age":"60","ep":"350e67be-9270-406b-8802-dd5e5f20","value":"29.75"}    
        notification.put("value", value);
        notification.put("path", uri);
        notification.put("ep",ep_name);
        
        // add a new field to denote its a GET
        notification.put("verb",verb);

        // we will send the raw CoAP JSON... AWSIoT can parse that... 
        String coap_raw_json = this.jsonGenerator().generateJson(notification);

        // strip off []...
        String coap_json_stripped = this.stripArrayChars(coap_raw_json);
        
        // encapsulate into a coap/device packet...
        String coap_json = coap_json_stripped;

        // DEBUG
        this.errorLogger().info("MQTT-STD: CoAP notification(" + verb + " REPLY): " + coap_json);
        
        // return the generic MQTT observation JSON...
        return coap_json;
    }
    
    // default formatter for AsyncResponse replies
    public String formatAsyncResponseAsReply(Map async_response,String verb) {
        // DEBUG
        this.errorLogger().info("MQTT-STD(" + verb + ") AsyncResponse: " + async_response);

        // Handle AsyncReplies that are CoAP GETs
        if (verb != null && verb.equalsIgnoreCase("GET") == true) {
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
                        this.errorLogger().info("MQTT-STD: Created(" + verb + ") GET observation: " + message + " reply topic: " + async_response.get("reply_topic"));

                        // return the message
                        return message;
                    }
                }
                else {
                    // GET should always have a payload
                    this.errorLogger().warning("MQTT-STD (" + verb + "): GET Observation has NULL payload... Ignoring...");
                }
            }
            catch (Exception ex) {
                // Error in creating the observation message from the AsyncResponse GET reply... 
                this.errorLogger().warning("MQTT-STD(GET): Exception in formatAsyncResponseAsReply(): ",ex);
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
                        this.errorLogger().info("MQTT-STD: Created(" + verb + ") PUT Observation: " + message);

                        // return the message
                        return message;
                    }
                }
                else {
                    // no payload... so we simply return the async-id
                    String message = (String)async_response.get("async-id");
                    
                    // DEBUG
                    this.errorLogger().info("MQTT-STD: Created(" + verb + ") PUT Observation: " + message);

                    // return message
                    return message;
                }
            }
            catch (Exception ex) {
                // Error in creating the observation message from the AsyncResponse PUT reply... 
                this.errorLogger().warning("MQTT-STD(PUT): Exception in formatAsyncResponseAsReply(): ",ex);
            }
        }
        
        // return null message
        return null;
    }
    
    // process new device registration
    protected Boolean registerNewDevice(Map message) {
        // not implemented
        return false;
    }
    
    // process device re-registration
    protected Boolean reregisterDevice(Map message) {
        // not implemented
        return false;
    }
    
    // process device de-registration
    protected Boolean deregisterDevice(String device) {
        // not implemented
        return false;
    }
    
    // process device registration expired
    protected Boolean expireDeviceRegistration(String device) {
        // not implemented
        return false;
    }
}
