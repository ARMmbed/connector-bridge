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
        this.initTopicRoot();
        
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
    
    // get our defaulted reply topic
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
    
    private void initTopicRoot() {
        this.m_topic_root = this.preferences().valueOf("mqtt_mds_topic_root",this.m_suffix);
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
        String topic = null;
        // DEBUG
        //this.errorLogger().info("processMDSMessage(STD)...");
        
        // get the list of parsed notifications
        List notifications = (List)data.get("notifications");
        for(int i=0;notifications != null && i<notifications.size();++i) {
            Map notification = (Map)notifications.get(i);
            
            // parse the payload 
            String b64_payload = (String)notification.get("payload");
            String parsed_payload = Utils.decodeCoAPPayload(b64_payload);
                                   
            // send it as JSON over the observation sub topic
            topic = this.getTopicRoot() + this.getDomain() + "/endpoints/" + notification.get("ep") + notification.get("path") + "/observation";
            
            // add a "value" pair with the parsed payload as a string
            notification.put("value", parsed_payload);
                        
            // we will send the raw CoAP JSON... IoTF can parse that... 
            String coap_raw_json = this.jsonGenerator().generateJson(notification);
            
            // strip off []...
            String coap_json_stripped = this.stripArrayChars(coap_raw_json);
            
            // encapsulate into a coap/device packet...
            String coap_json = coap_json_stripped;
            if (this.m_device_data_key != null && this.m_device_data_key.length() > 0) {
                coap_json = "{ \"" + this.m_device_data_key + "\":" + coap_json_stripped + "}";
            }
                       
            // DEBUG
            this.errorLogger().info("processNotification(MQTT-STD): CoAP notification: " + coap_json);
            
            // send to MQTT...
            this.mqtt().sendMessage(topic, coap_json);
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
        String topic = this.getTopicRoot() + this.getDomain() + "/endpoints";
        List endpoints = (List)data.get(key);
        for(int i=0;endpoints != null && i<endpoints.size();++i) {
            Map endpoint = (Map)endpoints.get(i);
            
            // mimick the message that we get from direct discovery...
            String message = "[{\"name\":\"" + endpoint.get("ep") + "\",\"type\":\"" + endpoint.get("ept") + "\",\"status\":\"ACTIVE\"}]";
            
            // DEBUG
            this.errorLogger().info("processNewRegistration(MQTT-STD) : Publishing new registration topic: " + topic + " message:" + message);
            this.mqtt().sendMessage(topic, message);
            
            // send it also raw... over a subtopic
            topic = topic + "/new_registration";
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
    
    // MQTT: messages from MQTT come here and are processed...
    @Override
    public void onMessageReceive(String topic, String message) {
        String verb = "PUT";
        
        // DEBUG
        this.errorLogger().info("GenericMQTT(CoAP Command): Topic: " + topic + " message: " + message);
        
        // Endpoint Discovery....
        if (this.isEndpointDiscovery(topic) && this.isNotDomainEndpointsOnly(topic) == false) {
            Map options = (Map)this.parseJson(message);
            String json = this.orchestrator().performDeviceDiscovery(options);
            if (json != null && json.length() > 0) {
                String response_topic = this.stripRequestTAG(topic);
                this.mqtt().sendMessage(response_topic, json);
            }
        }
        
        // Get/Put Endpoint Resource Value...
        else if (this.isEndpointResourceRequest(topic)) {
            Map options = (Map)this.parseJson(message);
            String json = null;
            
            if (options != null && this.isNotDomainEndpointsOnly(topic) == true)  {
                if (options.containsKey("new_value") == false) {
                    // GET the resource value
                    verb = "GET";
                }
                
                // perform the operation
                json = this.orchestrator().processEndpointResourceOperation(verb, this.stripRequestTAG(topic), options);
            }
            
            // send a response back if we have one...
            if (json != null && json.length() > 0) {
                // Strip the request tag
                String response_topic = this.stripRequestTAG(topic);
                
                // DEBUG
                this.errorLogger().info("onMessageReceive(MQTT-STD): sending reply for " + verb + ": " + json);
                
                // SYNC: here we have to handle AsyncResponses. if mDS returns an AsyncResponse... handle it
            
                // AsyncResponse detection and recording...
                if (this.isAsyncResponse(json) == true) {
                    if (verb.equalsIgnoreCase("get") == true || verb.equalsIgnoreCase("put") == true) {
                        // its an AsyncResponse to a GET or a PUT.. so record it... 
                        String endpoint = this.getElementFromTopic(topic,4);                        // topic position SENSITIVE
                        String uri = this.buildURIFromTopic(topic,endpoint);
                        this.recordAsyncResponse(json,verb,this.mqtt(),this,response_topic,message,endpoint,uri);
                    }
                    else {
                        // we dont process AsyncResponses to PUT,POST,DELETE
                        this.errorLogger().info("onMessageReceive(MQTT-STD): AsyncResponse to " + verb + " ignored (OK).");
                    }
                }
                else {
                    // not an AsyncResponse... so just emit it immediately... (GET only)
                    this.mqtt().sendMessage(response_topic, json);
                }
            }
        }
        
        // Endpoint Resource Discovery...
        else if (this.isEndpointResourcesDiscovery(topic)) {
            String json = this.orchestrator().performDeviceResourceDiscovery(this.stripRequestTAG(topic));
            if (json != null && json.length() > 0) {
                String response_topic = this.stripRequestTAG(topic);
                this.mqtt().sendMessage(response_topic, json);
            }
        }
        
        // Endpoint Notification Subscriptions
        else if (this.isEndpointNotificationSubscription(topic)) {
            Map options = (Map)this.parseJson(message);
            String json = null;
            if (options != null && options.containsKey("unsubscribe") == true) {
                // Unsubscribe 
                json = this.orchestrator().unsubscribeFromEndpointResource(this.stripRequestTAG(topic), options);
                
                // remove from the subscription list
                //this.errorLogger().info("processMessage(MQTT): TOPIC: " + topic);
                String endpoint = this.getElementFromTopic(topic,4);                        // topic position SENSITIVE
                String ep_type = this.getElementFromTopic(topic,2);                         // topic position SENSITIVE
                String uri = this.buildURIFromTopic(topic,endpoint);
                this.m_subscriptions.removeSubscription(this.m_mds_domain,endpoint,ep_type,uri);
            }
            else {
                // Subscribe
                this.errorLogger().info("processMessage(MQTT-STD): sending subscription request");
                json = this.orchestrator().subscribeToEndpointResource(this.stripRequestTAG(topic),options,true);
                
                // add to the subscription list
                String endpoint = this.getElementFromTopic(topic,4);                        // topic position SENSITIVE
                String ep_type = this.getElementFromTopic(topic,2);                         // topic position SENSITIVE
                String uri = this.buildURIFromTopic(topic,endpoint);
                
                // DEBUG
                this.errorLogger().info("processMessage(MQTT-STD): adding subscription TOPIC: " + topic + " endpoint: " + endpoint + " type: " + ep_type + " uri: " + uri);
                
                // add the subscription
                this.m_subscriptions.addSubscription(this.m_mds_domain,endpoint,ep_type,uri);
            }
            
            if (json != null && json.length() > 0) {                
                String response_topic = this.stripRequestTAG(topic);
                
                // SYNC: because we simply are processing subscription management... we can just emit the message directly...
                
                if (this.isAsyncResponse(json) == true) {
                    // its an AsyncResponse.. so record it... 
                    //this.recordAsyncResponse(json,verb,this,response_topic,message);
                    
                    // just send it...
                    this.mqtt().sendMessage(response_topic, json);
                }
                else {
                    // just send it...
                    this.mqtt().sendMessage(response_topic, json);
                }
            }
        }
    }
    
    // test to check if a topic is requesting endpoint resource subscription actions
    private boolean isEndpointNotificationSubscription(String topic) {
        boolean is_endpoint_notification_subscription = false;
        
        // simply check for "/subscriptions/"
        if (topic.contains(this.getTopicRoot()) && topic.contains("/subscriptions/")) is_endpoint_notification_subscription = true;
        
        return is_endpoint_notification_subscription;
    }
    
    // return the ith element from the topic
    private String getElementFromTopic(String topic) {
        String[] items = topic.split("/");
        return this.getElementFromTopic(topic,items.length - 1);
    }
    
    // return the ith element from the topic
    private String getElementFromTopic(String topic,int index) {
        String[] items = topic.split("/");
        return items[index];
    }
    
    private String buildURIFromTopic(String topic,String endpoint) {
        String uri = "/";
        String[] items = topic.split("/");
        boolean found = false;
        int index = 0;
        
        for(index=0;!found && index<items.length;++index) {
            if (items[index].equalsIgnoreCase(endpoint)) {
                found = true;
            }
        }
        
        for(int i=index;found && i<items.length;++i) {
            uri += items[i];
            if (i < (items.length -1)) uri += "/";
        }
        
        return uri;
    }
    
    private boolean isNotDomainEndpointsOnly(String uri) {
        String match_url = this.m_mds_domain + "/endpoints";
        return (uri.equalsIgnoreCase(match_url) == false);
    }
    
    private boolean isNotObservationRegistration(String uri) {
        if (uri != null) {
            return (uri.contains("/observation") == false && uri.contains("/new_registration") == false && this.isNotDomainEndpointsOnly(uri) == true);
        }
        return false;
    }
    
    // test to check if a topic is requesting endpoint resource itself
    private boolean isEndpointResourceRequest(String topic) {
        boolean is_endpoint_resource_request = false;
        if (this.isEndpointResourcesDiscovery(topic)) {
            // get the endpoint name
            String endpoint_name = this.getElementFromTopic(topic);
            
            // stripped topic
            String stripped_topic = this.stripRequestTAG(topic);
            
            // build our our match URL
            String match_url = this.getDomain() + "/endpoints/" + endpoint_name; // strip removes topic root...
            
            // now replace the match URL and see what's left
            String resource_uri = stripped_topic.replace(match_url, "");
            
            // see what we have
            if (resource_uri != null && resource_uri.length() > 0 && resource_uri.contains("/")) {
                if (this.isNotObservationRegistration(resource_uri) == true) {
                    is_endpoint_resource_request = true;
                
                    // DEBUG
                    this.errorLogger().info("MQTT(STD): Resource Request: Endpoint: " + endpoint_name + " Resource: " + resource_uri);
                }
            }
        }
        
        return is_endpoint_resource_request;
    }
    
    // test to check if a topic is requesting endpoint resource discovery
    private boolean isEndpointResourcesDiscovery(String topic) {
        boolean is_discovery = false;
        String stripped_topic = this.stripRequestTAG(topic);
        
        String endpoint_resource_discovery_topic = this.getDomain() + "/endpoints/"; // strip removes topic root...
        if (stripped_topic != null && stripped_topic.contains(endpoint_resource_discovery_topic)) {
            if (this.isNotObservationRegistration(stripped_topic) == true) {
                is_discovery = true;
            }
        }
        
        return is_discovery;
    }
    
    // test to check if a topic is requesting endpoint discovery
    private boolean isEndpointDiscovery(String topic) {
        boolean is_discovery = false;
        String stripped_topic = this.stripRequestTAG(topic);
        
        String endpoint_discovery_topic = this.getDomain() + "/endpoints";
        if (stripped_topic != null && stripped_topic.equalsIgnoreCase(endpoint_discovery_topic)) {
            if (this.isNotObservationRegistration(stripped_topic) == true) {
                is_discovery = true;
            }
        }
        
        return is_discovery;
    }
    
    // strip off the request TAG
    private String stripRequestTAG(String topic) {
        if (topic != null) return topic.replace(this.getTopicRoot() + this.m_mds_mqtt_request_tag, "");
        return null;
    }
    
    // response is an AsyncResponse?
    protected boolean isAsyncResponse(String response) {
        return (response.contains("\"async-response-id\":") == true);
    }
    
    // record AsyncResponses
    protected void recordAsyncResponse(String response,String coap_verb,MQTTTransport mqtt,GenericMQTTProcessor proc,String response_topic, String message, String ep_name, String uri) {
        this.asyncResponseManager().recordAsyncResponse(response, coap_verb, mqtt, proc, response_topic, this.getReplyTopic(ep_name,this.m_subscriptions.endpointTypeFromEndpointName(ep_name),response_topic), message, ep_name, uri);
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
                        String message = Utils.decodeCoAPPayload(payload);
                        
                        // DEBUG
                        this.errorLogger().info("MQTT-STD: Created(" + verb + ") GET Observation: " + message);

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
                        String message = Utils.decodeCoAPPayload(payload);
                        
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
