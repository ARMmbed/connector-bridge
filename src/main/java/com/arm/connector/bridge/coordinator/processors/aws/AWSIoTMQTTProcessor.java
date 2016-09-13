/**
 * @file    AWSIoTMQTTProcessor.java
 * @brief   AWS IoT MQTT Peer Processor
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

import com.arm.connector.bridge.coordinator.processors.arm.GenericMQTTProcessor;
import com.arm.connector.bridge.coordinator.Orchestrator;
import com.arm.connector.bridge.coordinator.processors.interfaces.AsyncResponseProcessor;
import com.arm.connector.bridge.coordinator.processors.interfaces.PeerInterface;
import com.arm.connector.bridge.core.Utils;
import com.arm.connector.bridge.transport.HttpTransport;
import com.arm.connector.bridge.transport.MQTTTransport;
import com.arm.connector.bridge.core.Transport;
import com.arm.connector.bridge.core.TransportReceiveThread;
import com.arm.connector.bridge.json.JSONParser;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.commons.codec.binary.Base64;
import org.fusesource.mqtt.client.QoS;
import org.fusesource.mqtt.client.Topic;

/**
 * AWS IoT peer processor based on MQTT
 * @author Doug Anson
 */
public class AWSIoTMQTTProcessor extends GenericMQTTProcessor implements Transport.ReceiveListener, PeerInterface, AsyncResponseProcessor {
    public static int                               NUM_COAP_VERBS = 4;                                   // GET, PUT, POST, DELETE
    public static int                               NUM_COAP_TOPICS = 1;                                  // # of MQTT Topics for CoAP verbs
    
    private String                                  m_observation_type = "observation";
    private String                                  m_async_response_type = "response";
    
    private String                                  m_aws_iot_observe_notification_topic = null;
    private String                                  m_aws_iot_coap_cmd_topic_get = null;
    private String                                  m_aws_iot_coap_cmd_topic_put = null;
    private String                                  m_aws_iot_coap_cmd_topic_post = null;
    private String                                  m_aws_iot_coap_cmd_topic_delete = null;
    
    private HashMap<String,Object>                  m_aws_iot_gw_endpoints = null;
    private HashMap<String,TransportReceiveThread>  m_mqtt_thread_list = null;
        
    // AWSIoT Device Manager
    private AWSIoTDeviceManager                     m_aws_iot_gw_device_manager = null;
        
    // constructor (singleton)
    public AWSIoTMQTTProcessor(Orchestrator manager,MQTTTransport mqtt,HttpTransport http) {
        this(manager,mqtt,null,http);
    }
    
    // constructor (with suffix for preferences)
    public AWSIoTMQTTProcessor(Orchestrator manager,MQTTTransport mqtt,String suffix,HttpTransport http) {
        super(manager,mqtt,suffix,http);
        
        // AWSIoT Processor Announce
        this.errorLogger().info("AWS IoT Processor ENABLED.");
        
        // initialize the endpoint map
        this.m_aws_iot_gw_endpoints = new HashMap<>();
        
        // initialize the listener thread map
        this.m_mqtt_thread_list = new HashMap<>();
        
        // Observation notification topic
        this.m_aws_iot_observe_notification_topic = this.orchestrator().preferences().valueOf("aws_iot_observe_notification_topic",this.m_suffix); 
        
        // initialize the topic root
        this.initTopicRoot("aws_iot_topic_root");
        
        // Send CoAP commands back through mDS into the endpoint via these Topics... 
        this.m_aws_iot_coap_cmd_topic_get = this.orchestrator().preferences().valueOf("aws_iot_coap_cmd_topic",this.m_suffix).replace("__TOPIC_ROOT__",this.getTopicRoot()).replace("__COMMAND_TYPE__","get");
        this.m_aws_iot_coap_cmd_topic_put = this.orchestrator().preferences().valueOf("aws_iot_coap_cmd_topic",this.m_suffix).replace("__TOPIC_ROOT__",this.getTopicRoot()).replace("__COMMAND_TYPE__","put");
        this.m_aws_iot_coap_cmd_topic_post = this.orchestrator().preferences().valueOf("aws_iot_coap_cmd_topic",this.m_suffix).replace("__TOPIC_ROOT__",this.getTopicRoot()).replace("__COMMAND_TYPE__","post");
        this.m_aws_iot_coap_cmd_topic_delete = this.orchestrator().preferences().valueOf("aws_iot_coap_cmd_topic",this.m_suffix).replace("__TOPIC_ROOT__",this.getTopicRoot()).replace("__COMMAND_TYPE__","delete");
                         
        // AWSIoT Device Manager - will initialize and update our AWSIoT bindings/metadata
        this.m_aws_iot_gw_device_manager = new AWSIoTDeviceManager(this.orchestrator().errorLogger(),this.orchestrator().preferences(),this.m_suffix,http,this.orchestrator());
                                             
        // initialize our MQTT transport list
        this.initMQTTTransportList();
    }
    
    // get our defaulted reply topic
    @Override
    public String getReplyTopic(String ep_name,String ep_type,String def) {
        return this.customizeTopic(this.m_aws_iot_observe_notification_topic,ep_name,ep_type).replace(this.m_observation_type, this.m_async_response_type);
    }
 
    // we have to override the creation of the authentication hash.. it has to be dependent on a given endpoint name
    @Override
    public String createAuthenticationHash() {
        return Utils.createHash(this.prefValue("aws_iot_gw_sas_token",this.m_suffix));
    }
    
    // OVERRIDE: initListener() needs to accomodate a MQTT connection for each endpoint
    @Override
    @SuppressWarnings("empty-statement")
    public void initListener() {
        // do nothing...
        ;
    }
    
    // OVERRIDE: stopListener() needs to accomodate a MQTT connection for each endpoint
    @Override
    @SuppressWarnings("empty-statement")
    public void stopListener() {
        // do nothing...
        ;
    }
    
    // Connection to AWSIoT MQTT vs. generic MQTT...
    private boolean connect(String ep_name) {
        return this.connect(ep_name,ep_name);
    }
    
    // Connection to AWSIoT MQTT vs. generic MQTT...
    private boolean connect(String ep_name,String client_id) {
        // if not connected attempt
        if (!this.isConnected(ep_name)) {
            if (this.mqtt(ep_name).connect(this.m_mqtt_host,this.m_mqtt_port,client_id,this.m_use_clean_session)) {
                this.orchestrator().errorLogger().info("AWSIoT: Setting CoAP command listener...");
                this.mqtt(ep_name).setOnReceiveListener(this);
                this.orchestrator().errorLogger().info("AWSIoT: connection completed successfully");
            }
        }
        else {
            // already connected
            this.orchestrator().errorLogger().info("AWSIoT: Already connected (OK)...");
        }
        
        // return our connection status
        this.orchestrator().errorLogger().info("AWSIoT: Connection status: " + this.isConnected(ep_name));
        return this.isConnected(ep_name);
    }
    
    // OVERRIDE: process a mDS notification for AWSIoT
    @Override
    public void processNotification(Map data) {
        // DEBUG
        //this.errorLogger().info("processNotification(AWSIoT)...");
        
        // get the list of parsed notifications
        List notifications = (List)data.get("notifications");
        for(int i=0;notifications != null && i<notifications.size();++i) {
            // we have to process the payload... this may be dependent on being a string core type... 
            Map notification = (Map)notifications.get(i);
            
            // decode the Payload...
            String b64_coap_payload = (String)notification.get("payload");
            String decoded_coap_payload = Utils.decodeCoAPPayload(b64_coap_payload);
            
            // DEBUG
            //this.errorLogger().info("AWSIoT: Decoded Payload: " + decoded_coap_payload);
            
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
            
            // get the path
            String path = (String)notification.get("path");
                        
            // we will send the raw CoAP JSON... AWSIoT can parse that... 
            String coap_raw_json = this.jsonGenerator().generateJson(notification);
            
            // strip off []...
            String coap_json_stripped = this.stripArrayChars(coap_raw_json);
            
            // get our endpoint name
            String ep_name = (String)notification.get("ep");
            
            // get our endpoint type
            String ep_type = this.getTypeFromEndpointName(ep_name);
            
            // encapsulate into a coap/device packet...
            String aws_iot_gw_coap_json = coap_json_stripped;
                                    
            // DEBUG
            this.errorLogger().info("AWSIoT: CoAP notification (STR): " + aws_iot_gw_coap_json);
            
            // send to AWSIoT...
            if (this.mqtt(ep_name) != null) {
                String topic = this.customizeTopic(this.m_aws_iot_observe_notification_topic,ep_name,ep_type) + path;
                boolean status = this.mqtt(ep_name).sendMessage(topic,aws_iot_gw_coap_json,QoS.AT_MOST_ONCE);           
                if (status == true) {
                    // not connected
                    this.errorLogger().info("AWSIoT: CoAP notification sent. SUCCESS");
                }
                else {
                    // send failed
                    this.errorLogger().warning("AWSIoT: CoAP notification not sent. SEND FAILED");
                }
            }
            else {
                // not connected
                this.errorLogger().warning("AWSIoT: CoAP notification not sent. NOT CONNECTED");
            }
        }
    }
    
    // OVERRIDE: process a re-registration in AWSIoT
    @Override
    public void processReRegistration(Map data) {
        List notifications = (List)data.get("reg-updates");
        for(int i=0;notifications != null && i<notifications.size();++i) {
            Map entry = (Map)notifications.get(i);
            // DEBUG
            // this.errorLogger().info("AWSIoT : CoAP re-registration: " + entry);
            if (this.hasSubscriptions((String)entry.get("ep")) == false) {
                // no subscriptions - so process as a new registration
                this.errorLogger().info("AWSIoT : CoAP re-registration: no subscriptions.. processing as new registration...");
                this.processRegistration(data,"reg-updates");
            }
            else {
                // already subscribed (OK)
                this.errorLogger().info("AWSIoT : CoAP re-registration: already subscribed (OK)");
            }
        }
    }
    
    // OVERRIDE: handle de-registrations for AWSIoT
    @Override
    public String[] processDeregistrations(Map parsed) {
        String[] deregistration = super.processDeregistrations(parsed);
        for(int i=0;deregistration != null && i<deregistration.length;++i) {
            // DEBUG
            this.errorLogger().info("AWSIoT : CoAP de-registration: " + deregistration[i]);
            
            // AWSIoT add-on... 
            this.unsubscribe(deregistration[i]);
            
            // Remove from AWSIoT
            this.deregisterDevice(deregistration[i]);
        }
        return deregistration;
    }
    
    // OVERRIDE: process mds registrations-expired messages 
    @Override
    public void processRegistrationsExpired(Map parsed) {
        this.processDeregistrations(parsed);
    }
    
    // OVERRIDE: process a received new registration for AWSIoT
    @Override
    public void processNewRegistration(Map data) {
        this.processRegistration(data,"registrations");
    }
    
    // OVERRIDE: process a received new registration for AWSIoT
    @Override
    protected synchronized void processRegistration(Map data,String key) {  
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
    
    // create the endpoint AWSIoT topic data
    private HashMap<String,Object> createEndpointTopicData(String ep_name,String ep_type) {
        HashMap<String,Object> topic_data = null;
        if (this.m_aws_iot_coap_cmd_topic_get != null) {
            Topic[] list = new Topic[NUM_COAP_VERBS];
            String[] topic_string_list = new String[NUM_COAP_VERBS];
            topic_string_list[0] = this.customizeTopic(this.m_aws_iot_coap_cmd_topic_get,ep_name,ep_type);
            topic_string_list[1] = this.customizeTopic(this.m_aws_iot_coap_cmd_topic_put,ep_name,ep_type);
            topic_string_list[2] = this.customizeTopic(this.m_aws_iot_coap_cmd_topic_post,ep_name,ep_type);
            topic_string_list[3] = this.customizeTopic(this.m_aws_iot_coap_cmd_topic_delete,ep_name,ep_type);
            for(int i=0;i<NUM_COAP_VERBS;++i) {
                list[i] = new Topic(topic_string_list[i],QoS.AT_LEAST_ONCE);
            }
            topic_data = new HashMap<>();
            topic_data.put("topic_list",list);
            topic_data.put("topic_string_list",topic_string_list);
            topic_data.put("ep_type",ep_type);
        }
        return topic_data;
    }
    
    // final customization of a MQTT Topic...
    private String customizeTopic(String topic,String ep_name,String ep_type) {
        String cust_topic = topic.replace("__EPNAME__", ep_name).replace("__TOPIC_ROOT__",this.m_topic_root);
        if (ep_type != null) cust_topic = cust_topic.replace("__DEVICE_TYPE__", ep_type);
        this.errorLogger().info("AWSIoT Customized Topic: " + cust_topic); 
        return cust_topic;
    }
    
    // disconnect
    private void disconnect(String ep_name) {
        if (this.isConnected(ep_name)) {
            this.mqtt(ep_name).disconnect(true);
        }
        this.remove(ep_name);
    }
    
    // are we connected
    private boolean isConnected(String ep_name) {
        if (this.mqtt(ep_name) != null) return this.mqtt(ep_name).isConnected();
        return false;
    }
    
    // subscribe to the AWSIoT MQTT topics
    private void subscribe_to_topics(String ep_name,Topic topics[]) {
        this.mqtt(ep_name).subscribe(topics);
    }
    
    // does this endpoint already have registered subscriptions?
    private boolean hasSubscriptions(String ep_name) {
        try {
            if (this.m_aws_iot_gw_endpoints.get(ep_name) != null) {
                HashMap<String,Object> topic_data = (HashMap<String,Object>)this.m_aws_iot_gw_endpoints.get(ep_name);
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
        if (ep_name != null && this.validateMQTTConnection(ep_name,ep_type)) {
            // DEBUG
            this.orchestrator().errorLogger().info("AWSIoT: Subscribing to CoAP command topics for endpoint: " + ep_name + " type: " + ep_type);
            try {
                HashMap<String,Object> topic_data = this.createEndpointTopicData(ep_name,ep_type);
                if (topic_data != null) {
                    // get,put,post,delete enablement
                    this.m_aws_iot_gw_endpoints.remove(ep_name);
                    this.m_aws_iot_gw_endpoints.put(ep_name,topic_data);
                    this.subscribe_to_topics(ep_name,(Topic[])topic_data.get("topic_list"));
                }
                else {
                    this.orchestrator().errorLogger().warning("AWSIoT: GET/PUT/POST/DELETE topic data NULL. GET/PUT/POST/DELETE disabled");
                }
            }
            catch (Exception ex) {
                this.orchestrator().errorLogger().info("AWSIoT: Exception in subscribe for " + ep_name + " : " + ex.getMessage());
            }
        }
        else {
            this.orchestrator().errorLogger().info("AWSIoT: NULL Endpoint name in subscribe()... ignoring...");
        }
    }
    
    // un-register topics for CoAP commands
    private boolean unsubscribe(String ep_name) {
        boolean unsubscribed = false;
        if (ep_name != null && this.mqtt(ep_name) != null) {
            // DEBUG
            this.orchestrator().errorLogger().info("AWSIoT: Un-Subscribing to CoAP command topics for endpoint: " + ep_name);
            try {
                HashMap<String,Object> topic_data = (HashMap<String,Object>)this.m_aws_iot_gw_endpoints.get(ep_name);
                if (topic_data != null) {
                    // unsubscribe...
                    this.mqtt(ep_name).unsubscribe((String[])topic_data.get("topic_string_list")); 
                } 
                else {
                    // not in subscription list (OK)
                    this.orchestrator().errorLogger().info("AWSIoT: Endpoint: " + ep_name + " not in subscription list (OK).");
                    unsubscribed = true;
                }
            }
            catch (Exception ex) {
                this.orchestrator().errorLogger().info("AWSIoT: Exception in unsubscribe for " + ep_name + " : " + ex.getMessage());
            }
        }
        else if (this.mqtt(ep_name) != null) {
            this.orchestrator().errorLogger().info("AWSIoT: NULL Endpoint name... ignoring unsubscribe()...");
            unsubscribed = true;
        }
        else {
            this.orchestrator().errorLogger().info("AWSIoT: No MQTT connection for " + ep_name + "... ignoring unsubscribe()...");
            unsubscribed = true;
        }  
        
        // clean up
        if (ep_name != null) this.m_aws_iot_gw_endpoints.remove(ep_name);
        
        // return the unsubscribe status
        return unsubscribed;
    }
    
    // retrieve a specific element from the topic structure
    private String getTopicElement(String topic,int index) {
        String element = "";
        String[] parsed = topic.split("/");
        if (parsed != null && parsed.length > index) 
            element = parsed[index];
        return element;
    }
    
    // get a topic element (parsed as a URL)
    @SuppressWarnings("empty-statement")
    private String getTopicElement(String topic,String key) {
        String value = null;
        
        try {
            // split by forward slash
            String tmp_slash[] = topic.split("/");
            
            // take the last element and split it again, by &
            if (tmp_slash != null && tmp_slash.length > 0) {
                String tmp_properties[] = tmp_slash[tmp_slash.length-1].split("&");
                for(int i=0;tmp_properties != null && i<tmp_properties.length && value == null;++i) {
                    String prop[] = tmp_properties[i].split("=");
                    if (prop != null && prop.length == 2 && prop[0].equalsIgnoreCase(key) == true) {
                        value = prop[1];
                    }
                }
            }
        }
        catch (Exception ex) {
            // Exception during parse
            this.errorLogger().info("WARNING: getTopicElement: Exception: " + ex.getMessage());
        }
        
        // DEBUG
        if (value != null) {
            // value found
            this.errorLogger().info("AWSIoT: getTopicElement: key: " + key + "  value: " + value);
        }
        else {
            // value not found
            this.errorLogger().info("AWSIoT: getTopicElement: key: " + key + "  value: NULL");
        }
        
        // return the value
        return value;
    }
    
    // create the URI path from the topic
    private String getURIPathFromTopic(String topic,int start_index) {
        String value = "";
        try {
            // split by forward slash
            String tmp_slash[] = topic.split("/");
            
            // we now re-assemble starting from a specific index
            for(int i=start_index;tmp_slash.length > 5 && i<tmp_slash.length;++i) {
                value = value + "/" + tmp_slash[i];
            }
        }
        catch (Exception ex) {
            // Exception during parse
            this.errorLogger().info("WARNING: getURIPathFromTopic: Exception: " + ex.getMessage());
        }
        return value;
    }
    
    // get the endpoint name from the MQTT topic
    private String getEndpointNameFromTopic(String topic) {
        // format: mbed/__COMMAND_TYPE__/__DEVICE_TYPE__/__EPNAME__/<uri path>
        return this.getTopicElement(topic,3);                                   // POSITION SENSITIVE
    }
    
    // get the CoAP verb from the MQTT topic
    private String getCoAPVerbFromTopic(String topic) {
        // format: mbed/__COMMAND_TYPE__/__DEVICE_TYPE__/__EPNAME__/<uri path>
        return this.getTopicElement(topic,1);                                   // POSITION SENSITIVE
    }
    
    // get the CoAP URI from the MQTT topic
    private String getCoAPURIFromTopic(String topic) {
        // format: mbed/__COMMAND_TYPE__/__DEVICE_TYPE__/__EPNAME__/<uri path>
        return this.getURIPathFromTopic(topic,4);                               // POSITION SENSITIVE
    }
    
    // get the resource URI from the message
    private String getCoAPURI(String message) {
        // expected format: { "path":"/303/0/5850", "new_value":"0", "ep":"mbed-eth-observe", "coap_verb": "get"}
        //this.errorLogger().info("getCoAPURI: payload: " + message);
        JSONParser parser = this.orchestrator().getJSONParser();
        Map parsed = this.tryJSONParse(message);
        String val =  (String)parsed.get("path");
        if (val == null) val = (String)parsed.get("resourceId");
        return val;
    }
    
    // get the resource value from the message
    private String getCoAPValue(String message) {
        // expected format: { "path":"/303/0/5850", "new_value":"0", "ep":"mbed-eth-observe" , "coap_verb": "get"}
        //this.errorLogger().info("getCoAPValue: payload: " + message);
        JSONParser parser = this.orchestrator().getJSONParser();
        Map parsed = this.tryJSONParse(message);
        String val =  (String)parsed.get("new_value");
        if (val == null) {
            val = (String)parsed.get("payload");
            if (val != null) {
                val = new String(Base64.decodeBase64(val));
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
        if (val == null) val = (String)parsed.get("deviceId");
        return val;
    }
    
    // pull the CoAP verb from the message
    private String getCoAPVerb(String message) {
        // expected format: { "path":"/303/0/5850", "new_value":"0", "ep":"mbed-eth-observe", "coap_verb": "get" }
        //this.errorLogger().info("getCoAPValue: payload: " + message);
        JSONParser parser = this.orchestrator().getJSONParser();
        Map parsed = this.tryJSONParse(message);
        String val =  (String)parsed.get("coap_verb");
        if (val == null) val = (String)parsed.get("method");
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
        this.errorLogger().info("AWSIoT(CoAP Command): Topic: " + topic + " message: " + message);
        
        // parse the topic to get the endpoint
        // format: mbed/__DEVICE_TYPE__/__EPNAME__/coap/__COMMAND_TYPE__/#
        String ep_name = this.getEndpointNameFromTopic(topic);
        
        // parse the topic to get the endpoint type
        String ep_type = this.getTypeFromEndpointName(ep_name);
        
        // pull the CoAP Path URI from the message itself... its JSON... 
        // format: { "path":"/303/0/5850", "new_value":"0", "ep":"mbed-eth-observe", "coap_verb": "get" }
        String uri = this.getCoAPURI(message);
        if (uri == null || uri.length() == 0) {
            // optionally pull the CoAP URI Path from the MQTT topic (SECONDARY)
            uri = this.getCoAPURIFromTopic(topic);
        }
        
        // pull the CoAP Payload from the message itself... its JSON... 
        // format: { "path":"/303/0/5850", "new_value":"0", "ep":"mbed-eth-observe", "coap_verb": "get" }
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

        // dispatch the coap resource operation request
        String response = this.orchestrator().processEndpointResourceOperation(coap_verb,ep_name,uri,value,options);
        
        // examine the response
        if (response != null && response.length() > 0) {
            // SYNC: We only process AsyncResponses from GET verbs... we dont sent HTTP status back through AWSIoT.
            this.errorLogger().info("AWSIoT(CoAP Command): Response: " + response);
            
            // AsyncResponse detection and recording...
            if (this.isAsyncResponse(response) == true) {
                // CoAP GET and PUT provides AsyncResponses...
                if (coap_verb.equalsIgnoreCase("get") == true || coap_verb.equalsIgnoreCase("put") == true) {
                    // its an AsyncResponse.. so record it...
                    this.recordAsyncResponse(response,coap_verb,this.mqtt(ep_name),this,topic,message,ep_name,uri);
                }
                else {
                    // we ignore AsyncResponses to PUT,POST,DELETE
                    this.errorLogger().info("AWSIoT(CoAP Command): Ignoring AsyncResponse for " + coap_verb + " (OK).");
                }
            }
            else if (coap_verb.equalsIgnoreCase("get")) {
                // not an AsyncResponse... so just emit it immediately... only for GET...
                this.errorLogger().info("AWSIoT(CoAP Command): Response: " + response + " from GET... creating observation...");
                
                // we have to format as an observation...
                String observation = this.createObservation(coap_verb,ep_name,uri,response);
                
                // DEBUG
                this.errorLogger().info("AWSIoT(CoAP Command): Sending Observation(GET): " + observation);
                
                // send the observation (GET reply)...
                if (this.mqtt(ep_name) != null) {
                    String reply_topic = this.customizeTopic(this.m_aws_iot_observe_notification_topic,ep_name,ep_type);
                    reply_topic = reply_topic.replace(this.m_observation_type,this.m_async_response_type);
                    boolean status = this.mqtt(ep_name).sendMessage(reply_topic,observation,QoS.AT_MOST_ONCE); 
                    if (status == true) {
                        // success
                        this.errorLogger().info("AWSIoT(CoAP Command): CoAP observation(get) sent. SUCCESS");
                    }
                    else {
                        // send failed
                        this.errorLogger().warning("AWSIoT(CoAP Command): CoAP observation(get) not sent. SEND FAILED");
                    }
                }
                else {
                    // not connected
                    this.errorLogger().warning("AWSIoT(CoAP Command): CoAP observation(get) not sent. NOT CONNECTED");
                }
            }
        }
        
        //house cleaning
        this.m_aws_iot_gw_device_manager.clearOrhpanedKeysAndCerts();
    }
    
    // create an observation JSON as a response to a GET request...
    private String createObservation(String verb, String ep_name, String uri, String value) {
        Map notification = new HashMap<>();
        
        // needs to look like this:  {"path":"/303/0/5700","payload":"MjkuNzU\u003d","max-age":"60","ep":"350e67be-9270-406b-8802-dd5e5f20ansond","value":"29.75"}    
        notification.put("value", value);
        notification.put("path", uri);
        notification.put("ep",ep_name);
        
        // add a new field to denote its a GET
        notification.put("coap_verb",verb);

        // we will send the raw CoAP JSON... AWSIoT can parse that... 
        String coap_raw_json = this.jsonGenerator().generateJson(notification);

        // strip off []...
        String coap_json_stripped = this.stripArrayChars(coap_raw_json);

        // encapsulate into a coap/device packet...
        String aws_iot_gw_coap_json = coap_json_stripped;

        // DEBUG
        this.errorLogger().info("AWSIoT: CoAP notification(" + verb + " REPLY): " + aws_iot_gw_coap_json);
        
        // return the AWSIoT-specific observation JSON...
        return aws_iot_gw_coap_json;
    }
    
    // default formatter for AsyncResponse replies
    @Override
    public String formatAsyncResponseAsReply(Map async_response,String verb) {
        // DEBUG
        this.errorLogger().info("AWSIoT(" + verb + ") AsyncResponse: " + async_response);
        
        if (verb != null && verb.equalsIgnoreCase("GET") == true) {           
            try {
                // DEBUG
                this.errorLogger().info("AWSIoT: CoAP AsyncResponse for GET: " + async_response);
                
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
                        this.errorLogger().info("AWSIoT: Created(" + verb + ") GET observation: " + message);

                        // return the message
                        return message;
                    }
                }
            }
            catch (Exception ex) {
                // Error in creating the observation message from the AsyncResponse GET reply... 
                this.errorLogger().warning("AWSIoT(GET): Exception in formatAsyncResponseAsReply(): ",ex);
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
                        this.errorLogger().info("AWSIoT: Created(" + verb + ") PUT Observation: " + message);

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
                    this.errorLogger().info("AWSIoT: Created(" + verb + ") PUT Observation: " + message);

                    // return message
                    return message;
                }
            }
            catch (Exception ex) {
                // Error in creating the observation message from the AsyncResponse PUT reply... 
                this.errorLogger().warning("AWSIoT(PUT): Exception in formatAsyncResponseAsReply(): ",ex);
            }
        }
        
         // return null message
        return null;
    }
    
    // validate the MQTT Connection
    private synchronized boolean validateMQTTConnection(String ep_name,String ep_type) {        
        // create a MQTT connection for this endpoint... 
        this.createAndStartMQTTForEndpoint(ep_name,ep_type);
        
        // return our connection status
        return this.isConnected(ep_name);
    }
    
    // process new device registration
    @Override
    protected synchronized Boolean registerNewDevice(Map message) {
        if (this.m_aws_iot_gw_device_manager != null) {
            // DEBUG
            this.errorLogger().info("AWSIoT: Registering new device: " + (String)message.get("ep") + " type: " + (String)message.get("ept"));
        
            // create the device in AWSIoT
            Boolean success = this.m_aws_iot_gw_device_manager.registerNewDevice(message);
            
            // if successful, validate (i.e. add...) an MQTT Connection
            if (success == true) this.validateMQTTConnection((String)message.get("ep"),(String)message.get("ept"));
            
            // return status
            return success;
        }
        return false;
    }
    
    // process device de-registration
    @Override
    protected synchronized Boolean deregisterDevice(String device) {
        if (this.m_aws_iot_gw_device_manager != null) {
            // DEBUG
            this.errorLogger().info("deregisterDevice(AWSIoT): deregistering device: " + device);
            
            // disconnect, remove the threaded listener... 
            if (this.m_mqtt_thread_list.get(device) != null) {
                try {
                    this.m_mqtt_thread_list.get(device).disconnect();
                } 
                catch (Exception ex) {
                    // note but continue...
                    this.errorLogger().warning("deregisterDevice(AWSIoT): exception during deregistration",ex);
                }
                this.m_mqtt_thread_list.remove(device);
            }
            
            // also remove MQTT Transport instance too...
            this.disconnect(device);
            
            // remove the device from AWSIoT
            if (this.m_aws_iot_gw_device_manager.deregisterDevice(device) == false) {
                this.errorLogger().warning("deregisterDevice(AWSIoT): unable to de-register device from AWSIoT...");
            }
        }
        return true;
    }
    
    // add a MQTT transport for a given endpoint - this is how MS AWSIoT MQTT integration works... 
    private synchronized void createAndStartMQTTForEndpoint(String ep_name,String ep_type) {
        try {
            // we may already have a connection established for this endpoint... if so, we just ignore...
            if (this.mqtt(ep_name) == null) {
                // no connection exists already... so... go get our endpoint details
                HashMap<String,String> ep = this.m_aws_iot_gw_device_manager.getEndpointDetails(ep_name);
                if (ep != null) {
                    // create a new MQTT Transport instance for our endpoint
                    MQTTTransport mqtt = new MQTTTransport(this.errorLogger(),this.preferences());
                    if (mqtt != null) {
                        // AWSIoT only works with PKI
                        mqtt.enablePKI(ep.get("PrivateKey"),ep.get("PublicKey"),ep.get("certificatePem"),ep.get("thingName"));

                        // set the AWSIoT endpoint address
                        this.m_mqtt_host = ep.get("endpointAddress");

                        // ClientID is the endpoint name
                        String client_id = ep_name;

                        // DEBUG override for testing internally... do not enable
                        //if (this.prefBoolValue("mqtt_debug_internal") == true) {
                        //    this.m_mqtt_host = "192.168.1.213";
                        //    client_id = null;
                        //    mqtt.useUserPass();
                        //     mqtt.enableMQTTVersionSet(false);
                        //}

                        // add it to the list indexed by the endpoint name... not the clientID...
                        this.addMQTTTransport(ep_name,mqtt);

                        // DEBUG
                        this.errorLogger().info("AWSIoT: connecting to MQTT for endpoint: " + ep_name + " type: " + ep_type + "...");

                        // connect and start listening... 
                        if (this.connect(ep_name,client_id) == true) {
                            // DEBUG
                            this.errorLogger().info("AWSIoT: connected to MQTT. Creating and registering listener Thread for endpoint: " + ep_name + " type: " + ep_type);

                            // ensure we only have 1 thread/endpoint
                            if (this.m_mqtt_thread_list.get(ep_name) != null) {
                                TransportReceiveThread listener = (TransportReceiveThread)this.m_mqtt_thread_list.get(ep_name);
                                listener.disconnect();
                                this.m_mqtt_thread_list.remove(ep_name);
                            }

                            // create and start the listener
                            TransportReceiveThread listener = new TransportReceiveThread(mqtt);
                            listener.setOnReceiveListener(this);
                            this.m_mqtt_thread_list.put(ep_name,listener);
                            listener.start();
                        } 
                        else {
                            // unable to connect!
                            this.errorLogger().critical("AWSIoT: Unable to connect to MQTT for endpoint: " + ep_name + " type: " + ep_type);
                            this.remove(ep_name);

                            // ensure we only have 1 thread/endpoint
                            if (this.m_mqtt_thread_list.get(ep_name) != null) {
                                TransportReceiveThread listener = (TransportReceiveThread)this.m_mqtt_thread_list.get(ep_name);
                                listener.disconnect();
                                this.m_mqtt_thread_list.remove(ep_name);
                            }
                        }
                    }
                    else {
                        // unable to allocate MQTT connection for our endpoint
                        this.errorLogger().critical("AWSIoT: ERROR. Unable to allocate MQTT connection for: " + ep_name);
                    }
                }
                else {
                    // unable to find endpoint details
                    this.errorLogger().warning("AWSIoT: unable to find endpoint details for: " + ep_name + "... ignoring...");
                }
            }
            else {
                // already connected... just ignore
                this.errorLogger().info("AWSIoT: already have connection for " + ep_name + " (OK)");
            }
        }
        catch (Exception ex) {
            // exception caught... capture and note the stack trace
            this.errorLogger().critical("AWSIoT: createAndStartMQTTForEndpoint(): exception: " + ex.getMessage() + " endpoint: "+ ep_name,ex);
        }
    }
    
    // get the endpoint type from the endpoint name
    private String getTypeFromEndpointName(String ep_name) {
        String ep_type = null;
        
        HashMap<String,Object> entry = (HashMap<String,Object>)this.m_aws_iot_gw_endpoints.get(ep_name);
        if (entry != null) {
            ep_type = (String)entry.get("ep_type");
        }
        
        return ep_type;
    }

    // AsyncResponse response processor
    @Override
    public boolean processAsyncResponse(Map endpoint) {        
        // with the attributes added, we finally create the device in Watson IoT
        this.completeNewDeviceRegistration(endpoint);
        
        // return our processing status
        return true;
    }
    
    // discover the endpoint attributes
    private void retrieveEndpointAttributes(Map endpoint) {
        // DEBUG
        this.errorLogger().info("AWSIoT: Requesting Device Metadata for: " + endpoint);
        
        // pre-populate the new endpoint with initial values for registration
        this.orchestrator().pullDeviceMetadata(endpoint,this); 
    }
    
    // complete processing of adding the new device
    private void completeNewDeviceRegistration(Map endpoint) {
        try {
            // create the device in AWSIoT
            this.errorLogger().info("completeNewDeviceRegistration: calling registerNewDevice(): " + endpoint);
            this.registerNewDevice(endpoint);
            this.errorLogger().info("completeNewDeviceRegistration: registerNewDevice() completed");
        }
        catch (Exception ex) {
            this.errorLogger().warning("completeNewDeviceRegistration: caught exception in registerNewDevice(): " + endpoint,ex); 
        }

        try {
            // subscribe for AWSIoT as well..
            this.errorLogger().info("completeNewDeviceRegistration: calling subscribe(): " + endpoint);
            this.subscribe((String)endpoint.get("ep"),(String)endpoint.get("ept"));
            this.errorLogger().info("completeNewDeviceRegistration: subscribe() completed");
        }
        catch (Exception ex) {
            this.errorLogger().warning("completeNewDeviceRegistration: caught exception in subscribe(): " + endpoint,ex); 
        }
    }
}
