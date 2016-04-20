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
import com.arm.connector.bridge.coordinator.processors.interfaces.PeerInterface;
import com.arm.connector.bridge.core.Utils;
import com.arm.connector.bridge.transport.HttpTransport;
import com.arm.connector.bridge.transport.MQTTTransport;
import com.arm.connector.bridge.core.Transport;
import com.arm.connector.bridge.json.JSONParser;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.fusesource.mqtt.client.QoS;
import org.fusesource.mqtt.client.Topic;

/**
 * IBM WatsonIoT peer processor based on MQTT with MessageSight
 * @author Doug Anson
 */
public class WatsonIoTMQTTProcessor extends GenericMQTTProcessor implements Transport.ReceiveListener, PeerInterface {
    public static int               NUM_COAP_VERBS = 4;                                   // GET, PUT, POST, DELETE
    private String                  m_mqtt_ip_address = null;
    private int                     m_mqtt_port = 0;
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
    private Boolean                 m_use_clean_session = false;
    
    // WatsonIoT bindings
    private String                  m_watson_iot_api_key = null;
    private String                  m_watson_iot_auth_token = null;
    
    // RTI 
    private boolean                 m_rti_format_enable = false;
    
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
        
        // RTI
        this.m_rti_format_enable = this.orchestrator().preferences().booleanValueOf("iotf_use_rti_format",this.m_suffix);
        if (this.m_rti_format_enable) {
            this.errorLogger().info("RTI Formatting ENABLED.");
        }
        
        // starter kit supports observation notifications
        this.m_watson_iot_observe_notification_topic = this.orchestrator().preferences().valueOf("iotf_observe_notification_topic",this.m_suffix).replace("__EVENT_TYPE__","observation"); 
        
        // starter kit can send CoAP commands back through mDS into the endpoint via these Topics... 
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
        
        // WatsonIoT Device Manager - will initialize and update our WatsonIoT bindings/metadata
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
    
    // parse the WatsonIoT Username
    private void parseWatsonIoTUsername() {
        String[] elements = this.m_watson_iot_api_key.replace("-"," ").split(" ");
        if (elements != null && elements.length >= 3) {
            this.m_watson_iot_org_id = elements[1];
            this.m_watson_iot_org_key = elements[2];
            //this.errorLogger().info("WatsonIoT: org_id: " + elements[1] + " apikey: " + elements[2]);
        }
        else {
            this.errorLogger().info("WatsonIoT: unable to parse WatsonIoT Username: " + this.m_watson_iot_api_key);
        }
    }
    
    // OVERRIDE: Connection to WatsonIoT vs. stock MQTT...
    @Override
    protected boolean connectMQTT() {
        return this.mqtt().connect(this.m_mqtt_ip_address,this.m_mqtt_port,this.m_client_id,this.m_use_clean_session);
    }
    
    // OVERRIDE: (Listening) Topics for WatsonIoT vs. stock MQTT...
    @Override
    @SuppressWarnings("empty-statement")
    protected void subscribeToMQTTTopics() {
        // do nothing... WatsonIoT will have "listenable" topics for the CoAP verbs via the CMD event type...
        ;
    }
    
    // RTI
    private Map rtiFormatMessage(Map coap_message,String json) {
        Map rti_message = coap_message;
        
        // Optional Formatting for RTI
        if (this.m_rti_format_enable) {
            try {
                // parse the input json...
                Map parsed = this.jsonParser().parseJson(json);

                // Create a new message with the parsed JSON and elements of the CoAP message
                parsed.put("ep", (String)coap_message.get("ep"));
                parsed.put("path", (String)coap_message.get("path"));
                parsed.put("max-age", (String)coap_message.get("max-age"));
                
                // use the modified message
                rti_message = parsed;
            }
            catch (Exception ex) {
                // unable to parse input... so just pass through the original message
                this.errorLogger().info("rtiFormatMessage: unable to parse input JSON: " + json + "... disabling formatting...");
            }
        }
        
        return rti_message;
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
            
            // Try a JSON parse... if it succeeds, assume the payload is a composite JSON value...
            Map json_parsed = this.tryJSONParse(decoded_coap_payload);
            if (json_parsed != null) {
                // add in a JSON object payload value directly... 
                notification.put("value", json_parsed);
            }
            else {
                // add in a decoded payload value as a string type...
                notification.put("value", decoded_coap_payload);
            }
                        
            // RTI
            notification = this.rtiFormatMessage(notification,decoded_coap_payload);
                        
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
            this.errorLogger().info("WatsonIoT: CoAP notification: " + iotf_coap_json);
            //this.errorLogger().info("WatsonIoT: CoAP notification (JSON): " + notification);
            
            // send to WatsonIoT...
            this.mqtt().sendMessage(this.customizeTopic(this.m_watson_iot_observe_notification_topic,ep_name,this.m_watson_iot_device_manager.getDeviceType(ep_name)),iotf_coap_json,QoS.AT_MOST_ONCE);           
         }
    }
    
    // OVERRIDE: process a re-registration in WatsonIoT
    @Override
    public void processReRegistration(Map data) {
        List notifications = (List)data.get("reg-updates");
        for(int i=0;notifications != null && i<notifications.size();++i) {
            Map entry = (Map)notifications.get(i);
            this.errorLogger().info("WatsonIoT : CoAP re-registration: " + entry);
            boolean do_register = this.unsubscribe((String)entry.get("ep"));
            if (do_register == true) 
                this.processRegistration(data,"reg-updates");
            else 
                this.subscribe((String)entry.get("ep"),(String)entry.get("ept"));
        }
    }
    
    // OVERRIDE: handle de-registrations for WatsonIoT
    @Override
    public String[] processDeregistrations(Map parsed) {
        String[] deregistration = super.processDeregistrations(parsed);
        for(int i=0;deregistration != null && i<deregistration.length;++i) {
            // DEBUG
            this.errorLogger().info("WatsonIoT : CoAP de-registration: " + deregistration[i]);
            
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
                if (this.m_subscriptions.containsSubscription(this.m_mds_domain,(String)endpoint.get("ep"),(String)resource.get("path"))) {
                    // re-subscribe to this resource
                    this.orchestrator().subscribeToEndpointResource((String)endpoint.get("ep"),(String)resource.get("path"),false);
                    
                    // SYNC: here we dont have to worry about Sync options - we simply dispatch the subscription to mDS and setup for it...
                    this.m_subscriptions.removeSubscription(this.m_mds_domain,(String)endpoint.get("ep"),(String)resource.get("path"));
                    this.m_subscriptions.addSubscription(this.m_mds_domain,(String)endpoint.get("ep"),(String)resource.get("path"));
                }
                
                // auto-subscribe
                else if (this.isObservableResource(resource) && this.m_auto_subscribe_to_obs_resources == true) {
                    // auto-subscribe to observable resources... if enabled.
                    this.orchestrator().subscribeToEndpointResource((String)endpoint.get("ep"),(String)resource.get("path"),false);
                    
                    // SYNC: here we dont have to worry about Sync options - we simply dispatch the subscription to mDS and setup for it...
                    this.m_subscriptions.removeSubscription(this.m_mds_domain,(String)endpoint.get("ep"),(String)resource.get("path"));
                    this.m_subscriptions.addSubscription(this.m_mds_domain,(String)endpoint.get("ep"),(String)resource.get("path"));
                }
            }    
            
            // pre-populate the new endpoint with initial values for registration
            this.orchestrator().pullDeviceMetadata(endpoint);
            
            try {
                // create the device in WatsonIoT
                this.errorLogger().info("processRegistration: calling registerNewDevice(): " + endpoint);
                this.registerNewDevice(endpoint);
                this.errorLogger().info("processRegistration: registerNewDevice() completed");
            }
            catch (Exception ex) {
                this.errorLogger().warning("processRegistration: caught exception in registerNewDevice(): " + endpoint,ex); 
            }
            
            try {
                // subscribe for WatsonIoT as well..
                this.errorLogger().info("processRegistration: calling subscribe(): " + endpoint);
                this.subscribe((String)endpoint.get("ep"),(String)endpoint.get("ept"));
                this.errorLogger().info("processRegistration: subscribe() completed");
            }
            catch (Exception ex) {
                this.errorLogger().warning("processRegistration: caught exception in registerNewDevice(): " + endpoint,ex); 
            }
        }
    }
    
    // create the WatsonIoT clientID
    private String createWatsonIoTClientID(String domain) {
        int length = 12;
        if (domain == null) domain = this.prefValue("mds_def_domain",this.m_suffix);
        if (domain.length() < 12) length = domain.length();
        return this.m_client_id_template + domain.substring(0,length);  // 12 digits only of the domain
    }
    
    // create the StarterKit compatible clientID
    private String createStarterKitClientID(String domain) {
        //
        // StarterKit clientID format:  "d:<org>:<type>:<device id>"
        // Where:
        // org - "quickstart" 
        // type - we list it as "iotsample-mbed"
        // deviceID - we bring in the custom name
        //
        String device_id = this.prefValue("iotf_starterkit_device_id"); 
        return "d:quickstart:iotsample-mbed-k64f:" + device_id;
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
    
    private String customizeTopic(String topic,String ep_name,String ep_type) {
        String cust_topic = topic.replace("__EPNAME__", ep_name).replace("__DEVICE_TYPE__", ep_type);
        this.errorLogger().info("WatsonIoT Customized Topic: " + cust_topic); 
        return cust_topic;
    }
    
    // connect
    public boolean connect() {
        // if not connected attempt
        if (!this.isConnected()) {
            if (this.mqtt().connect(this.m_mqtt_ip_address, this.m_mqtt_port, this.m_client_id, true)) {
                this.orchestrator().errorLogger().info("WatsonIoT: Setting CoAP command listener...");
                this.mqtt().setOnReceiveListener(this);
                this.orchestrator().errorLogger().info("WatsonIoT: connection completed successfully");
            }
        }
        else {
            // already connected
            this.orchestrator().errorLogger().info("WatsonIoT: Already connected (OK)...");
        }
        
        // return our connection status
        this.orchestrator().errorLogger().info("WatsonIoT: Connection status: " + this.isConnected());
        return this.isConnected();
    }
    
    // disconnect
    public void disconnect() {
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
    
    // register topics for CoAP commands
    @SuppressWarnings("empty-statement")
    public void subscribe(String ep_name,String ep_type) {
        if (ep_name != null) {
            // DEBUG
            this.orchestrator().errorLogger().info("WatsonIoT: Subscribing to CoAP command topics for endpoint: " + ep_name);
            try {
                HashMap<String,Object> topic_data = this.createEndpointTopicData(ep_name,ep_type);
                if (topic_data != null) {
                    // get,put,post,delete enablement
                    this.m_watson_iot_endpoints.put(ep_name, topic_data);
                    this.subscribe_to_topics((Topic[])topic_data.get("topic_list"));
                }
                else {
                    this.orchestrator().errorLogger().warning("WatsonIoT: GET/PUT/POST/DELETE topic data NULL. GET/PUT/POST/DELETE disabled");
                }
            }
            catch (Exception ex) {
                this.orchestrator().errorLogger().info("WatsonIoT: Exception in subscribe for " + ep_name + " : " + ex.getMessage());
            }
        }
        else {
            this.orchestrator().errorLogger().info("WatsonIoT: NULL Endpoint name in subscribe()... ignoring...");
        }
    }
    
    // un-register topics for CoAP commands
    public boolean unsubscribe(String ep_name) {
        boolean do_register = false;
        if (ep_name != null) {
            // DEBUG
            this.orchestrator().errorLogger().info("WatsonIoT: Un-Subscribing to CoAP command topics for endpoint: " + ep_name);
            try {
                HashMap<String,Object> topic_data = (HashMap<String,Object>)this.m_watson_iot_endpoints.get(ep_name);
                if (topic_data != null) {
                    // unsubscribe...
                    this.mqtt().unsubscribe((String[])topic_data.get("topic_string_list"));
                } 
                else {
                    // not in subscription list (OK)
                    this.orchestrator().errorLogger().info("WatsonIoT: Endpoint: " + ep_name + " not in subscription list (OK).");
                    do_register = true;
                }
            }
            catch (Exception ex) {
                this.orchestrator().errorLogger().info("WatsonIoT: Exception in unsubscribe for " + ep_name + " : " + ex.getMessage());
            }
        }
        else {
            this.orchestrator().errorLogger().info("WatsonIoT: NULL Endpoint name in unsubscribe()... ignoring...");
        }
        return do_register;
    }
    
    private String getTopicElement(String topic,int index) {
        String element = "";
        String[] parsed = topic.split("/");
        if (parsed != null && parsed.length > index) 
            element = parsed[index];
        return element;
    }
    
    private String getEndpointNameFromTopic(String topic) {
        // format: iot-2/type/mbed/id/mbed-eth-observe/cmd/put/fmt/json
        return this.getTopicElement(topic,4);
    }
    
    private String getCoAPVerbFromTopic(String topic) {
        // format: iot-2/type/mbed/id/mbed-eth-observe/cmd/put/fmt/json
        return this.getTopicElement(topic, 6);
    }
    
    private String getCoAPURI(String message) {
        // expected format: { "path":"/303/0/5850", "new_value":"0", "ep":"mbed-eth-observe" }
        //this.errorLogger().info("getCoAPURI: payload: " + message);
        JSONParser parser = this.orchestrator().getJSONParser();
        Map parsed = parser.parseJson(message);
        return (String)parsed.get("path");
    }
    
    private String getCoAPValue(String message) {
        // expected format: { "path":"/303/0/5850", "new_value":"0", "ep":"mbed-eth-observe" }
        //this.errorLogger().info("getCoAPValue: payload: " + message);
        JSONParser parser = this.orchestrator().getJSONParser();
        Map parsed = parser.parseJson(message);
        return (String)parsed.get("new_value");
    }
    
    private String getCoAPEndpointName(String message) {
        // expected format: { "path":"/303/0/5850", "new_value":"0", "ep":"mbed-eth-observe" }
        //this.errorLogger().info("getCoAPValue: payload: " + message);
        JSONParser parser = this.orchestrator().getJSONParser();
        Map parsed = parser.parseJson(message);
        return (String)parsed.get("ep");
    }
    
    @Override
    public void onMessageReceive(String topic, String message) {
        // DEBUG
        this.errorLogger().info("WatsonIoT(CoAP Command): Topic: " + topic + " message: " + message);
        
        // parse the topic to get the endpoint and CoAP verb
        // format: iot-2/type/mbed/id/mbed-eth-observe/cmd/put/fmt/json
        String ep_name = this.getEndpointNameFromTopic(topic);
        String coap_verb = this.getCoAPVerbFromTopic(topic);
        
        // pull the CoAP URI and Payload from the message itself... its JSON... 
        // format: { "path":"/303/0/5850", "new_value":"0", "ep":"mbed-eth-observe" }
        String uri = this.getCoAPURI(message);
        String value = this.getCoAPValue(message);
        
        // if the ep_name is wildcarded... get the endpoint name from the JSON payload
        // format: { "path":"/303/0/5850", "new_value":"0", "ep":"mbed-eth-observe" }
        if (ep_name == null || ep_name.length() <= 0 || ep_name.equalsIgnoreCase("+")) {
            ep_name = this.getCoAPEndpointName(message);
        }
        
        // dispatch the coap resource operation request
        String response = this.orchestrator().processEndpointResourceOperation(coap_verb,ep_name,uri,value);
        
        // examine the response
        if (response != null && response.length() > 0) {
            // SYNC: We only process AsyncResponses from GET verbs... we dont sent HTTP status back through WatsonIoT.
            this.errorLogger().info("WatsonIoT(CoAP Command): Response: " + response);
            
            // AsyncResponse detection and recording...
            if (this.isAsyncResponse(response) == true) {
                if (coap_verb.equalsIgnoreCase("get") == true) {
                    // its an AsyncResponse.. so record it...
                    this.recordAsyncResponse(response,coap_verb,this.mqtt(),this,topic,message,uri,ep_name);
                }
                else {
                    // we ignore AsyncResponses to PUT,POST,DELETE
                    this.errorLogger().info("WatsonIoT(CoAP Command): Ignoring AsyncResponse for " + coap_verb + " (OK).");
                }
            }
            else if (coap_verb.equalsIgnoreCase("get")) {
                // not an AsyncResponse... so just emit it immediately... only for GET...
                this.errorLogger().info("WatsonIoT(CoAP Command): Response: " + response + " from GET... creating observation...");
                
                // we have to format as an observation...
                String observation = this.createObservation(coap_verb,ep_name,uri,response);
                
                // DEBUG
                this.errorLogger().info("WatsonIoT(CoAP Command): Sending Observation(GET): " + observation);
                
                // send the observation (GET reply)...
                this.mqtt().sendMessage(this.customizeTopic(this.m_watson_iot_observe_notification_topic,ep_name,this.m_watson_iot_device_manager.getDeviceType(ep_name)),observation,QoS.AT_MOST_ONCE); 
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
        notification.put("verb",verb);

        // RTI
        notification = this.rtiFormatMessage(notification,value);

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
        this.errorLogger().info("WatsonIoT: CoAP notification(GET REPLY): " + iotf_coap_json);
        
        // return the WatsonIoT-specific observation JSON...
        return iotf_coap_json;
    }
    
    // default formatter for AsyncResponse replies
    @Override
    public String formatAsyncResponseAsReply(Map async_response,String verb) {
        if (verb != null && verb.equalsIgnoreCase("GET") == true) {           
            try {
                // DEBUG
                this.errorLogger().info("WatsonIoT: CoAP AsyncResponse for GET: " + async_response);
                
                // get the Map of the response
                Map response_map = (Map)async_response.get("response_map");
                
                // Convert back to String, then to List
                String t = this.orchestrator().getJSONGenerator().generateJson(response_map);
                List async_responses = (List)this.orchestrator().getJSONParser().parseJson(t);
                for(int i=0;async_responses != null && i<async_responses.size();++i) {
                    // get the ith entry from the list
                    Map response = (Map)async_responses.get(i);
                    
                    // DEBUG
                    this.errorLogger().info("WatsonIoT: CoAP response(" + i + "): " + response);
                    
                    // get the payload from the ith entry
                    String payload = (String)response.get("payload");
                    if (payload != null) {
                        // trim 
                        payload = payload.trim();
                        
                        // parse if present
                        if (payload.length() > 0) {
                            // Base64 decode
                            String value = Utils.decodeCoAPPayload(payload);
                            
                            // build out the response
                            String uri = (String)async_response.get("uri");
                            String ep_name = (String)async_response.get("ep_name");
                            
                            // build out the 
                            String message = this.createObservation(verb, ep_name, uri, value);
                            
                            // DEBUG
                            this.errorLogger().info("WatsonIoT: Created(" + verb + ") GET Observation: " + message);
                            
                            // return the message
                            return message;
                        }
                    }
                }
            }
            catch (Exception ex) {
                // Error in creating the observation message from the AsyncResponse GET reply... 
                this.errorLogger().warning("formatAsyncResponseAsReply(WatsonIoT): Exception during GET reply -> observation creation. Not sending GET as observation...",ex);
            }
        }
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
}
