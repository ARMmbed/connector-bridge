/**
 * @file    GenericMQTTProcessor.java
 * @brief Generic MQTT peer processor for connector bridge
 * @author Doug Anson
 * @version 1.0
 * @see
 *
 * Copyright 2015. ARM Ltd. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */
package com.arm.connector.bridge.coordinator.processors.arm;

import com.arm.connector.bridge.coordinator.Orchestrator;
import com.arm.connector.bridge.coordinator.processors.core.PeerProcessor;
import com.arm.connector.bridge.coordinator.processors.interfaces.AsyncResponseProcessor;
import com.arm.connector.bridge.coordinator.processors.interfaces.PeerInterface;
import com.arm.connector.bridge.transport.HttpTransport;
import com.arm.connector.bridge.transport.MQTTTransport;
import com.arm.connector.bridge.core.Transport;
import com.arm.connector.bridge.core.TransportReceiveThread;
import com.arm.connector.bridge.core.Utils;
import com.arm.connector.bridge.data.SerializableHashMap;
import java.util.HashMap;
import java.util.Map;
import org.apache.commons.codec.binary.Base64;
import org.fusesource.mqtt.client.QoS;
import org.fusesource.mqtt.client.Topic;
import com.arm.connector.bridge.coordinator.processors.interfaces.ConnectionCreator;

/**
 * Generic MQTT peer processor
 *
 * @author Doug Anson
 */
public class GenericMQTTProcessor extends PeerProcessor implements Transport.ReceiveListener, PeerInterface {
    // default generic MQTT thread key
    private static String DEFAULT_GENERIC_RT_KEY = "__generic__";
    
    protected String m_mqtt_host = null;
    protected int m_mqtt_port = 0;
    protected String m_client_id = null;
    private HttpTransport m_http = null;
    protected boolean m_use_clean_session = false;
    private String m_device_data_key = null;
    private String m_default_tr_key = null;
    
    private HashMap<String, MQTTTransport> m_mqtt = null;
    protected SerializableHashMap m_endpoints = null;
    protected HashMap<String, TransportReceiveThread> m_mqtt_thread_list = null;

    // Factory method for initializing the Sample 3rd Party peer
    public static GenericMQTTProcessor createPeerProcessor(Orchestrator manager, HttpTransport http) {
        return new GenericMQTTProcessor(manager, new MQTTTransport(manager.errorLogger(), manager.preferences()), http);
    }
    
    // constructor (singleton)
    public GenericMQTTProcessor(Orchestrator orchestrator, MQTTTransport mqtt, HttpTransport http) {
        this(orchestrator, mqtt, null, http);
    }

    // constructor (suffix for preferences)
    public GenericMQTTProcessor(Orchestrator orchestrator, MQTTTransport mqtt, String suffix, HttpTransport http) {
        super(orchestrator, suffix);

        // HTTP support if we need it
        this.m_http = http;

        // MQTT transport list
        this.m_mqtt = new HashMap<>();
        
        // initialize the endpoint map
        this.m_endpoints = new SerializableHashMap(orchestrator,"ENDPOINT_MAP");
        
        // initialize the listener thread map
        this.m_mqtt_thread_list = new HashMap<>();
        
        // get the default generic name 
        this.m_default_tr_key = orchestrator.preferences().valueOf("mqtt_default_rt_key",this.m_suffix);
        if (this.m_default_tr_key == null || this.m_default_tr_key.length() == 0) {
            this.m_default_tr_key = DEFAULT_GENERIC_RT_KEY;
        }
        
        // initialize the topic root (MQTT)
        this.initTopicRoot("mqtt_mds_topic_root");

        // Get the device data key if one exists
        this.m_device_data_key = orchestrator.preferences().valueOf("mqtt_device_data_key", this.m_suffix);

        // build out our configuration
        this.m_mqtt_host = orchestrator.preferences().valueOf("mqtt_address", this.m_suffix);
        this.m_mqtt_port = orchestrator.preferences().intValueOf("mqtt_port", this.m_suffix);

        // establish the MQTT mDS request tag
        this.initRequestTag("mds_mqtt_request_tag");
        
        // clean session
        this.m_use_clean_session = this.orchestrator().preferences().booleanValueOf("mqtt_clean_session", this.m_suffix);

        // assign our MQTT transport if we have one...
        if (mqtt != null) {
            this.m_client_id = mqtt.getClientID();
            this.addMQTTTransport(this.m_client_id, mqtt);
        }

        // auto-subscribe behavior
        this.initAutoSubscribe("mqtt_obs_auto_subscribe");

        // setup our MQTT listener if we have one...
        if (mqtt != null) {
            // MQTT PeerProcessor listener thread setup
            TransportReceiveThread rt = new TransportReceiveThread(this.mqtt());
            rt.setOnReceiveListener(this);
            this.m_mqtt_thread_list.put(this.m_default_tr_key, rt);
        }
    }
    
    // create an observation JSON
    protected String createObservation(String verb, String ep_name, String uri, String value) {
        Map notification = new HashMap<>();

        // needs to look like this:  {"path":"/303/0/5700","payload":"MjkuNzU\u003d","max-age":"60","ep":"350e67be-9270-406b-8802-dd5e5f20","value":"29.75"}    
        if (value != null && value.length() > 0) {
            notification.put("value", this.fundamentalTypeDecoder().getFundamentalValue(value));
        }
        else {
            notification.put("value", this.fundamentalTypeDecoder().getFundamentalValue("0"));
        }
        notification.put("path", uri);
        notification.put("ep", ep_name);

        // add a new field to denote its a GET
        notification.put("coap_verb", verb);

        // Unified Format?
        if (this.unifiedFormatEnabled() == true) {
            notification.put("resourceId", uri.substring(1)); // strip off leading /
            notification.put("deviceId", ep_name);
            if (value != null) {
                notification.put("payload", Base64.encodeBase64String(value.getBytes()));  // Base64 Encoded payload
            }
            else {
                notification.put("payload", Base64.encodeBase64String("0".getBytes()));    // Base64 Encoded payload
            }
            notification.put("method", verb.toUpperCase()); // VERB is upper case
        }

        // we will send the raw CoAP JSON... 
        String coap_raw_json = this.jsonGenerator().generateJson(notification);

        // strip off []...
        String observation_json = this.stripArrayChars(coap_raw_json);


        // DEBUG
        this.errorLogger().info("CoAP notification(" + verb + " REPLY): " + observation_json);

        // return observation JSON
        return observation_json;
    }
    
    // default formatter for AsyncResponse replies
    @Override
    public String formatAsyncResponseAsReply(Map async_response, String verb) {
        // DEBUG
        this.errorLogger().info("MQTT(" + verb + ") AsyncResponse: ID: " + async_response.get("id") + " response: " + async_response);

        if (verb != null && verb.equalsIgnoreCase("GET") == true) {
            try {
                // DEBUG
                this.errorLogger().info("MQTT: CoAP AsyncResponse for GET: " + async_response);

                // get the payload from the ith entry
                String payload = (String) async_response.get("payload");
                if (payload != null) {
                    // trim 
                    payload = payload.trim();

                    // parse if present
                    if (payload.length() > 0) {
                        // Base64 decode
                        String value = Utils.decodeCoAPPayload(payload);

                        // build out the response
                        String uri = this.getURIFromAsyncID((String) async_response.get("id"));
                        String ep_name = this.getEndpointNameFromAsyncID((String) async_response.get("id"));

                        // build out the observation
                        String message = this.createObservation(verb, ep_name, uri, value);

                        // DEBUG
                        this.errorLogger().info("MQTT: Created(" + verb + ") GET observation: " + message);

                        // return the message
                        return message;
                    }
                }
            }
            catch (Exception ex) {
                // Error in creating the observation message from the AsyncResponse GET reply... 
                this.errorLogger().warning("MQTT(GET): Exception in formatAsyncResponseAsReply(): ", ex);
            }
        }

        // Handle AsyncReplies that are CoAP PUTs
        if (verb != null && verb.equalsIgnoreCase("PUT") == true) {
            try {
                // check to see if we have a payload or not... 
                String payload = (String) async_response.get("payload");
                if (payload != null) {
                    // trim 
                    payload = payload.trim();

                    // parse if present
                    if (payload.length() > 0) {
                        // Base64 decode
                        String value = Utils.decodeCoAPPayload(payload);

                        // build out the response
                        String uri = this.getURIFromAsyncID((String) async_response.get("id"));
                        String ep_name = this.getEndpointNameFromAsyncID((String) async_response.get("id"));

                        // build out the observation
                        String message = this.createObservation(verb, ep_name, uri, value);

                        // DEBUG
                        this.errorLogger().info("MQTT: Created(" + verb + ") PUT Observation: " + message);

                        // return the message
                        return message;
                    }
                }
                else {
                    // no payload... so we simply return the async-id
                    String value = (String) async_response.get("async-id");

                    // build out the response
                    String uri = this.getURIFromAsyncID((String) async_response.get("id"));
                    String ep_name = this.getEndpointNameFromAsyncID((String) async_response.get("id"));

                    // build out the observation
                    String message = this.createObservation(verb, ep_name, uri, value);

                    // DEBUG
                    this.errorLogger().info("MQTT: Created(" + verb + ") PUT Observation: " + message);

                    // return message
                    return message;
                }
            }
            catch (Exception ex) {
                // Error in creating the observation message from the AsyncResponse PUT reply... 
                this.errorLogger().warning("MQTT(PUT): Exception in formatAsyncResponseAsReply(): ", ex);
            }
        }

        // return null message
        return null;
    }
    
    // subscribe MQTT Topics
    protected void subscribe_to_topics(String ep_name, Topic topics[]) {
        this.mqtt(ep_name).subscribe(topics);
    }

    // does this endpoint already have registered subscriptions?
    protected boolean hasSubscriptions(String ep_name) {
        try {
            if (this.m_endpoints.get(ep_name) != null) {
                HashMap<String, Object> topic_data = (HashMap<String, Object>) this.m_endpoints.get(ep_name);
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
    
    // create endpoint topic data
    protected HashMap<String, Object> createEndpointTopicData(String ep_name, String ep_type) {
        // base class is empty
        return null;
    }
    
    // register topics for CoAP commands
    protected void subscribe(String ep_name, String ep_type,HashMap<String, Object> topic_data,ConnectionCreator cc) {
        if (ep_name != null && this.validateMQTTConnection(cc, ep_name, ep_type)) {
            // DEBUG
            this.orchestrator().errorLogger().info("MQTT: Subscribing to CoAP command topics for endpoint: " + ep_name + " type: " + ep_type);
            try {
                if (topic_data != null) {
                    // get,put,post,delete enablement
                    this.m_endpoints.remove(ep_name);
                    this.m_endpoints.put(ep_name, topic_data);
                    this.setEndpointTypeFromEndpointName(ep_name, ep_type);
                    cc.subscribe_to_topics(ep_name, (Topic[]) topic_data.get("topic_list"));
                }
                else {
                    // unable to register as topic data is NULL
                    this.orchestrator().errorLogger().warning("MQTT: GET/PUT/POST/DELETE topic data NULL. Unable to subscribe(): ep_name: " + ep_name + " ept: " + ep_type);
                }
            }
            catch (Exception ex) {
                this.orchestrator().errorLogger().warning("MQTT: Exception in subscribe for " + ep_name + " : " + ex.getMessage(),ex);
            }
        }
        else if (ep_name != null) {
            // unable to validate the MQTT connection
            this.orchestrator().errorLogger().warning("MQTT: Unable to validate MQTT connection. Unable to subscribe(): ep_name: " + ep_name + " ept: " + ep_type);
        }
        else {
            // NULL endpoint name
            this.orchestrator().errorLogger().warning("MQTT: NULL Endpoint name in subscribe()... ignoring...");
        }
    }
    
    // validate the MQTT Connection
    protected synchronized boolean validateMQTTConnection(ConnectionCreator cc,String ep_name, String ep_type) {
        if (cc != null) {
            // create a MQTT connection via the connector validator
            return cc.createAndStartMQTTForEndpoint(ep_name, ep_type);
        }
        else {
            // invalid params
            this.errorLogger().critical("MQTT: validateMQTTConnection(). Unable to call createAndStartMQTTForEndpoint() as ConnectionCreator parameter is NULL");
        }
        return false;
    }
    
    // un-register topics for CoAP commands
    protected boolean unsubscribe(String ep_name) {
        boolean unsubscribed = false;
        if (ep_name != null && this.mqtt(ep_name) != null) {
            // DEBUG
            this.orchestrator().errorLogger().info("MQTT: Un-Subscribing to CoAP command topics for endpoint: " + ep_name);
            try {
                HashMap<String, Object> topic_data = (HashMap<String, Object>) this.m_endpoints.get(ep_name);
                if (topic_data != null) {
                    // unsubscribe...
                    this.mqtt(ep_name).unsubscribe((String[]) topic_data.get("topic_string_list"));
                }
                else {
                    // not in subscription list (OK)
                    this.orchestrator().errorLogger().info("MQTT: Endpoint: " + ep_name + " not in subscription list (OK).");
                    unsubscribed = true;
                }
            }
            catch (Exception ex) {
                this.orchestrator().errorLogger().info("MQTT: Exception in unsubscribe for " + ep_name + " : " + ex.getMessage());
            }
        }
        else if (this.mqtt(ep_name) != null) {
            this.orchestrator().errorLogger().info("v: NULL Endpoint name... ignoring unsubscribe()...");
            unsubscribed = true;
        }
        else {
            this.orchestrator().errorLogger().info("MQTT: No MQTT connection for " + ep_name + "... ignoring unsubscribe()...");
            unsubscribed = true;
        }

        // clean up
        if (ep_name != null) {
            this.m_endpoints.remove(ep_name);
        }

        // return the unsubscribe status
        return unsubscribed;
    }

    // discover the endpoint attributes
    protected void retrieveEndpointAttributes(Map endpoint,AsyncResponseProcessor arp) {
        // DEBUG
        this.errorLogger().info("MQTT: Creating New Device: " + endpoint);

        // pre-populate the new endpoint with initial values for registration
        this.orchestrator().pullDeviceMetadata(endpoint, arp);
    }
    
    // create our MQTT-based authentication hash
    @Override
    public String createAuthenticationHash() {
        return this.mqtt().createAuthenticationHash();
    }
    
    // disconnect
    protected void disconnect(String ep_name) {
        if (this.isConnected(ep_name)) {
            this.mqtt(ep_name).disconnect(true);
        }
        this.remove(ep_name);
    }
    
    // are we connected (indexed by ep_name)
    protected boolean isConnected(String ep_name) {
        if (this.mqtt(ep_name) != null) {
            return this.mqtt(ep_name).isConnected();
        }
        return false;
    }
    
    // are we connected (non-indexed)
    protected boolean isConnected() {
        if (this.mqtt() != null) {
            return this.mqtt().isConnected();
        }
        return false;
    }
    
    // start our MQTT listener
    @Override
    public void initListener() {
        // connect and begin listening for requests (wildcard based on request TAG and domain)
        if (this.connectMQTT()) {
            this.subscribeToMQTTTopics();
            TransportReceiveThread rt = this.m_mqtt_thread_list.get(this.m_default_tr_key);
            if (rt != null) {
                rt.start();
            }
        }
    }

    // stop our MQTT listener
    @Override
    public void stopListener() {
        if (this.mqtt() != null) {
            this.mqtt().disconnect();
        }
    }
    
    // GenericSender Implementation: send a message
    @Override
    public void sendMessage(String to, String message) {
        // send a message over Google Cloud...
        this.errorLogger().info("sendMessage(MQTT-STD): Sending Message to: " + to + " message: " + message);
        
        // send the message over MQTT
        this.mqtt().sendMessage(to, message);
    }

    // OVERRIDE: Connection stock MQTT...
    protected boolean connectMQTT() {
        return this.mqtt().connect(this.m_mqtt_host, this.m_mqtt_port, null, true);
    }

    // OVERRIDE: Topics for stock MQTT...
    protected void subscribeToMQTTTopics() {
        String request_topic_str = this.getTopicRoot() + this.getRequestTag() + this.getDomain() + "/#";
        this.errorLogger().info("subscribeToMQTTTopics(MQTT-STD): listening on REQUEST topic: " + request_topic_str);
        Topic request_topic = new Topic(request_topic_str, QoS.AT_LEAST_ONCE);
        Topic[] topic_list = {request_topic};
        this.mqtt().subscribe(topic_list);
    }

    // get HTTP if needed
    protected HttpTransport http() {
        return this.m_http;
    }

    // get our defaulted reply topic (defaulted)
    public String getReplyTopic(String ep_name, String ep_type, String def) {
        return def;
    }

    // add a MQTT transport instance
    protected void addMQTTTransport(String id, MQTTTransport mqtt) {
        if (this.m_mqtt != null) {
            this.m_mqtt.remove(id);
            this.m_mqtt.put(id, mqtt);
        }
    }

    // initialize the MQTT transport instance list
    protected void initMQTTTransportList() {
        this.closeMQTTTransportList();
        if (this.m_mqtt != null) {
            this.m_mqtt.clear();
        }
    }

    // PROTECTED: get the MQTT transport for the default clientID
    protected MQTTTransport mqtt() {
        if (this.m_mqtt != null) {
            return this.mqtt(this.m_client_id); // clientID is default "id"
        }
        return null;
    }

    // PROTECTED: get the MQTT transport for a given clientID
    protected MQTTTransport mqtt(String id) {
        if (this.m_mqtt != null) {
            return this.m_mqtt.get(id);
        }
        return null;
    }

    // PROTECTED: remove MQTT Transport for a given clientID
    protected void remove(String id) {
        if (this.m_mqtt != null) {
            this.m_mqtt.remove(id);
        }
    }

    // close the tranports in the list
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
            }
        }
    }
}
