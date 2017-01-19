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
import com.arm.connector.bridge.coordinator.processors.core.Processor;
import com.arm.connector.bridge.coordinator.processors.interfaces.GenericSender;
import com.arm.connector.bridge.coordinator.processors.interfaces.PeerInterface;
import com.arm.connector.bridge.core.Utils;
import com.arm.connector.bridge.transport.HttpTransport;
import com.arm.connector.bridge.transport.MQTTTransport;
import com.arm.connector.bridge.core.Transport;
import com.arm.connector.bridge.core.TransportReceiveThread;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.fusesource.mqtt.client.QoS;
import org.fusesource.mqtt.client.Topic;

/**
 * Generic MQTT peer processor
 *
 * @author Doug Anson
 */
public class GenericMQTTProcessor extends Processor implements Transport.ReceiveListener, PeerInterface {
    protected TransportReceiveThread m_mqtt_thread = null;
    protected String m_mqtt_host = null;
    protected int m_mqtt_port = 0;
    protected String m_client_id = null;
    private HashMap<String, MQTTTransport> m_mqtt = null;
    private HttpTransport m_http = null;
    protected boolean m_use_clean_session = false;
    private HashMap<String, String> m_mqtt_endpoint_type_list = null;
    private String m_device_data_key = null;

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
        
        // initialize the topic root (MQTT)
        this.initTopicRoot("mqtt_mds_topic_root");

        // initialize the endpoint type map
        this.m_mqtt_endpoint_type_list = new HashMap<>();

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
            // MQTT Processor listener thread setup
            this.m_mqtt_thread = new TransportReceiveThread(this.mqtt());
            this.m_mqtt_thread.setOnReceiveListener(this);
        }
    }

    // process a received new registration
    @Override
    protected void processRegistration(Map data, String key) {
        List endpoints = (List) data.get(key);
        for (int i = 0; endpoints != null && i < endpoints.size(); ++i) {
            Map endpoint = (Map) endpoints.get(i);

            // ensure we have the endpoint type
            this.setEndpointTypeFromEndpointName((String) endpoint.get("ep"), (String) endpoint.get("ept"));

            // mimic the message that we get from direct discovery...
            String message = "[{\"name\":\"" + endpoint.get("ep") + "\",\"type\":\"" + endpoint.get("ept") + "\",\"status\":\"ACTIVE\"}]";
            String topic = this.createNewRegistrationTopic((String) endpoint.get("ept"), (String) endpoint.get("ep"));

            // DEBUG
            this.errorLogger().info("processRegistration(MQTT-STD) : Publishing new registration topic: " + topic + " message:" + message);
            this.mqtt().sendMessage(topic, message);

            // send it also raw... over a subtopic
            topic = this.createNewRegistrationTopic((String) endpoint.get("ept"), (String) endpoint.get("ep"));
            message = this.jsonGenerator().generateJson(endpoint);

            // DEBUG
            this.errorLogger().info("processRegistration(MQTT-STD) : Publishing new registration topic: " + topic + " message:" + message);
            this.mqtt().sendMessage(topic, message);

            // re-subscribe if previously subscribed to observable resources
            List resources = (List) endpoint.get("resources");
            for (int j = 0; resources != null && j < resources.size(); ++j) {
                Map resource = (Map) resources.get(j);
                if (this.subscriptionsList().containsSubscription(this.m_mds_domain, (String) endpoint.get("ep"), (String) endpoint.get("ept"), (String) resource.get("path"))) {
                    // re-subscribe to this resource
                    this.orchestrator().subscribeToEndpointResource((String) endpoint.get("ep"), (String) resource.get("path"), false);

                    // SYNC: here we dont have to worry about Sync options - we simply dispatch the subscription to mDS and setup for it...
                    this.subscriptionsList().removeSubscription(this.m_mds_domain, (String) endpoint.get("ep"), (String) endpoint.get("ept"), (String) resource.get("path"));
                    this.subscriptionsList().addSubscription(this.m_mds_domain, (String) endpoint.get("ep"), (String) endpoint.get("ept"), (String) resource.get("path"));
                }
                else if (this.isObservableResource(resource) && this.m_auto_subscribe_to_obs_resources == true) {
                    // auto-subscribe to observable resources... if enabled.
                    this.orchestrator().subscribeToEndpointResource((String) endpoint.get("ep"), (String) resource.get("path"), false);

                    // SYNC: here we dont have to worry about Sync options - we simply dispatch the subscription to mDS and setup for it...
                    this.subscriptionsList().removeSubscription(this.m_mds_domain, (String) endpoint.get("ep"), (String) endpoint.get("ept"), (String) resource.get("path"));
                    this.subscriptionsList().addSubscription(this.m_mds_domain, (String) endpoint.get("ep"), (String) endpoint.get("ept"), (String) resource.get("path"));
                }
            }
        }
    }
    
    // process a reregistration
    @Override
    public void processReRegistration(Map data) {
        List notifications = (List) data.get("reg-updates");
        for (int i = 0; notifications != null && i < notifications.size(); ++i) {
            Map endpoint = (Map) notifications.get(i);
            this.setEndpointTypeFromEndpointName((String) endpoint.get("ep"), (String) endpoint.get("ept"));
            List resources = (List) endpoint.get("resources");
            for (int j = 0; resources != null && j < resources.size(); ++j) {
                Map resource = (Map) resources.get(j);
                if (this.isObservableResource(resource)) {
                    this.errorLogger().info("processReRegistration(MQTT-STD) : CoAP re-registration: " + endpoint + " Resource: " + resource);
                    if (this.subscriptionsList().containsSubscription(this.m_mds_domain, (String) endpoint.get("ep"), (String) endpoint.get("ept"), (String) resource.get("path")) == false) {
                        this.errorLogger().info("processReRegistration(MQTT-STD) : CoAP re-registering OBS resources for: " + endpoint + " Resource: " + resource);
                        this.processRegistration(data, "reg-updates");
                        this.subscriptionsList().addSubscription(this.m_mds_domain, (String) endpoint.get("ep"), (String) endpoint.get("ept"), (String) resource.get("path"));
                    }
                }
            }
        }
    }

    // process a deregistration
    @Override
    public String[] processDeregistrations(Map parsed) {
        String[] deregistrations = this.parseDeRegistrationBody(parsed);
        this.orchestrator().processDeregistrations(deregistrations);
        for (int i = 0; i < deregistrations.length; ++i) {
            this.m_mqtt_endpoint_type_list.remove(deregistrations[i]);
        }
        return deregistrations;
    }
    
    // process a mDS notification for generic MQTT peers
    @Override
    public void processNotification(Map data) {
        // DEBUG
        //this.errorLogger().info("processMDSMessage(STD)...");

        // get the list of parsed notifications
        List notifications = (List) data.get("notifications");
        for (int i = 0; notifications != null && i < notifications.size(); ++i) {
            Map notification = (Map) notifications.get(i);

            // decode the Payload...
            String b64_coap_payload = (String) notification.get("payload");
            String decoded_coap_payload = Utils.decodeCoAPPayload(b64_coap_payload);

            // DEBUG
            //this.errorLogger().info("Watson IoT: Decoded Payload: " + decoded_coap_payload);
            // Try a JSON parse... if it succeeds, assume the payload is a composite JSON value...
            Map json_parsed = this.tryJSONParse(decoded_coap_payload);
            if (json_parsed != null && json_parsed.isEmpty() == false) {
                // add in a JSON object payload value directly... 
                notification.put("value", Utils.retypeMap(json_parsed, this.fundamentalTypeDecoder()));             // its JSON (flat...)                                                   // its JSON 
            }
            else {
                // add in a decoded payload value as a fundamental type...
                notification.put("value", this.fundamentalTypeDecoder().getFundamentalValue(decoded_coap_payload)); // its a Float, Integer, or String
            }

            // we will send the raw CoAP JSON... WatsonIoT can parse that... 
            String coap_raw_json = this.jsonGenerator().generateJson(notification);

            // strip off []...
            String coap_json_stripped = this.stripArrayChars(coap_raw_json);

            // get our endpoint name
            String ep_name = (String) notification.get("ep");

            // get our endpoint type
            String ep_type = (String) notification.get("ept");
            if (ep_type == null) {
                ep_type = this.getEndpointTypeFromEndpointName(ep_name);
            }

            // get the resource URI
            String uri = (String) notification.get("path");

            // make sure we have an active subscription for this notification
            if (this.subscriptionsList().containsSubscription(this.m_mds_domain, ep_name, ep_type, uri) == true) {
                // send it as JSON over the observation sub topic
                String topic = this.createObservationTopic(ep_type, ep_name, uri);

                // encapsulate into a coap/device packet...
                String coap_json = coap_json_stripped;
                if (this.m_device_data_key != null && this.m_device_data_key.length() > 0) {
                    coap_json = "{ \"" + this.m_device_data_key + "\":" + coap_json_stripped + "}";
                }

                // DEBUG
                this.errorLogger().info("processNotification(MQTT-STD): Active subscription for ep_name: " + ep_name + " ep_type: " + ep_type + " uri: " + uri);
                this.errorLogger().info("processNotification(MQTT-STD): Publishing notification: payload: " + coap_json + " topic: " + topic);

                // send to MQTT...
                this.mqtt().sendMessage(topic, coap_json);
            }
            else {
                // no active subscription present - so note but do not send
                this.errorLogger().info("processNotification(MQTT-STD): no active subscription for ep_name: " + ep_name + " ep_type: " + ep_type + " uri: " + uri + "... dropping notification...");
            }
        }
    }
    
    // create our MQTT-based authentication hash
    @Override
    public String createAuthenticationHash() {
        return this.mqtt().createAuthenticationHash();
    }
    
    // start our MQTT listener
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

    // stop our MQTT listener
    @Override
    public void stopListener() {
        if (this.mqtt() != null) {
            this.mqtt().disconnect();
        }
    }
    
    // get the endpoint type from the endpoint name
    @Override
    protected String getEndpointTypeFromEndpointName(String ep_name) {
        String t = super.getEndpointTypeFromEndpointName(ep_name);
        if (t != null) return t;
        return this.m_mqtt_endpoint_type_list.get(ep_name);
    }

    // set the endpoint type from the endpoint name
    protected void setEndpointTypeFromEndpointName(String ep_name, String ep_type) {
        this.m_mqtt_endpoint_type_list.put(ep_name, ep_type);
    }
    
    // messages from MQTT come here and are processed...
    @Override
    public void onMessageReceive(String topic, String message) {
        String verb = "PUT";

        // DEBUG
        this.errorLogger().info("onMessageReceive(MQTT-STD): Topic: " + topic + " message: " + message);

        // Endpoint Discovery....
        if (this.isEndpointDiscovery(topic)) {
            Map options = (Map) this.parseJson(message);
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

            // get the endpoint type from the endpoint name
            String ep_type = this.getEndpointTypeFromEndpointName(ep_name);

            // perform the operation
            json = this.orchestrator().processEndpointResourceOperation(verb, ep_name, uri, value, options);

            // send a response back if we have one...
            if (json != null && json.length() > 0) {
                // Strip the request tag
                String response_topic = this.createResourceResponseTopic(ep_type, ep_name, uri);

                // SYNC: here we have to handle AsyncResponses. if mDS returns an AsyncResponse... handle it
                if (this.isAsyncResponse(json) == true) {
                    if (verb.equalsIgnoreCase("get") == true || verb.equalsIgnoreCase("put") == true) {
                        // DEBUG
                        this.errorLogger().info("onMessageReceive(MQTT-STD): saving async response (" + verb + ") on topic: " + response_topic + " value: " + json);

                        // its an AsyncResponse to a GET or PUT.. so record it... 
                        this.recordAsyncResponse(json, verb, this.mqtt(), this, response_topic, message, ep_name, uri);
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
                    this.mqtt().sendMessage(response_topic, json);
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
            Map parsed = (Map) this.parseJson(message);
            String json = null;
            String ep_name = (String) parsed.get("ep");
            String uri = (String) parsed.get("path");
            String ep_type = (String) parsed.get("ept");
            verb = (String) parsed.get("verb");
            if (parsed != null && verb.equalsIgnoreCase("unsubscribe") == true) {
                // Unsubscribe 
                this.errorLogger().info("onMessageReceive(MQTT-STD): sending subscription request (remove subscription)");
                json = this.orchestrator().unsubscribeFromEndpointResource(this.orchestrator().createSubscriptionURI(ep_name, uri), parsed);

                // remove from the subscription list
                this.errorLogger().info("onMessageReceive(MQTT-STD): removing subscription TOPIC: " + topic + " endpoint: " + ep_name + " type: " + ep_type + " uri: " + uri);
                this.subscriptionsList().removeSubscription(this.m_mds_domain, ep_name, ep_type, uri);
            }
            else if (parsed != null && verb.equalsIgnoreCase("subscribe") == true) {
                // Subscribe
                this.errorLogger().info("onMessageReceive(MQTT-STD): sending subscription request (add subscription)");
                json = this.orchestrator().subscribeToEndpointResource(this.orchestrator().createSubscriptionURI(ep_name, uri), parsed, true);

                // add to the subscription list
                this.errorLogger().info("onMessageReceive(MQTT-STD): adding subscription TOPIC: " + topic + " endpoint: " + ep_name + " type: " + ep_type + " uri: " + uri);
                this.subscriptionsList().addSubscription(this.m_mds_domain, ep_name, ep_type, uri);
            }
            else if (parsed != null) {
                // verb not recognized
                this.errorLogger().info("onMessageReceive(MQTT-STD): Unable to process subscription request: unrecognized verb: " + verb);
            }
            else {
                // invalid message
                this.errorLogger().info("onMessageReceive(MQTT-STD): Unable to process subscription request: invalid message: " + message);
            }
        }
    }

    // record an async response to process later (override for MQTT-based peers)
    protected void recordAsyncResponse(String response, String coap_verb, GenericSender sender, Processor proc, String response_topic, String reply_topic, String message, String ep_name, String uri) {
        this.asyncResponseManager().recordAsyncResponse(response, coap_verb, sender, proc, response_topic, reply_topic, message, ep_name, uri);
    }

    // record an async response to process later (override for MQTT-based peers)
    private void recordAsyncResponse(String response, String coap_verb, GenericSender sender, Processor proc, String response_topic, String message, String ep_name, String uri) {
        this.asyncResponseManager().recordAsyncResponse(response, coap_verb, sender, proc, response_topic, this.getReplyTopic(ep_name, this.getEndpointTypeFromEndpointName(ep_name), uri, response_topic), message, ep_name, uri);
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

    // get our reply topic (if we specify URI, the build out the full resource response topic)
    private String getReplyTopic(String ep_name, String ep_type, String uri, String def) {
        return this.createResourceResponseTopic(ep_type, ep_name, uri);
    }

    // get our defaulted reply topic (defaulted)
    public String getReplyTopic(String ep_name, String ep_type, String def) {
        return def;
    }

    // add a MQTT transport instance
    protected void addMQTTTransport(String clientID, MQTTTransport mqtt) {
        this.m_mqtt.put(clientID, mqtt);
    }

    // initialize the MQTT transport instance list
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
}
