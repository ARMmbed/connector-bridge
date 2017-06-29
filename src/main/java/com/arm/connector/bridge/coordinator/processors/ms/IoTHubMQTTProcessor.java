/**
 * @file    IoTHubMQTTProcessor.java
 * @brief MS IoTHub MQTT Peer Processor
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
package com.arm.connector.bridge.coordinator.processors.ms;

import com.arm.connector.bridge.coordinator.processors.arm.GenericMQTTProcessor;
import com.arm.connector.bridge.coordinator.Orchestrator;
import com.arm.connector.bridge.coordinator.processors.interfaces.AsyncResponseProcessor;
import com.arm.connector.bridge.coordinator.processors.interfaces.PeerInterface;
import com.arm.connector.bridge.core.Utils;
import com.arm.connector.bridge.transport.HttpTransport;
import com.arm.connector.bridge.transport.MQTTTransport;
import com.arm.connector.bridge.core.Transport;
import com.arm.connector.bridge.core.TransportReceiveThread;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.commons.codec.binary.Base64;
import org.fusesource.mqtt.client.QoS;
import org.fusesource.mqtt.client.Topic;

/**
 * MS IoTHub peer processor based on MQTT
 *
 * @author Doug Anson
 */
public class IoTHubMQTTProcessor extends GenericMQTTProcessor implements Transport.ReceiveListener, PeerInterface, AsyncResponseProcessor {
    private int m_num_coap_topics = 1;                                  // # of MQTT Topics for CoAP verbs in IoTHub implementation
    private String m_iot_hub_observe_notification_topic = null;
    private String m_iot_hub_coap_cmd_topic_base = null;
    private String m_iot_hub_name = null;
    private String m_iot_hub_password_template = null;
    private HashMap<String, Object> m_iot_hub_endpoints = null;
    private HashMap<String, TransportReceiveThread> m_mqtt_thread_list = null;
    private IoTHubDeviceManager m_iot_hub_device_manager = null;
    private boolean m_iot_event_hub_enable_device_id_prefix = false;
    private String m_iot_event_hub_device_id_prefix = null;

    // constructor (singleton)
    public IoTHubMQTTProcessor(Orchestrator manager, MQTTTransport mqtt, HttpTransport http) {
        this(manager, mqtt, null, http);
    }

    // constructor (with suffix for preferences)
    public IoTHubMQTTProcessor(Orchestrator manager, MQTTTransport mqtt, String suffix, HttpTransport http) {
        super(manager, mqtt, suffix, http);

        // IoTHub Processor Announce
        this.errorLogger().info("MS IoTHub Processor ENABLED.");

        // initialize the endpoint map
        this.m_iot_hub_endpoints = new HashMap<>();

        // initialize the listener thread map
        this.m_mqtt_thread_list = new HashMap<>();

        // get our defaults
        this.m_iot_hub_name = this.orchestrator().preferences().valueOf("iot_event_hub_name", this.m_suffix);
        this.m_mqtt_host = this.orchestrator().preferences().valueOf("iot_event_hub_mqtt_ip_address", this.m_suffix).replace("__IOT_EVENT_HUB__", this.m_iot_hub_name);

        // Observation notification topic
        this.m_iot_hub_observe_notification_topic = this.orchestrator().preferences().valueOf("iot_event_hub_observe_notification_topic", this.m_suffix) + this.m_observation_key;

        // Send CoAP commands back through mDS into the endpoint via these Topics... 
        this.m_iot_hub_coap_cmd_topic_base = this.orchestrator().preferences().valueOf("iot_event_hub_coap_cmd_topic", this.m_suffix).replace("__COMMAND_TYPE__", "#");

        // IoTHub Device Manager - will initialize and update our IoTHub bindings/metadata
        this.m_iot_hub_device_manager = new IoTHubDeviceManager(this.orchestrator().errorLogger(), this.orchestrator().preferences(), this.m_suffix, http, this.orchestrator());

        // set the MQTT password template
        this.m_iot_hub_password_template = this.orchestrator().preferences().valueOf("iot_event_hub_mqtt_password", this.m_suffix).replace("__IOT_EVENT_HUB__", this.m_iot_hub_name);

        // Enable prefixing of mbed Cloud names for IoTHub
        this.m_iot_event_hub_enable_device_id_prefix = this.prefBoolValue("iot_event_hub_enable_device_id_prefix", this.m_suffix);
        this.m_iot_event_hub_device_id_prefix = null;

        // If prefixing is enabled, get the prefix
        if (this.m_iot_event_hub_enable_device_id_prefix == true) {
            this.m_iot_event_hub_device_id_prefix = this.preferences().valueOf("iot_event_hub_device_id_prefix", this.m_suffix);
            if (this.m_iot_event_hub_device_id_prefix != null) {
                this.m_iot_event_hub_device_id_prefix += "-";
            }
        }

        // initialize our MQTT transport list
        this.initMQTTTransportList();
    }

    // OVERRIDE: process a received new registration for IoTHub
    @Override
    protected synchronized void processRegistration(Map data, String key) {
        List endpoints = (List) data.get(key);
        for (int i = 0; endpoints != null && i < endpoints.size(); ++i) {
            Map endpoint = (Map) endpoints.get(i);
            List resources = (List) endpoint.get("resources");
            for (int j = 0; resources != null && j < resources.size(); ++j) {
                Map resource = (Map) resources.get(j);

                // get the endpoint name
                String ep_name = (String) endpoint.get("ep");

                // re-subscribe
                if (this.subscriptionsList().containsSubscription(this.m_mds_domain, ep_name, (String) endpoint.get("ept"), (String) resource.get("path"))) {
                    // re-subscribe to this resource
                    this.orchestrator().subscribeToEndpointResource(ep_name, (String) resource.get("path"), false);

                    // SYNC: here we dont have to worry about Sync options - we simply dispatch the subscription to mDS and setup for it...
                    this.subscriptionsList().removeSubscription(this.m_mds_domain, ep_name, (String) endpoint.get("ept"), (String) resource.get("path"));
                    this.subscriptionsList().addSubscription(this.m_mds_domain, ep_name, (String) endpoint.get("ept"), (String) resource.get("path"), this.isObservableResource(resource));
                }

                // auto-subscribe
                else if (this.isObservableResource(resource) && this.m_auto_subscribe_to_obs_resources == true) {
                    // auto-subscribe to observable resources... if enabled.
                    this.orchestrator().subscribeToEndpointResource(ep_name, (String) resource.get("path"), false);

                    // SYNC: here we dont have to worry about Sync options - we simply dispatch the subscription to mDS and setup for it...
                    this.subscriptionsList().removeSubscription(this.m_mds_domain, ep_name, (String) endpoint.get("ept"), (String) resource.get("path"));
                    this.subscriptionsList().addSubscription(this.m_mds_domain, ep_name, (String) endpoint.get("ept"), (String) resource.get("path"), this.isObservableResource(resource));
                }
            }

            // invoke a GET to get the resource information for this endpoint... we will update the Metadata when it arrives
            this.retrieveEndpointAttributes(endpoint);
        }
    }

    // OVERRIDE: process a re-registration in IoTHub
    @Override
    public void processReRegistration(Map data) {
        List notifications = (List) data.get("reg-updates");
        for (int i = 0; notifications != null && i < notifications.size(); ++i) {
            Map entry = (Map) notifications.get(i);

            // IOTHUB Prefix
            String iothub_ep_name = this.addDeviceIDPrefix((String) entry.get("ep"));

            // DEBUG
            // this.errorLogger().info("IoTHub : CoAP re-registration: " + entry);
            if (this.hasSubscriptions(iothub_ep_name) == false) {
                // no subscriptions - so process as a new registration
                this.errorLogger().info("IoTHub : CoAP re-registration: no subscriptions.. processing as new registration...");
                this.processRegistration(data, "reg-updates");
            }
            else {
                // already subscribed (OK)
                this.errorLogger().info("IoTHub : CoAP re-registration: already subscribed (OK)");
            }
        }
    }

    // OVERRIDE: handle de-registrations for IoTHub
    @Override
    public String[] processDeregistrations(Map parsed) {
        String[] deregistration = super.processDeregistrations(parsed);
        for (int i = 0; deregistration != null && i < deregistration.length; ++i) {
            // DEBUG
            this.errorLogger().info("IoTHub : CoAP de-registration: " + deregistration[i]);

            // IOTHUB Prefix
            String iothub_ep_name = this.addDeviceIDPrefix(deregistration[i]);

            // IoTHub add-on... 
            this.unsubscribe(iothub_ep_name);

            // Remove from IoTHub
            this.deregisterDevice(iothub_ep_name);
        }
        return deregistration;
    }
    
    // OVERRIDE: process a mDS notification for IoTHub
    @Override
    public void processNotification(Map data) {
        // DEBUG
        //this.errorLogger().info("processIncomingDeviceServerMessage(IoTHub)...");

        // get the list of parsed notifications
        List notifications = (List) data.get("notifications");
        for (int i = 0; notifications != null && i < notifications.size(); ++i) {
            // we have to process the payload... this may be dependent on being a string core type... 
            Map notification = (Map) notifications.get(i);

            // decode the Payload...
            String b64_coap_payload = (String) notification.get("payload");
            String decoded_coap_payload = Utils.decodeCoAPPayload(b64_coap_payload);

            // DEBUG
            //this.errorLogger().info("IoTHub: Decoded Payload: " + decoded_coap_payload);
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

            // get our endpoint name
            String ep_name = (String) notification.get("ep");

            // IOTHUB Prefix
            String iothub_ep_name = this.addDeviceIDPrefix(ep_name);

            // IOTHUB Prefix - re-write EP
            notification.put("ep", iothub_ep_name);

            // we will send the raw CoAP JSON... IoTHub can parse that... 
            String coap_raw_json = this.jsonGenerator().generateJson(notification);

            // strip off []...
            String coap_json_stripped = this.stripArrayChars(coap_raw_json);

            // encapsulate into a coap/device packet...
            String iot_event_hub_coap_json = coap_json_stripped;

            // DEBUG
            this.errorLogger().info("IoTHub: CoAP notification (STR): " + iot_event_hub_coap_json);

            // send to IoTHub...
            if (this.mqtt(iothub_ep_name) != null) {
                boolean status = this.mqtt(iothub_ep_name).sendMessage(this.customizeTopic(this.m_iot_hub_observe_notification_topic, iothub_ep_name, null), iot_event_hub_coap_json, QoS.AT_MOST_ONCE);
                if (status == true) {
                    // not connected
                    this.errorLogger().info("IoTHub: CoAP notification sent. SUCCESS");
                }
                else {
                    // send failed
                    this.errorLogger().warning("IoTHub: CoAP notification not sent. SEND FAILED");
                }
            }
            else {
                // not connected
                this.errorLogger().warning("IoTHub: CoAP notification not sent. NOT CONNECTED");
            }
        }
    }

    // subscribe to the IoTHub MQTT topics
    private void subscribe_to_topics(String ep_name, Topic topics[]) {
        // IOTHUB DeviceID Prefix
        String iothub_ep_name = this.addDeviceIDPrefix(ep_name);
        this.mqtt(iothub_ep_name).subscribe(topics);
    }

    // does this endpoint already have registered subscriptions?
    private boolean hasSubscriptions(String ep_name) {
        try {
            // IOTHUB DeviceID Prefix
            String iothub_ep_name = this.addDeviceIDPrefix(ep_name);
            if (this.m_iot_hub_endpoints.get(iothub_ep_name) != null) {
                HashMap<String, Object> topic_data = (HashMap<String, Object>) this.m_iot_hub_endpoints.get(iothub_ep_name);
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
    private void subscribe(String ep_name, String ep_type) {
        // IOTHUB Prefix
        String iothub_ep_name = this.addDeviceIDPrefix(ep_name);

        if (iothub_ep_name != null && this.validateMQTTConnection(iothub_ep_name, ep_type)) {
            // DEBUG
            this.orchestrator().errorLogger().info("IoTHub: Subscribing to CoAP command topics for endpoint: " + ep_name);
            try {
                HashMap<String, Object> topic_data = this.createEndpointTopicData(iothub_ep_name, ep_type);
                if (topic_data != null) {
                    // get,put,post,delete enablement
                    this.m_iot_hub_endpoints.remove(iothub_ep_name);
                    this.m_iot_hub_endpoints.put(iothub_ep_name, topic_data);
                    this.setEndpointTypeFromEndpointName(ep_name, ep_type);
                    this.subscribe_to_topics(iothub_ep_name, (Topic[]) topic_data.get("topic_list"));
                }
                else {
                    this.orchestrator().errorLogger().warning("IoTHub: GET/PUT/POST/DELETE topic data NULL. GET/PUT/POST/DELETE disabled");
                }
            }
            catch (Exception ex) {
                this.orchestrator().errorLogger().info("IoTHub: Exception in subscribe for " + iothub_ep_name + " : " + ex.getMessage());
            }
        }
        else {
            this.orchestrator().errorLogger().info("IoTHub: NULL Endpoint name in subscribe()... ignoring...");
        }
    }

    // un-register topics for CoAP commands
    private boolean unsubscribe(String ep_name) {
        boolean unsubscribed = false;

        // IOTHUB Prefix
        String iothub_ep_name = this.addDeviceIDPrefix(ep_name);

        if (iothub_ep_name != null && this.mqtt(iothub_ep_name) != null) {
            // DEBUG
            this.orchestrator().errorLogger().info("IoTHub: Un-Subscribing to CoAP command topics for endpoint: " + iothub_ep_name);
            try {
                HashMap<String, Object> topic_data = (HashMap<String, Object>) this.m_iot_hub_endpoints.get(iothub_ep_name);
                if (topic_data != null) {
                    // unsubscribe...
                    this.mqtt(iothub_ep_name).unsubscribe((String[]) topic_data.get("topic_string_list"));
                }
                else {
                    // not in subscription list (OK)
                    this.orchestrator().errorLogger().info("IoTHub: Endpoint: " + iothub_ep_name + " not in subscription list (OK).");
                    unsubscribed = true;
                }
            }
            catch (Exception ex) {
                this.orchestrator().errorLogger().info("IoTHub: Exception in unsubscribe for " + ep_name + " : " + ex.getMessage());
            }
        }
        else if (this.mqtt(iothub_ep_name) != null) {
            this.orchestrator().errorLogger().info("IoTHub: NULL Endpoint name... ignoring unsubscribe()...");
            unsubscribed = true;
        }
        else {
            this.orchestrator().errorLogger().info("IoTHub: No MQTT connection for " + iothub_ep_name + "... ignoring unsubscribe()...");
            unsubscribed = true;
        }

        // clean up
        if (iothub_ep_name != null) {
            this.m_iot_hub_endpoints.remove(iothub_ep_name);
        }

        // return the unsubscribe status
        return unsubscribed;
    }

    // get a topic element (parsed as a URL)
    @SuppressWarnings("empty-statement")
    private String getTopicElement(String topic, String key) {
        String value = null;

        try {
            // split by forward slash
            String tmp_slash[] = topic.split("/");

            // take the last element and split it again, by &
            if (tmp_slash != null && tmp_slash.length > 0) {
                String tmp_properties[] = tmp_slash[tmp_slash.length - 1].split("&");
                for (int i = 0; tmp_properties != null && i < tmp_properties.length && value == null; ++i) {
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
            this.errorLogger().info("IoTHub: getTopicElement: key: " + key + "  value: " + value);
        }
        else {
            // value not found
            this.errorLogger().info("IoTHub: getTopicElement: key: " + key + "  value: NULL");
        }

        // return the value
        return value;
    }

    // CoAP command handler - processes CoAP commands coming over MQTT channel
    @Override
    public void onMessageReceive(String topic, String message) {
        // DEBUG
        this.errorLogger().info("IoTHub(CoAP Command): Topic: " + topic + " message: " + message);

        // parse the topic to get the endpoint
        // format: devices/__EPNAME__/messages/devicebound/#
        // IOTHUB DevicIDPrefix
        String iothub_ep_name = this.addDeviceIDPrefix(this.getEndpointNameFromTopic(topic));

        // pull the CoAP Path URI from the message itself... its JSON... 
        // format: { "path":"/303/0/5850", "new_value":"0", "ep":"mbed-eth-observe", "coap_verb": "get" }
        String uri = this.getCoAPURI(message);
        if (uri == null || uri.length() == 0) {
            // optionally pull the CoAP URI Path from the MQTT topic (SECONDARY)
            uri = this.getResourceURIFromTopic(topic);
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
        if (iothub_ep_name == null || iothub_ep_name.length() <= 0 || iothub_ep_name.equalsIgnoreCase("+")) {
            iothub_ep_name = this.addDeviceIDPrefix(this.getCoAPEndpointName(message));
        }

        // if there are mDC/mDS REST options... lets add them
        // format: { "path":"/303/0/5850", "new_value":"0", "ep":"mbed-eth-observe", "coap_verb": "get", "options":"noResp=true" }
        String options = this.getRESTOptions(message);

        // dispatch the coap resource operation request
        String response = this.orchestrator().processEndpointResourceOperation(coap_verb, this.removeDeviceIDPrefix(iothub_ep_name), uri, value, options);

        // examine the response
        if (response != null && response.length() > 0) {
            // SYNC: We only process AsyncResponses from GET verbs... we dont sent HTTP status back through IoTHub.
            this.errorLogger().info("IoTHub(CoAP Command): Response: " + response);

            // AsyncResponse detection and recording...
            if (this.isAsyncResponse(response) == true) {
                // CoAP GET and PUT provides AsyncResponses...
                if (coap_verb.equalsIgnoreCase("get") == true || coap_verb.equalsIgnoreCase("put") == true) {
                    // its an AsyncResponse.. so record it...
                    this.recordAsyncResponse(response, coap_verb, this.mqtt(iothub_ep_name), this, topic, this.getReplyTopic(iothub_ep_name, this.getEndpointTypeFromEndpointName(iothub_ep_name), uri), message, iothub_ep_name, uri);
                }
                else {
                    // we ignore AsyncResponses to PUT,POST,DELETE
                    this.errorLogger().info("IoTHub(CoAP Command): Ignoring AsyncResponse for " + coap_verb + " (OK).");
                }
            }
            else if (coap_verb.equalsIgnoreCase("get")) {
                // not an AsyncResponse... so just emit it immediately... only for GET...
                this.errorLogger().info("IoTHub(CoAP Command): Response: " + response + " from GET... creating observation...");

                // we have to format as an observation...
                String observation = this.createObservation(coap_verb, iothub_ep_name, uri, response);

                // DEBUG
                this.errorLogger().info("IoTHub(CoAP Command): Sending Observation(GET): " + observation);

                // send the observation (GET reply)...
                if (this.mqtt(iothub_ep_name) != null) {
                    String reply_topic = this.customizeTopic(this.m_iot_hub_observe_notification_topic, iothub_ep_name, null);
                    reply_topic = reply_topic.replace(this.m_observation_key, this.m_cmd_response_key);
                    boolean status = this.mqtt(iothub_ep_name).sendMessage(reply_topic, observation, QoS.AT_MOST_ONCE);
                    if (status == true) {
                        // success
                        this.errorLogger().info("IoTHub(CoAP Command): CoAP observation(get) sent. SUCCESS");
                    }
                    else {
                        // send failed
                        this.errorLogger().warning("IoTHub(CoAP Command): CoAP observation(get) not sent. SEND FAILED");
                    }
                }
                else {
                    // not connected
                    this.errorLogger().warning("IoTHub(CoAP Command): CoAP observation(get) not sent. NOT CONNECTED");
                }
            }
        }
    }

    // create an observation JSON as a response to a GET request...
    private String createObservation(String verb, String ep_name, String uri, String value) {
        Map notification = new HashMap<>();

        // needs to look like this: {"path":"/303/0/5700","payload":"MjkuNzU\u003d","max-age":"60","ep":"350e67be-9270-406b-8802-dd5e5f20ansond","value":"29.75"}    
        notification.put("value", this.fundamentalTypeDecoder().getFundamentalValue(value));
        notification.put("path", uri);
        notification.put("ep", ep_name);

        // add a new field to denote its a GET
        notification.put("coap_verb", verb);

        // Unified Format?
        if (this.unifiedFormatEnabled() == true) {
            notification.put("resourceId", uri);
            notification.put("deviceId", ep_name);
            if (value != null) {
                notification.put("payload", Base64.encodeBase64(value.getBytes()));  // Base64 Encoded payload
            }
            else {
                notification.put("payload", Base64.encodeBase64("0".getBytes()));    // Base64 Encoded payload
            }
            notification.put("method", verb);
        }

        // we will send the raw CoAP JSON... IoTHub can parse that... 
        String coap_raw_json = this.jsonGenerator().generateJson(notification);

        // strip off []...
        String coap_json_stripped = this.stripArrayChars(coap_raw_json);

        // encapsulate into a coap/device packet...
        String iot_event_hub_coap_json = coap_json_stripped;

        // DEBUG
        this.errorLogger().info("IoTHub: CoAP notification(" + verb + " REPLY): " + iot_event_hub_coap_json);

        // return the IoTHub-specific observation JSON...
        return iot_event_hub_coap_json;
    }

    // default formatter for AsyncResponse replies
    @Override
    public String formatAsyncResponseAsReply(Map async_response, String verb) {
        // DEBUG
        this.errorLogger().info("IoTHub(" + verb + ") AsyncResponse: " + async_response);

        if (verb != null && verb.equalsIgnoreCase("GET") == true) {
            try {
                // DEBUG
                this.errorLogger().info("IoTHub: CoAP AsyncResponse for GET: " + async_response);

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

                        // IOTHUB DeviceID Prefix
                        String iothub_ep_name = this.addDeviceIDPrefix(ep_name);

                        // build out the observation
                        String message = this.createObservation(verb, iothub_ep_name, uri, value);

                        // DEBUG
                        this.errorLogger().info("IoTHub: Created(" + verb + ") GET observation: " + message);

                        // return the message
                        return message;
                    }
                }
            }
            catch (Exception ex) {
                // Error in creating the observation message from the AsyncResponse GET reply... 
                this.errorLogger().warning("IoTHub(GET): Exception in formatAsyncResponseAsReply(): ", ex);
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

                        // IOTHUB DeviceID Prefix
                        String iothub_ep_name = this.addDeviceIDPrefix(ep_name);

                        // build out the observation
                        String message = this.createObservation(verb, iothub_ep_name, uri, value);

                        // DEBUG
                        this.errorLogger().info("IoTHub: Created(" + verb + ") PUT Observation: " + message);

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

                    // IOTHUB DeviceID Prefix
                    String iothub_ep_name = this.addDeviceIDPrefix(ep_name);

                    // build out the observation
                    String message = this.createObservation(verb, iothub_ep_name, uri, value);

                    // DEBUG
                    this.errorLogger().info("IoTHub: Created(" + verb + ") PUT Observation: " + message);

                    // return message
                    return message;
                }
            }
            catch (Exception ex) {
                // Error in creating the observation message from the AsyncResponse PUT reply... 
                this.errorLogger().warning("IoTHub(PUT): Exception in formatAsyncResponseAsReply(): ", ex);
            }
        }

        // return null message
        return null;
    }

    // process new device registration
    @Override
    protected synchronized Boolean registerNewDevice(Map message) {
        if (this.m_iot_hub_device_manager != null) {
            // create the device in IoTHub
            Boolean success = this.m_iot_hub_device_manager.registerNewDevice(message);

            // IOTHUB DeviceID Prefix
            String iothub_ep_name = this.addDeviceIDPrefix((String) message.get("ep"));

            // if successful, validate (i.e. add...) an MQTT Connection
            if (success == true) {
                this.validateMQTTConnection(iothub_ep_name, (String) message.get("ept"));
            }

            // return status
            return success;
        }
        return false;
    }

    // process device de-registration
    @Override
    protected synchronized Boolean deregisterDevice(String ep_name) {
        if (this.m_iot_hub_device_manager != null) {
            // IOTHUB DeviceID Prefix
            String iothub_ep_name = this.addDeviceIDPrefix(ep_name);

            // DEBUG
            this.errorLogger().info("deregisterDevice(IoTHub): deregistering device: " + iothub_ep_name);

            // disconnect, remove the threaded listener... 
            if (this.m_mqtt_thread_list.get(iothub_ep_name) != null) {
                try {
                    this.m_mqtt_thread_list.get(iothub_ep_name).disconnect();
                }
                catch (Exception ex) {
                    // note but continue...
                    this.errorLogger().warning("deregisterDevice(IoTHub): exception during deregistration", ex);
                }
                this.m_mqtt_thread_list.remove(iothub_ep_name);
            }

            // also remove MQTT Transport instance too...
            this.disconnect(iothub_ep_name);

            // remove the device from IoTHub
            if (this.m_iot_hub_device_manager.deregisterDevice(iothub_ep_name) == false) {
                this.errorLogger().warning("deregisterDevice(IoTHub): unable to de-register device from IoTHub...");
            }
        }
        return true;
    }

    // AsyncResponse response processor
    @Override
    public boolean processAsyncResponse(Map endpoint) {
        // with the attributes added, we finally create the device in Watson IoT
        this.completeNewDeviceRegistration(endpoint);

        // return our processing status
        return true;
    }

    // complete processing of adding the new device
    private void completeNewDeviceRegistration(Map endpoint) {
        try {
            // create the device in IoTHub
            this.errorLogger().info("completeNewDeviceRegistration: calling registerNewDevice(): " + endpoint);
            this.registerNewDevice(endpoint);
            this.errorLogger().info("completeNewDeviceRegistration: registerNewDevice() completed");
        }
        catch (Exception ex) {
            this.errorLogger().warning("completeNewDeviceRegistration: caught exception in registerNewDevice(): " + endpoint, ex);
        }

        try {
            // subscribe for IoTHub as well..
            this.errorLogger().info("completeNewDeviceRegistration: calling subscribe(): " + endpoint);
            this.subscribe((String) endpoint.get("ep"), (String) endpoint.get("ept"));
            this.errorLogger().info("completeNewDeviceRegistration: subscribe() completed");
        }
        catch (Exception ex) {
            this.errorLogger().warning("completeNewDeviceRegistration: caught exception in subscribe(): " + endpoint, ex);
        }
    }
    
    // discover the endpoint attributes
    private void retrieveEndpointAttributes(Map endpoint) {
        // DEBUG
        this.errorLogger().warning("IoTHub: Creating New Device: " + endpoint);

        // pre-populate the new endpoint with initial values for registration
        this.orchestrator().pullDeviceMetadata(endpoint, this);
    }
    
    // IoTHub DeviceID Prefix enabler
    private String addDeviceIDPrefix(String ep_name) {
        String iothub_ep_name = ep_name;
        if (this.m_iot_event_hub_device_id_prefix != null && ep_name != null) {
            if (ep_name.contains(this.m_iot_event_hub_device_id_prefix) == false) {
                iothub_ep_name = this.m_iot_event_hub_device_id_prefix + ep_name;
            }
        }

        // DEBUG
        //this.errorLogger().info("addDeviceIDPrefix: ep_name: " + ep_name + " --> iothub_ep_name: " + iothub_ep_name);
        return iothub_ep_name;
    }

    // IoTHub DeviceID Prefix remover
    private String removeDeviceIDPrefix(String iothub_ep_name) {
        String ep_name = iothub_ep_name;
        if (this.m_iot_event_hub_device_id_prefix != null && iothub_ep_name != null) {
            return iothub_ep_name.replace(this.m_iot_event_hub_device_id_prefix, "");
        }

        // trim..
        if (ep_name != null) {
            ep_name = ep_name.trim();
        }

        // DEBUG
        //this.errorLogger().info("removeDeviceIDPrefix: iothub_ep_name: " + iothub_ep_name + " --> ep_name: " + ep_name);
        return ep_name;
    }
    
    // create the endpoint IoTHub topic data
    private HashMap<String, Object> createEndpointTopicData(String ep_name, String ep_type) {
        HashMap<String, Object> topic_data = null;
        if (this.m_iot_hub_coap_cmd_topic_base != null) {
            Topic[] list = new Topic[m_num_coap_topics];
            String[] topic_string_list = new String[m_num_coap_topics];

            // IOTHUB DeviceID Prefix
            String iothub_ep_name = this.addDeviceIDPrefix(ep_name);

            topic_string_list[0] = this.customizeTopic(this.m_iot_hub_coap_cmd_topic_base, iothub_ep_name, ep_type);
            for (int i = 0; i < m_num_coap_topics; ++i) {
                list[i] = new Topic(topic_string_list[i], QoS.AT_LEAST_ONCE);
            }
            topic_data = new HashMap<>();
            topic_data.put("topic_list", list);
            topic_data.put("topic_string_list", topic_string_list);
        }
        return topic_data;
    }
    
    // get our defaulted reply topic
    @Override
    public String getReplyTopic(String ep_name, String ep_type, String def) {
        // IOTHUB DeviceID Prefix
        String iothub_ep_name = this.addDeviceIDPrefix(ep_name);
        return this.customizeTopic(this.m_iot_hub_observe_notification_topic, iothub_ep_name, ep_type).replace(this.m_observation_key, this.m_cmd_response_key);
    }

    // final customization of a MQTT Topic...
    private String customizeTopic(String topic, String ep_name, String ep_type) {
        String cust_topic = topic.replace("__EPNAME__", ep_name);
        if (ep_type == null) {
            ep_type = this.getEndpointTypeFromEndpointName(ep_name);
        }
        if (ep_type != null) {
            cust_topic = cust_topic.replace("__DEVICE_TYPE__", ep_type);
            this.errorLogger().info("IoTHub Customized Topic: " + cust_topic);
        }
        else {
            // replace with "default"
            cust_topic = cust_topic.replace("__DEVICE_TYPE__", "default");
            
            // WARN
            this.errorLogger().warning("IoTHub Customized Topic (EPT UNK): " + cust_topic);
        }
        return cust_topic;
    }
    
    // we have to override the creation of the authentication hash.. it has to be dependent on a given endpoint name
    @Override
    public String createAuthenticationHash() {
        return Utils.createHash(this.prefValue("iot_event_hub_sas_token", this.m_suffix));
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
    
    // add a MQTT transport for a given endpoint - this is how MS IoTHub MQTT integration works... 
    private synchronized void createAndStartMQTTForEndpoint(String ep_name, String ep_type) {
        // IOTHUB DeviceID Prefix
        String iothub_ep_name = this.addDeviceIDPrefix(ep_name);

        if (this.mqtt(iothub_ep_name) == null) {
            // create a new MQTT Transport instance
            MQTTTransport mqtt = new MQTTTransport(this.errorLogger(), this.preferences());

            // MQTT username is based upon the device ID (endpoint_name)
            String username = this.orchestrator().preferences().valueOf("iot_event_hub_mqtt_username", this.m_suffix).replace("__IOT_EVENT_HUB__", this.m_iot_hub_name).replace("__EPNAME__", iothub_ep_name);

            // set the creds for this MQTT Transport instance
            mqtt.setClientID(iothub_ep_name);
            mqtt.setUsername(username);
            mqtt.setPassword(this.m_iot_hub_device_manager.createMQTTPassword(iothub_ep_name));

            // IoTHub only works with SSL... so force it
            mqtt.forceSSLUsage(true);

            // add it to the list indexed by the endpoint name... not the clientID...
            this.addMQTTTransport(iothub_ep_name, mqtt);

            // DEBUG
            this.errorLogger().info("IoTHub: connecting to MQTT for endpoint: " + iothub_ep_name + " type: " + ep_type + "...");

            // connect and start listening... 
            if (this.connect(iothub_ep_name) == true) {
                // DEBUG
                this.errorLogger().info("IoTHub: connected to MQTT. Creating and registering listener Thread for endpoint: " + iothub_ep_name + " type: " + ep_type);

                // ensure we only have 1 thread/endpoint
                if (this.m_mqtt_thread_list.get(iothub_ep_name) != null) {
                    TransportReceiveThread listener = (TransportReceiveThread) this.m_mqtt_thread_list.get(iothub_ep_name);
                    listener.disconnect();
                    this.m_mqtt_thread_list.remove(iothub_ep_name);
                }

                // create and start the listener
                TransportReceiveThread listener = new TransportReceiveThread(mqtt);
                listener.setOnReceiveListener(this);
                this.m_mqtt_thread_list.put(iothub_ep_name, listener);
                listener.start();
            }
            else {
                // unable to connect!
                this.errorLogger().critical("IoTHub: Unable to connect to MQTT for endpoint: " + iothub_ep_name + " type: " + ep_type);
                this.remove(iothub_ep_name);

                // ensure we only have 1 thread/endpoint
                if (this.m_mqtt_thread_list.get(iothub_ep_name) != null) {
                    TransportReceiveThread listener = (TransportReceiveThread) this.m_mqtt_thread_list.get(iothub_ep_name);
                    listener.disconnect();
                    this.m_mqtt_thread_list.remove(iothub_ep_name);
                }
            }
        }
        else {
            // already connected... just ignore
            this.errorLogger().info("IoTHub: already have connection for " + iothub_ep_name + " (OK)");
        }
    }

    // validate the MQTT Connection
    private synchronized boolean validateMQTTConnection(String ep_name, String ep_type) {
        // IOTHUB DeviceID Prefix
        String iothub_ep_name = this.addDeviceIDPrefix(ep_name);

        // see if we already have a connection for this endpoint...
        if (this.mqtt(iothub_ep_name) == null) {
            // create a MQTT connection for this endpoint... 
            this.createAndStartMQTTForEndpoint(iothub_ep_name, ep_type);
        }

        // return our connection status
        return this.isConnected(iothub_ep_name);
    }
    
    // Connection to IoTHub MQTT vs. generic MQTT...
    private boolean connect(String ep_name) {
        // IOTHUB Prefix 
        String iothub_ep_name = this.addDeviceIDPrefix(ep_name);

        // if not connected attempt
        if (!this.isConnected(iothub_ep_name)) {
            if (this.mqtt(iothub_ep_name).connect(this.m_mqtt_host, this.m_mqtt_port, iothub_ep_name, this.m_use_clean_session)) {
                this.orchestrator().errorLogger().info("IoTHub: Setting CoAP command listener...");
                this.mqtt(iothub_ep_name).setOnReceiveListener(this);
                this.orchestrator().errorLogger().info("IoTHub: connection completed successfully");
            }
        }
        else {
            // already connected
            this.orchestrator().errorLogger().info("IoTHub: Already connected (OK)...");
        }

        // return our connection status
        this.orchestrator().errorLogger().info("IoTHub: Connection status: " + this.isConnected(iothub_ep_name));
        return this.isConnected(iothub_ep_name);
    }
    
    // disconnect
    private void disconnect(String ep_name) {
        // IOTHUB DeviceID Prefix
        String iothub_ep_name = this.addDeviceIDPrefix(ep_name);
        if (this.isConnected(iothub_ep_name)) {
            this.mqtt(iothub_ep_name).disconnect(true);
        }
        this.remove(iothub_ep_name);
    }

    // are we connected
    private boolean isConnected(String ep_name) {
        // IOTHUB DeviceID Prefix
        String iothub_ep_name = this.addDeviceIDPrefix(ep_name);
        if (this.mqtt(iothub_ep_name) != null) {
            return this.mqtt(iothub_ep_name).isConnected();
        }
        return false;
    }
    
    // get the endpoint name from the MQTT topic
    @Override
    public String getEndpointNameFromTopic(String topic) {
        // format: devices/__EPNAME__/messages/devicebound/#
        return this.getTopicElement(topic, 1);
    }

    // get the CoAP verb from the MQTT topic
    @Override
    public String getCoAPVerbFromTopic(String topic) {
        // format: devices/__EPNAME__/messages/devicebound/coap_verb=put....
        return this.getTopicElement(topic, "coap_verb");
    }

    // get the CoAP URI from the MQTT topic
    @Override
    public String getResourceURIFromTopic(String topic) {
        // format: devices/__EPNAME__/messages/devicebound/coap_uri=....
        return this.getTopicElement(topic, "coap_uri");
    }
}
