/**
 * @file    GoogleCloudMQTTProcessor.java
 * @brief Google Cloud MQTT Peer Processor
 * @author Doug Anson
 * @version 1.0
 * @see
 *
 * Copyright 2018. ARM Ltd. All rights reserved.
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
package com.arm.connector.bridge.coordinator.processors.google;

import com.arm.connector.bridge.coordinator.processors.arm.GenericMQTTProcessor;
import com.arm.connector.bridge.coordinator.Orchestrator;
import com.arm.connector.bridge.coordinator.processors.interfaces.AsyncResponseProcessor;
import com.arm.connector.bridge.coordinator.processors.interfaces.PeerInterface;
import com.arm.connector.bridge.core.Utils;
import com.arm.connector.bridge.transport.HttpTransport;
import com.arm.connector.bridge.transport.MQTTTransport;
import com.arm.connector.bridge.core.Transport;
import com.arm.connector.bridge.core.TransportReceiveThread;
import com.google.api.client.googleapis.auth.oauth2.GoogleCredential;
import com.google.api.client.googleapis.javanet.GoogleNetHttpTransport;
import com.google.api.client.http.HttpRequestInitializer;
import com.google.api.client.json.JsonFactory;
import com.google.api.client.json.jackson2.JacksonFactory;
import com.google.api.services.pubsub.PubsubScopes;
import com.google.api.services.cloudiot.v1.CloudIot;
import com.google.api.services.pubsub.Pubsub;
import io.jsonwebtoken.JwtBuilder;
import io.jsonwebtoken.Jwts;
import io.jsonwebtoken.SignatureAlgorithm;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.Serializable;
import java.security.GeneralSecurityException;
import java.security.KeyFactory;
import java.security.NoSuchAlgorithmException;
import java.security.spec.InvalidKeySpecException;
import java.security.spec.PKCS8EncodedKeySpec;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.commons.codec.binary.Base64;
import org.fusesource.mqtt.client.QoS;
import org.fusesource.mqtt.client.Topic;

/**
 * Google Cloud peer processor based on MQTT
 *
 * @author Doug Anson
 */
public class GoogleCloudMQTTProcessor extends GenericMQTTProcessor implements Transport.ReceiveListener, PeerInterface, AsyncResponseProcessor {
    // Google Cloud IoT notifications get published to this topic:  /devices/{deviceID}/events
    private static String GOOGLE_CLOUDIOT_EVENT_TAG = "events";
    
    // Google Cloud IoT device state changes tag
    private static String GOOGLE_CLOUDIOT_STATE_TAG = "state";
    
    // SUBSCRIBE to these topics
    private String m_google_cloud_coap_config_topic = null;
    
    // PUBLISH to these topics
    private String m_google_cloud_coap_state_topic = null;
    private String m_google_cloud_observe_notification_topic = null;
    
    // keystore root directory
    private String m_keystore_rootdir = null;

    // GoogleCloud Device Manager
    private GoogleCloudDeviceManager m_google_cloud_gw_device_manager = null;
    
    // Client ID Template
    private String m_google_cloud_client_id_template = null;
    
    // GoogleCloud Project ID
    private String m_google_cloud_project_id = null;
    
    // GoogleCloud Region
    private String m_google_cloud_region = null;
    
    // GoogleCloud MQTT Host
    private String m_google_cloud_mqtt_host = null;
    
    // GoogleCloud MQTT Port
    private int m_google_cloud_mqtt_port = 0;
    
    // GoogleCloud MQTT Version
    private String m_google_cloud_mqtt_version = null;
    
    // Google Cloud Credential
    private GoogleCredential m_credential = null;
    
    // Google Cloud AUTH Cred
    private String m_google_cloud_auth_json = null;
    
    // Google CloudIoT instance
    private CloudIot m_cloud_iot = null;
    
    // Google Pubsub instance
    private Pubsub m_pub_sub = null;
    
    // Google CloudIoT Application Name
    private String m_google_cloud_application_name = null;
    
    // Google CloudIoT Registry Name
    private String m_google_cloud_registry_name = null;
    
    // Login status
    private boolean m_google_cloud_logged_in = false;
    
    // default JWT expiration length (in seconds)
    private int m_jwt_refresh_interval = (18 * 60* 60);     // JwT refresh interval: 18 hours
    private long m_jwt_expiration_secs = (23 * 60 * 60);    // JwT token expiration : 23 hours
    
    // JwT refresher Thread
    private HashMap<String,GoogleJwTRefresherThread> m_jwt_refesher_thread_list = null;

    // constructor (singleton)
    public GoogleCloudMQTTProcessor(Orchestrator manager, MQTTTransport mqtt, HttpTransport http) {
        this(manager, mqtt, null, http);
    }

    // constructor (with suffix for preferences)
    public GoogleCloudMQTTProcessor(Orchestrator manager, MQTTTransport mqtt, String suffix, HttpTransport http) {
        super(manager, mqtt, suffix, http);

        // GoogleCloud Processor Announce
        this.errorLogger().info("Google Cloud MQTT Processor ENABLED.");
        
        // domain and suffix setup
        this.m_mds_domain = manager.getDomain();
        this.m_suffix = suffix;
        
        // keystore root directory
        this.m_keystore_rootdir = this.orchestrator().preferences().valueOf("mqtt_keystore_basedir",this.m_suffix);

        // get the client ID template
        this.m_google_cloud_client_id_template = this.orchestrator().preferences().valueOf("google_cloud_client_id_template",this.m_suffix);
        
        // get our Google AUTH Json
        this.m_google_cloud_auth_json = this.orchestrator().preferences().valueOf("google_cloud_auth_json",this.m_suffix);

        // get the Project ID
        this.m_google_cloud_project_id = this.getProjectID(this.m_google_cloud_auth_json);
        
        // Google CloudIot Application Name
        this.m_google_cloud_application_name = this.getApplicationName(this.m_google_cloud_auth_json);
        
        // get the Region
        this.m_google_cloud_region = this.orchestrator().preferences().valueOf("google_cloud_region",this.m_suffix);
        
        // get the MQTT Host
        this.m_google_cloud_mqtt_host = this.orchestrator().preferences().valueOf("google_cloud_mqtt_host",this.m_suffix);
        
        // get the MQTT Port
        this.m_google_cloud_mqtt_port = this.orchestrator().preferences().intValueOf("google_cloud_mqtt_port",this.m_suffix);
        
        // get the MQTT Version
        this.m_google_cloud_mqtt_version = this.orchestrator().preferences().valueOf("google_cloud_mqtt_version",this.m_suffix);
        
        // Google CloudIot Registry Name
        this.m_google_cloud_registry_name = this.orchestrator().preferences().valueOf("google_cloud_registry_name",this.m_suffix);
        
        // Observation notification topic (PUBLISH)
        this.m_google_cloud_observe_notification_topic = this.orchestrator().preferences().valueOf("google_cloud_observe_notification_topic",this.m_suffix);
        
        // We receive commands/results that go down to mbed Cloud via the CONFIG topic
        this.m_google_cloud_coap_config_topic = this.orchestrator().preferences().valueOf("google_cloud_coap_config_topic", this.m_suffix);
        
        // we publish state changes that go up to Google Cloud IoT from mbed Cloud via the STATE topic
        this.m_google_cloud_coap_state_topic = this.orchestrator().preferences().valueOf("google_cloud_coap_state_topic", this.m_suffix);

        // Required Google Cloud format:  Event Tag redefinition
        this.m_observation_key = GOOGLE_CLOUDIOT_EVENT_TAG;
        
        // Required Google Cloud format: State Tag redefinition
        this.m_cmd_response_key = GOOGLE_CLOUDIOT_STATE_TAG;
        
        // DEBUG
        this.errorLogger().info("ProjectID: " + this.m_google_cloud_project_id + 
                                "Application Name: " + this.m_google_cloud_application_name + 
                                "Region: " + this.m_google_cloud_region);

        // initialize the topic root
        this.initTopicRoot("google_cloud_topic_root");
        
        // create the CloudIoT instance
        this.m_cloud_iot = this.createCloudIoTInstance();
        
        // create the Pubsub instance
        this.m_pub_sub = this.createPubSubInstance();
        
        // GoogleCloud Device Manager - will initialize and upsert our GoogleCloud bindings/metadata
        this.m_google_cloud_gw_device_manager = new GoogleCloudDeviceManager(this.orchestrator().errorLogger(), this.orchestrator().preferences(), this.m_suffix, http, this.orchestrator(), this.m_google_cloud_project_id, this.m_google_cloud_region, this.m_cloud_iot,this.m_pub_sub,this.m_observation_key,this.m_cmd_response_key,this.subscriptionsManager());

        // initialize our MQTT transport list
        this.initMQTTTransportList();
        
        // initialize the JwT refresher thread list
        this.m_jwt_refesher_thread_list = new HashMap<>();
    }
    
    // get the JwT refresh interval in seconds
    public long getJwTRefreshIntervalInSeconds() {
        return this.m_jwt_refresh_interval;
    }
    
    // Get our Google Project ID from the Auth JSON
    private String getProjectID(String auth_json) {
        Map parsed = this.jsonParser().parseJson(auth_json);
        if (parsed != null) {
            return (String)parsed.get("project_id");
        }
        return null;
    }
    
    // Get our Google Application Name from the Auth JSON
    private String getApplicationName(String auth_json) {
        String project_id = this.getProjectID(auth_json);
        if (project_id != null) {
            project_id = project_id.replace("-", " ");
            String parts[] = project_id.split(" ");
            return parts[0];
        }
        return null;
    }
    
    // OVERRIDE: process a new registration in GoogleCloud
    @Override
    protected synchronized void processRegistration(Map data, String key) {
        List endpoints = (List) data.get(key);
        for (int i = 0; endpoints != null && i < endpoints.size(); ++i) {
            Map endpoint = (Map) endpoints.get(i);
            List resources = (List) endpoint.get("resources");
            for (int j = 0; resources != null && j < resources.size(); ++j) {
                Map resource = (Map) resources.get(j);

                // re-subscribe
                if (this.subscriptionsManager().containsSubscription(this.m_mds_domain, (String) endpoint.get("ep"), (String) endpoint.get("ept"), (String) resource.get("path"))) {
                    // re-subscribe to this resource
                    this.orchestrator().subscribeToEndpointResource((String) endpoint.get("ep"), (String) resource.get("path"), false);

                    // SYNC: here we dont have to worry about Sync options - we simply dispatch the subscription to mDS and setup for it...
                    this.subscriptionsManager().removeSubscription(this.m_mds_domain, (String) endpoint.get("ep"), (String) endpoint.get("ept"), (String) resource.get("path"));
                    this.subscriptionsManager().addSubscription(this.m_mds_domain, (String) endpoint.get("ep"), (String) endpoint.get("ept"), (String) resource.get("path"), this.isObservableResource(resource));
                }

                // auto-subscribe
                else if (this.isObservableResource(resource) && this.m_auto_subscribe_to_obs_resources == true) {
                    // auto-subscribe to observable resources... if enabled.
                    this.orchestrator().subscribeToEndpointResource((String) endpoint.get("ep"), (String) resource.get("path"), false);

                    // SYNC: here we dont have to worry about Sync options - we simply dispatch the subscription to mDS and setup for it...
                    this.subscriptionsManager().removeSubscription(this.m_mds_domain, (String) endpoint.get("ep"), (String) endpoint.get("ept"), (String) resource.get("path"));
                    this.subscriptionsManager().addSubscription(this.m_mds_domain, (String) endpoint.get("ep"), (String) endpoint.get("ept"), (String) resource.get("path"), this.isObservableResource(resource));
                }
            }

            // invoke a GET to get the resource information for this endpoint... we will upsert the Metadata when it arrives
            this.retrieveEndpointAttributes(endpoint);
        }
    }

    // OVERRIDE: process a re-registration in GoogleCloud
    @Override
    public void processReRegistration(Map data) {
        List notifications = (List) data.get("reg-updates");
        for (int i = 0; notifications != null && i < notifications.size(); ++i) {
            Map entry = (Map) notifications.get(i);
            // DEBUG
            // this.errorLogger().info("GoogleCloud : CoAP re-registration: " + entry);
            if (this.hasSubscriptions((String) entry.get("ep")) == false) {
                // no subscriptions - so process as a new registration
                this.errorLogger().info("GoogleCloud : CoAP re-registration: no subscriptions.. processing as new registration...");
                this.processRegistration(data, "reg-updates");
            }
            else {
                // already subscribed (OK)
                this.errorLogger().info("GoogleCloud : CoAP re-registration: already subscribed (OK)");
            }
        }
    }

    // OVERRIDE: handle de-registrations for GoogleCloud
    @Override
    public String[] processDeregistrations(Map parsed) {
        String[] deregistration = super.processDeregistrations(parsed);
        for (int i = 0; deregistration != null && i < deregistration.length; ++i) {
            // DEBUG
            this.errorLogger().info("GoogleCloud : CoAP de-registration: " + deregistration[i]);

            // GoogleCloud add-on... 
            this.unsubscribe(deregistration[i]);

            // Remove from GoogleCloud
            this.deregisterDevice(deregistration[i]);
        }
        return deregistration;
    }
    
    // OVERRIDE: process a notification/observation in GoogleCloud
    @Override
    public void processNotification(Map data) {
        // DEBUG
        //this.errorLogger().info("processIncomingDeviceServerMessage(GoogleCloud)...");

        // get the list of parsed notifications
        List notifications = (List) data.get("notifications");
        for (int i = 0; notifications != null && i < notifications.size(); ++i) {
            // we have to process the payload... this may be dependent on being a string core type... 
            Map notification = (Map) notifications.get(i);

            // decode the Payload...
            String b64_coap_payload = (String) notification.get("payload");
            String decoded_coap_payload = Utils.decodeCoAPPayload(b64_coap_payload);

            // DEBUG
            //this.errorLogger().info("GoogleCloud: Decoded Payload: " + decoded_coap_payload);
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

            // get the path
            String path = (String) notification.get("path");

            // we will send the raw CoAP JSON... GoogleCloud can parse that... 
            String coap_raw_json = this.jsonGenerator().generateJson(notification);

            // strip off []...
            String coap_json_stripped = this.stripArrayChars(coap_raw_json);

            // get our endpoint name
            String ep_name = (String) notification.get("ep");

            // get our endpoint type
            String ep_type = this.getTypeFromEndpointName(ep_name);

            // encapsulate into a coap/device packet...
            String google_cloud_gw_coap_json = coap_json_stripped;

            // DEBUG
            this.errorLogger().info("GoogleCloud: CoAP notification (STR): " + google_cloud_gw_coap_json);

            // send to GoogleCloud...
            if (this.mqtt(ep_name) != null) {
                // do not use subdirectories for the topic... no "path" at the end...
                String topic = this.customizeTopic(this.m_google_cloud_observe_notification_topic,ep_name);
                
                // send the observation...
                boolean status = this.mqtt(ep_name).sendMessage(topic, google_cloud_gw_coap_json, QoS.AT_MOST_ONCE);
                if (status == true) {
                    // not connected
                    this.errorLogger().info("GoogleCloud: CoAP notification sent. SUCCESS");
                }
                else {
                    // send failed
                    this.errorLogger().warning("GoogleCloud: CoAP notification not sent. SEND FAILED");
                }
            }
            else {
                // not connected
                this.errorLogger().warning("GoogleCloud: CoAP notification not sent. NOT CONNECTED");
            }
        }
    }

    // create the endpoint GoogleCloud topic data
    private HashMap<String, Object> createEndpointTopicData(String ep_name, String ep_type) {
        HashMap<String, Object> topic_data = null;
        
        // these will be topics that we SUBSCRIBE to... hence CONFIG only for Google Cloud IoT
        if (this.m_google_cloud_coap_config_topic != null) {
            // config topic is the only one to listen on for Google
            Topic[] list = new Topic[1];
            String[] config_topic_str = { this.customizeTopic(this.m_google_cloud_coap_config_topic,ep_name) };
            list[0] = new Topic(config_topic_str[0], QoS.AT_LEAST_ONCE);
            topic_data = new HashMap<>();
            topic_data.put("topic_list", list);
            topic_data.put("topic_string_list", config_topic_str);
            topic_data.put("ep_type", ep_type);
        }
        return topic_data;
    }

    // final customization of our MQTT Topic...
    private String customizeTopic(String topic, String ep_name) {
        String cust_topic = topic.replace("__EPNAME__",this.m_google_cloud_gw_device_manager.mbedDeviceIDToGoogleDeviceID(ep_name));
        return cust_topic;
    }

    // CoAP command handler - processes CoAP commands coming over MQTT channel
    @Override
    public void onMessageReceive(String topic, String message) {
        // DEBUG
        this.errorLogger().info("GoogleCloud(CoAP Command): Topic: " + topic + " message: " + message);

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
        String response = this.orchestrator().processEndpointResourceOperation(coap_verb, ep_name, uri, value, options);

        // examine the response
        if (response != null && response.length() > 0) {
            // SYNC: We only process AsyncResponses from GET verbs... we dont sent HTTP status back through GoogleCloud.
            this.errorLogger().info("GoogleCloud(CoAP Command): Response: " + response);

            // AsyncResponse detection and recording...
            if (this.isAsyncResponse(response) == true) {
                // CoAP GET and PUT provides AsyncResponses...
                if (coap_verb.equalsIgnoreCase("get") == true || coap_verb.equalsIgnoreCase("put") == true) {
                    // its an AsyncResponse.. so record it...
                    this.recordAsyncResponse(response, coap_verb, this.mqtt(ep_name), this, topic, this.getReplyTopic(ep_name, this.getEndpointTypeFromEndpointName(ep_name), uri), message, ep_name, uri);
                }
                else {
                    // we ignore AsyncResponses to PUT,POST,DELETE
                    this.errorLogger().info("GoogleCloud(CoAP Command): Ignoring AsyncResponse for " + coap_verb + " (OK).");
                }
            }
            else if (coap_verb.equalsIgnoreCase("get")) {
                // not an AsyncResponse... so just emit it immediately... only for GET...
                this.errorLogger().info("GoogleCloud(CoAP Command): Response: " + response + " from GET... creating observation...");

                // we have to format as an observation...
                String observation = this.createObservation(coap_verb, ep_name, uri, response);

                // DEBUG
                this.errorLogger().info("GoogleCloud(CoAP Command): Sending Observation(GET): " + observation);

                // send the observation (a GET reply)...
                if (this.mqtt(ep_name) != null) {
                    // Google: we publish this to the STATE change topic in Google... 
                    String reply_topic = this.customizeTopic(this.m_google_cloud_coap_state_topic,ep_name);
                    boolean status = this.mqtt(ep_name).sendMessage(reply_topic, observation, QoS.AT_MOST_ONCE);
                    if (status == true) {
                        // success
                        this.errorLogger().info("GoogleCloud(CoAP Command): CoAP observation(get) sent. SUCCESS");
                    }
                    else {
                        // send failed
                        this.errorLogger().warning("GoogleCloud(CoAP Command): CoAP observation(get) not sent. SEND FAILED");
                    }
                }
                else {
                    // not connected
                    this.errorLogger().warning("GoogleCloud(CoAP Command): CoAP observation(get) not sent. NOT CONNECTED");
                }
            }
        }
    }

    // create an observation JSON as a response to a GET request...
    private String createObservation(String verb, String ep_name, String uri, String value) {
        Map notification = new HashMap<>();

        // needs to look like this:  {"path":"/303/0/5700","payload":"MjkuNzU\u003d","max-age":"60","ep":"350e67be-9270-406b-8802-dd5e5f20","value":"29.75"}    
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
                notification.put("payload", Base64.encodeBase64String(value.getBytes()));  // Base64 Encoded payload
            }
            else {
                notification.put("payload", Base64.encodeBase64String("0".getBytes()));    // Base64 Encoded payload
            }
            notification.put("method", verb);
        }

        // we will send the raw CoAP JSON... GoogleCloud can parse that... 
        String coap_raw_json = this.jsonGenerator().generateJson(notification);

        // strip off []...
        String coap_json_stripped = this.stripArrayChars(coap_raw_json);

        // encapsulate into a coap/device packet...
        String google_cloud_gw_coap_json = coap_json_stripped;

        // DEBUG
        this.errorLogger().info("GoogleCloud: CoAP notification(" + verb + " REPLY): " + google_cloud_gw_coap_json);

        // return the GoogleCloud-specific observation JSON...
        return google_cloud_gw_coap_json;
    }

    // default formatter for AsyncResponse replies
    @Override
    public String formatAsyncResponseAsReply(Map async_response, String verb) {
        // DEBUG
        this.errorLogger().info("GoogleCloud(" + verb + ") AsyncResponse: ID: " + async_response.get("id") + " response: " + async_response);

        if (verb != null && verb.equalsIgnoreCase("GET") == true) {
            try {
                // DEBUG
                this.errorLogger().info("GoogleCloud: CoAP AsyncResponse for GET: " + async_response);

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
                        this.errorLogger().info("GoogleCloud: Created(" + verb + ") GET observation: " + message);

                        // return the message
                        return message;
                    }
                }
            }
            catch (Exception ex) {
                // Error in creating the observation message from the AsyncResponse GET reply... 
                this.errorLogger().warning("GoogleCloud(GET): Exception in formatAsyncResponseAsReply(): ", ex);
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
                        this.errorLogger().info("GoogleCloud: Created(" + verb + ") PUT Observation: " + message);

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
                    this.errorLogger().info("GoogleCloud: Created(" + verb + ") PUT Observation: " + message);

                    // return message
                    return message;
                }
            }
            catch (Exception ex) {
                // Error in creating the observation message from the AsyncResponse PUT reply... 
                this.errorLogger().warning("GoogleCloud(PUT): Exception in formatAsyncResponseAsReply(): ", ex);
            }
        }

        // return null message
        return null;
    }

    // subscribe to the GoogleCloud MQTT topics
    private void subscribe_to_topics(String ep_name, Topic topics[]) {
        this.mqtt(ep_name).subscribe(topics);
    }

    // does this endpoint already have registered subscriptions?
    private boolean hasSubscriptions(String ep_name) {
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

    // register topics for CoAP commands
    private void subscribe(String ep_name, String ep_type) {
        if (ep_name != null && this.validateMQTTConnection(ep_name, ep_type)) {
            // DEBUG
            this.orchestrator().errorLogger().info("GoogleCloud: Subscribing to CoAP command topics for endpoint: " + ep_name + " type: " + ep_type);
            try {
                HashMap<String, Object> topic_data = this.createEndpointTopicData(ep_name, ep_type);
                if (topic_data != null) {
                    // get,put,post,delete enablement
                    this.m_endpoints.remove(ep_name);
                    this.m_endpoints.put(ep_name, topic_data);
                    this.setEndpointTypeFromEndpointName(ep_name, ep_type);
                    this.subscribe_to_topics(ep_name, (Topic[]) topic_data.get("topic_list"));
                }
                else {
                    this.orchestrator().errorLogger().warning("GoogleCloud: GET/PUT/POST/DELETE topic data NULL. GET/PUT/POST/DELETE disabled");
                }
            }
            catch (Exception ex) {
                this.orchestrator().errorLogger().info("GoogleCloud: Exception in subscribe for " + ep_name + " : " + ex.getMessage());
            }
        }
        else {
            this.orchestrator().errorLogger().info("GoogleCloud: NULL Endpoint name in subscribe()... ignoring...");
        }
    }

    // un-register topics for CoAP commands
    private boolean unsubscribe(String ep_name) {
        boolean unsubscribed = false;
        if (ep_name != null && this.mqtt(ep_name) != null) {
            // DEBUG
            this.orchestrator().errorLogger().info("GoogleCloud: Un-Subscribing to CoAP command topics for endpoint: " + ep_name);
            try {
                HashMap<String, Object> topic_data = (HashMap<String, Object>) this.m_endpoints.get(ep_name);
                if (topic_data != null) {
                    // unsubscribe...
                    this.mqtt(ep_name).unsubscribe((String[]) topic_data.get("topic_string_list"));
                }
                else {
                    // not in subscription list (OK)
                    this.orchestrator().errorLogger().info("GoogleCloud: Endpoint: " + ep_name + " not in subscription list (OK).");
                    unsubscribed = true;
                }
            }
            catch (Exception ex) {
                this.orchestrator().errorLogger().info("GoogleCloud: Exception in unsubscribe for " + ep_name + " : " + ex.getMessage());
            }
        }
        else if (this.mqtt(ep_name) != null) {
            this.orchestrator().errorLogger().info("GoogleCloud: NULL Endpoint name... ignoring unsubscribe()...");
            unsubscribed = true;
        }
        else {
            this.orchestrator().errorLogger().info("GoogleCloud: No MQTT connection for " + ep_name + "... ignoring unsubscribe()...");
            unsubscribed = true;
        }

        // clean up
        if (ep_name != null) {
            this.m_endpoints.remove(ep_name);
        }

        // return the unsubscribe status
        return unsubscribed;
    }

    // process new device registration
    @Override
    protected synchronized Boolean registerNewDevice(Map message) {
        if (this.m_google_cloud_gw_device_manager != null) {
            // DEBUG
            this.errorLogger().info("GoogleCloud: Registering new device: " + (String) message.get("ep") + " type: " + (String) message.get("ept"));
            
            // save off the endpoint type/ep name
            this.setEndpointTypeFromEndpointName((String)message.get("ep"),(String)message.get("ept"));

            // create the device in GoogleCloud
            Boolean success = this.m_google_cloud_gw_device_manager.registerNewDevice(message);

            // if successful, validate (i.e. add...) an MQTT Connection
            if (success == true) {
                this.validateMQTTConnection((String) message.get("ep"), (String) message.get("ept"));
            }

            // return status
            return success;
        }
        return false;
    }

    // process device de-registration
    @Override
    protected synchronized Boolean deregisterDevice(String device) {
        if (this.m_google_cloud_gw_device_manager != null) {
            // DEBUG
            this.errorLogger().info("deregisterDevice(GoogleCloud): deregistering device: " + device);

            // disconnect, remove the threaded listener... 
            if (this.m_mqtt_thread_list.get(device) != null) {
                try {
                    // stop the refresher list
                    this.stopJwTRefresherThread(device);
                    
                    // disconnect
                    this.m_mqtt_thread_list.get(device).disconnect();
                }
                catch (Exception ex) {
                    // note but continue...
                    this.errorLogger().warning("deregisterDevice(GoogleCloud): exception during deregistration", ex);
                }
                this.m_mqtt_thread_list.remove(device);
            }

            // also remove MQTT Transport instance too...
            this.disconnect(device);

            // remove the device from GoogleCloud
            if (this.m_google_cloud_gw_device_manager.deregisterDevice(device) == false) {
                this.errorLogger().warning("deregisterDevice(GoogleCloud): unable to de-register device from GoogleCloud...");
            }
        }
        return true;
    }
    
    // create our specific Google Cloud JWT for a device
    public String createGoogleCloudJWT(String ep_name) throws IOException {
        try {
            // use the appropriate keyfile
            Date now = new Date();
            long expiration_seconds = (now.getTime()/1000) + this.m_jwt_expiration_secs; // 23 hours from now... expire.
            Date expire_date = new Date(expiration_seconds*1000); // must be in ms
            JwtBuilder jwtBuilder =
                Jwts.builder()
                    .setIssuedAt(now)
                    .setExpiration(expire_date)
                    .setAudience(this.m_google_cloud_project_id);

            byte[] privKey = Utils.readRSAKeyforDevice(this.errorLogger(),this.m_keystore_rootdir, ep_name, true); // priv key read
            if (privKey != null && privKey.length > 1) {
                PKCS8EncodedKeySpec spec = new PKCS8EncodedKeySpec(privKey);
                KeyFactory kf = KeyFactory.getInstance("RSA");
                return jwtBuilder.signWith(SignatureAlgorithm.RS256, kf.generatePrivate(spec)).compact();
            }
            else {
                // invalid key read
                this.errorLogger().warning("createGoogleCloudJWT: WARNING: input key is null or has length 1");
            }
        }
        catch (InvalidKeySpecException | NoSuchAlgorithmException ex) {
            // error creating JWT
            this.errorLogger().critical("createGoogleCloudJWT: Exception in creating JWT: " + ex.getMessage());
        }
        return null;
    }
    
    // Refresh the JwT for a given endpoint
    public void refreshJwTForEndpoint(String ep_name) {
        try {
            // create a new JwT
            String jwt = this.createGoogleCloudJWT(ep_name);

            // refresh the Google devices' JwT via CloudIoT API
            if (this.mqtt(ep_name) != null) {
                // disconnect MQTT
                this.disconnect(ep_name);
                
                // create a new MQTT connection
                MQTTTransport mqtt = new MQTTTransport(this.errorLogger(), this.preferences());
                
                // add it to the list indexed by the endpoint name... not the clientID...
                this.addMQTTTransport(ep_name, mqtt);
                        
                // re-connect
                boolean connected = this.connect(ep_name,this.createGoogleCloudMQTTclientID(ep_name),jwt);
                if (connected == true) {
                    // success! new JwT active...
                    this.errorLogger().info("refreshJwTForEndpoint: reconnected with new JwT (SUCCESS)");
                }
                else {
                    // failure to reconnect
                    this.errorLogger().critical("refreshJwTForEndpoint: FAILED to reconnect with new JwT!!");
                }
            }
        }
        catch(IOException ex) {
            // error creating JWT
            this.errorLogger().critical("refreshJwTForEndpoint: Exception in refreshing JWT: " + ex.getMessage());
        }
    }
    
    // End the JwT refresher thread
    public void stopJwTRefresherThread(String ep_name) {
        GoogleJwTRefresherThread doomed = this.m_jwt_refesher_thread_list.get(ep_name);
        if (doomed != null) {
            // DEBUG
            this.errorLogger().warning("stopJwTRefresherThread: Stopping JwT Refresher for: " + ep_name);
            
            // remove from ThreadList
            this.m_jwt_refesher_thread_list.remove(ep_name);
            
            try {
                // stop the event loop in the thread
                doomed.haltThread();
                
                // re-join the thread to end it
                doomed.join();
            }
            catch (InterruptedException ex) {
                // silent
            }
        }
    }
    
    // create our specific Google Cloud ClientID
    private String createGoogleCloudMQTTclientID(String ep_name) {
        // create the Google Cloud MQTT connection client ID
        return this.m_google_cloud_client_id_template
                .replace("__PROJECT_ID__", this.m_google_cloud_project_id)
                .replace("__CLOUD_REGION__", this.m_google_cloud_region)
                .replace("__REGISTRY_NAME__", this.m_google_cloud_registry_name)
                .replace("__EPNAME__",this.m_google_cloud_gw_device_manager.mbedDeviceIDToGoogleDeviceID(ep_name));
    }

    // add a MQTT transport for a given endpoint - this is how MS GoogleCloud MQTT integration works... 
    private synchronized void createAndStartMQTTForEndpoint(String ep_name, String ep_type) {
        try {
            // we may already have a connection established for this endpoint... if so, we just ignore...
            if (this.mqtt(ep_name) == null) {
                // no connection exists already... so... go get our endpoint details
                HashMap<String, Serializable> ep = this.m_google_cloud_gw_device_manager.getEndpointDetails(ep_name);
                if (ep != null) {
                    // create a new MQTT Transport instance for our endpoint
                    MQTTTransport mqtt = new MQTTTransport(this.errorLogger(), this.preferences());
                    if (mqtt != null) {
                        // ClientID creation for Google Cloud MQTT
                        String client_id = this.createGoogleCloudMQTTclientID(ep_name);
                        
                        // JWT creation for Google Cloud MQTT Authentication
                        String jwt = this.createGoogleCloudJWT(ep_name);

                        // add it to the list indexed by the endpoint name... not the clientID...
                        this.addMQTTTransport(ep_name, mqtt);

                        // DEBUG
                        this.errorLogger().info("GoogleCloud: connecting to MQTT for endpoint: " + ep_name + " type: " + ep_type + "...");

                        // connect and start listening... 
                        if (this.connect(ep_name, client_id, jwt) == true) {
                            // DEBUG
                            this.errorLogger().info("GoogleCloud: connected to MQTT. Creating and registering listener Thread for endpoint: " + ep_name + " type: " + ep_type);

                            // ensure we only have 1 thread/endpoint
                            if (this.m_mqtt_thread_list.get(ep_name) != null) {
                                TransportReceiveThread listener = (TransportReceiveThread) this.m_mqtt_thread_list.get(ep_name);
                                listener.disconnect();
                                this.m_mqtt_thread_list.remove(ep_name);
                            }

                            // create and start the listener
                            TransportReceiveThread listener = new TransportReceiveThread(mqtt);
                            listener.setOnReceiveListener(this);
                            this.m_mqtt_thread_list.put(ep_name, listener);
                            listener.start();
                            
                            // Also start a JwT refresher
                            GoogleJwTRefresherThread jwt_refresher = new GoogleJwTRefresherThread(this,ep_name);
                            this.m_jwt_refesher_thread_list.put(ep_name,jwt_refresher);
                            jwt_refresher.start();
                        }
                        else {
                            // unable to connect!
                            this.errorLogger().critical("GoogleCloud: Unable to connect to MQTT for endpoint: " + ep_name + " type: " + ep_type);
                            this.remove(ep_name);

                            // ensure we only have 1 thread/endpoint
                            if (this.m_mqtt_thread_list.get(ep_name) != null) {
                                TransportReceiveThread listener = (TransportReceiveThread) this.m_mqtt_thread_list.get(ep_name);
                                listener.disconnect();
                                this.m_mqtt_thread_list.remove(ep_name);
                            }
                        }
                    }
                    else {
                        // unable to allocate MQTT connection for our endpoint
                        this.errorLogger().critical("GoogleCloud: ERROR. Unable to allocate MQTT connection for: " + ep_name);
                    }
                }
                else {
                    // unable to find endpoint details
                    this.errorLogger().warning("GoogleCloud: unable to find endpoint details for: " + ep_name + "... ignoring...");
                }
            }
            else {
                // already connected... just ignore
                this.errorLogger().info("GoogleCloud: already have connection for " + ep_name + " (OK)");
            }
        }
        catch (IOException ex) {
            // exception caught... capture and note the stack trace
            this.errorLogger().critical("GoogleCloud: createAndStartMQTTForEndpoint(): exception: " + ex.getMessage() + " endpoint: " + ep_name, ex);
        }
    }
    
    // AsyncResponse response processor
    @Override
    public boolean processAsyncResponse(Map endpoint) {
        // with the attributes added, we finally create the device in Watson IoT
        this.completeNewDeviceRegistration(endpoint);

        // return our processing status
        return true;
    }
    
    // get our defaulted reply topic
    @Override
    public String getReplyTopic(String ep_name, String ep_type, String def) {
        return this.customizeTopic(this.m_google_cloud_observe_notification_topic, ep_name).replace(this.m_observation_key, this.m_cmd_response_key);
    }

    // we have to override the creation of the authentication hash.. it has to be dependent on a given endpoint name
    @Override
    public String createAuthenticationHash() {
        return Utils.createHash(this.prefValue("google_cloud_gw_sas_token", this.m_suffix));
    }
    
    // get the endpoint name from the MQTT topic
    @Override
    public String getEndpointNameFromTopic(String topic) {
        // format: mbed/__COMMAND_TYPE__/__DEVICE_TYPE__/__EPNAME__/<uri path>
        return null;                                   // unused
    }

    // get the CoAP verb from the MQTT topic
    @Override
    public String getCoAPVerbFromTopic(String topic) {
        // format: mbed/__COMMAND_TYPE__/__DEVICE_TYPE__/__EPNAME__/<uri path>
        return null;                                   // unused
    }

    // get the CoAP URI from the MQTT topic
    private String getCoAPURIFromTopic(String topic) {
        // format: mbed/__COMMAND_TYPE__/__DEVICE_TYPE__/__EPNAME__/<uri path>
        return null;                               // unused
    }

    // get the endpoint type from the endpoint name
    private String getTypeFromEndpointName(String ep_name) {
        String ep_type = null;

        HashMap<String, Object> entry = (HashMap<String, Object>) this.m_endpoints.get(ep_name);
        if (entry != null) {
            ep_type = (String) entry.get("ep_type");
        }

        return ep_type;
    }

    // discover the endpoint attributes
    private void retrieveEndpointAttributes(Map endpoint) {
        // DEBUG
        this.errorLogger().info("GoogleCloud: Requesting Device Metadata for: " + endpoint);

        // pre-populate the new endpoint with initial values for registration
        this.orchestrator().pullDeviceMetadata(endpoint, this);
    }

    // complete processing of adding the new device
    private void completeNewDeviceRegistration(Map endpoint) {
        try {
            // create the device in GoogleCloud
            this.errorLogger().info("completeNewDeviceRegistration: calling registerNewDevice(): " + endpoint);
            this.registerNewDevice(endpoint);
            this.errorLogger().info("completeNewDeviceRegistration: registerNewDevice() completed");
        }
        catch (Exception ex) {
            this.errorLogger().warning("completeNewDeviceRegistration: caught exception in registerNewDevice(): " + endpoint, ex);
        }

        try {
            // subscribe for GoogleCloud as well..
            this.errorLogger().info("completeNewDeviceRegistration: calling subscribe(): " + endpoint);
            this.subscribe((String) endpoint.get("ep"), (String) endpoint.get("ept"));
            this.errorLogger().info("completeNewDeviceRegistration: subscribe() completed");
        }
        catch (Exception ex) {
            this.errorLogger().warning("completeNewDeviceRegistration: caught exception in subscribe(): " + endpoint, ex);
        }
    }
    
    // validate the MQTT Connection
    private synchronized boolean validateMQTTConnection(String ep_name, String ep_type) {
        // create a MQTT connection for this endpoint... 
        this.createAndStartMQTTForEndpoint(ep_name, ep_type);

        // return our connection status
        return this.isConnected(ep_name);
    }
    
    // Connection to GoogleCloud MQTT vs. generic MQTT...
    private boolean connect(String ep_name, String client_id, String jwt) {
        // if not connected attempt
        if (!this.isConnected(ep_name)) {
            // Set our Username and PW for Google Cloud MQTT
            this.mqtt(ep_name).setUsername("ignored");      // unused
            this.mqtt(ep_name).setPassword(jwt);            // JWT in string form
            
            // MQTT version set must also be explicit
            this.mqtt(ep_name).setMQTTVersion(this.m_google_cloud_mqtt_version);
            
            // MQTT must use SSL
            this.mqtt(ep_name).useSSLConnection(true);
            
            // Connect to the Google MQTT Service
            if (this.mqtt(ep_name).connect(this.m_google_cloud_mqtt_host,this.m_google_cloud_mqtt_port,client_id,this.m_use_clean_session,ep_name)) {
                this.orchestrator().errorLogger().info("GoogleCloud: Setting CoAP command listener...");
                this.mqtt(ep_name).setOnReceiveListener(this);
                this.orchestrator().errorLogger().info("GoogleCloud: connection completed successfully");
            }
        }
        else {
            // already connected
            this.orchestrator().errorLogger().info("GoogleCloud: Already connected (OK)...");
        }

        // return our connection status
        this.orchestrator().errorLogger().info("GoogleCloud: Connection status: " + this.isConnected(ep_name));
        return this.isConnected(ep_name);
    }
    
    // are we connected
    private boolean isConnected(String ep_name) {
        if (this.mqtt(ep_name) != null) {
            return this.mqtt(ep_name).isConnected();
        }
        return false;
    }
    
    // disconnect
    private void disconnect(String ep_name) {
        if (this.isConnected(ep_name)) {
            this.mqtt(ep_name).disconnect(true);
        }
        this.remove(ep_name);
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
    
    // create our CloudIoT instance
    private CloudIot createCloudIoTInstance() {
        CloudIot inst = null;
        
        // Log into Google Cloud
        this.m_google_cloud_logged_in = this.googleCloudLogin(this.m_google_cloud_project_id, this.m_google_cloud_auth_json);
        if (this.m_google_cloud_logged_in == true) {
            try {
                // JSON factory
                JsonFactory jsonFactory = JacksonFactory.getDefaultInstance();

                // setup the Http wrapper
                HttpRequestInitializer init = new RetryHttpInitializerWrapper(this.m_credential);
            
                // create the CloudIot instance
                inst = new CloudIot.Builder(GoogleNetHttpTransport.newTrustedTransport(),jsonFactory, init)
                        .setApplicationName(this.m_google_cloud_application_name)
                        .build();
            } 
            catch (GeneralSecurityException | IOException ex) {
                this.errorLogger().critical("Google: Unable to create CloudIot instance: " + ex.getMessage());
                inst = null;
            }
        }
        
        // return our instance
        return inst;
    }
    
    // create our Pubsub instance
    private Pubsub createPubSubInstance() {
        Pubsub inst = null;
        
        // only if logged in...
        if (this.m_google_cloud_logged_in == true) {
            try {
                // JSON factory
                JsonFactory jsonFactory = JacksonFactory.getDefaultInstance();
                
                // setup the Http wrapper
                HttpRequestInitializer init = new RetryHttpInitializerWrapper(this.m_credential);
                
                // create the Pubsub instance
                inst = new Pubsub.Builder(GoogleNetHttpTransport.newTrustedTransport(),jsonFactory, init)
                                 .setApplicationName(this.m_google_cloud_application_name)
                                 .build();
            }
            catch (GeneralSecurityException | IOException ex) {
                this.errorLogger().critical("Google: Unable to create Pubsub instance: " + ex.getMessage());
                inst = null;
            }
        }
        
        // return our instance
        return inst;
    }
    
    // log into the Google Cloud as a Service Account
    private boolean googleCloudLogin(String project_id,String auth_json) {
        boolean success = false;
        String edited_auth_json = null;
        
        try {
            // announce login
            this.errorLogger().warning("googleCloudLogin(): logging into project_id: " + project_id + "...");
            
            // remove \\00A0 as it can be copied during config setting of the auth json by the configurator...
            // hex(A0) = dec(160)... just replace with an ordinary space... that will make Google's JSON parser happy...
            edited_auth_json = com.arm.connector.bridge.core.Utils.replaceAllCharOccurances(auth_json,(char)160,' ');
            
            // DEBUG
            //this.errorLogger().info("googleCloudLogin():AUTH:" + edited_auth_json);
            
            // Build service account credential.
            this.m_credential = GoogleCredential.fromStream(new ByteArrayInputStream(edited_auth_json.getBytes()));
            
            // add scopes
            if (this.m_credential.createScopedRequired()) {
                this.m_credential = this.m_credential.createScoped(PubsubScopes.all());
            }
            
            // success!
            success = true;
            
            // DEBUG
            this.errorLogger().warning("googleCloudLogin(): LOGIN SUCCESSFUL. project_id: " + project_id);
        }
        catch (com.google.api.client.googleapis.json.GoogleJsonResponseException ex) {
            // caught exception during login
            this.errorLogger().warning("googleCloudLogin(GoogleJsonResponseException): Unable to log into Google Cloud: " + ex.getMessage());
        }
        catch (IOException ex) {
            // caught exception during login
            this.errorLogger().warning("googleCloudLogin(): Unable to log into Google Cloud: " + ex.getMessage());
            success = false;
        }
        
        // return our status
        return success;
    }
}
