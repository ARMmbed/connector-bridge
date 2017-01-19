/**
 * @file GoogleCloudProcessor.java
 * @brief Google Cloud Processor
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
package com.arm.connector.bridge.coordinator.processor.google;

import com.arm.connector.bridge.coordinator.Orchestrator;
import com.arm.connector.bridge.coordinator.processors.core.Processor;
import com.arm.connector.bridge.coordinator.processors.interfaces.GenericSender;
import com.arm.connector.bridge.coordinator.processors.interfaces.PeerInterface;
import com.arm.connector.bridge.core.Utils;
import com.google.api.client.googleapis.auth.oauth2.GoogleCredential;
import com.google.api.client.googleapis.javanet.GoogleNetHttpTransport;
import com.google.api.client.http.javanet.NetHttpTransport;
import com.google.api.client.json.JsonFactory;
import com.google.api.client.json.jackson2.JacksonFactory;
import com.google.api.services.plus.Plus;
import com.google.api.services.plus.PlusScopes;
import com.google.cloud.pubsub.PubSub;
import com.google.cloud.pubsub.PubSubOptions;
import com.google.cloud.pubsub.Topic;
import com.google.cloud.pubsub.TopicInfo;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.security.GeneralSecurityException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Google Cloud peer processor
 *
 * @author Doug Anson
 */
public class GoogleCloudProcessor extends Processor implements PeerInterface, GenericSender {
    private Plus m_plus = null;
    private GoogleCredential m_credential = null;
    private NetHttpTransport m_google_http = null;
    private JsonFactory m_google_json = null;
    private PubSub m_pubsub = null;
    private String m_app_name = null;
    private String m_auth_json = null;
    private boolean m_logged_in = false;
    private String m_google_cloud_observe_notification_topic = null;
    private String m_google_cloud_coap_cmd_topic_get = null;
    private String m_google_cloud_coap_cmd_topic_put = null;
    private String m_google_cloud_coap_cmd_topic_post = null;
    private String m_google_cloud_coap_cmd_topic_delete = null;
    private HashMap<String, String> m_google_cloud_endpoint_type_list = null;
    
    private Topic  m_observe_topic = null;
    
    private HashMap<String, Object> m_google_cloud_gw_endpoints = null;

    // (OPTIONAL) Factory method for initializing the Sample 3rd Party peer
    public static GoogleCloudProcessor createPeerProcessor(Orchestrator manager) {
        // create me
        GoogleCloudProcessor me = new GoogleCloudProcessor(manager);

        // return me
        return me;
    }
    
    // constructor
    public GoogleCloudProcessor(Orchestrator manager) {
        this(manager,null);
    }
    
    // constructor
    public GoogleCloudProcessor(Orchestrator manager, String suffix) {
        super(manager, suffix);
        this.m_mds_domain = manager.getDomain();
        this.m_suffix = suffix;
        
        // initialize the topic root
        this.initTopicRoot("google_cloud_topic_root");
        
        // auto-subscribe behavior
        this.initAutoSubscribe("google_cloud_obs_auto_subscribe");
        
        // Google Cloud peer Processor Announce
        this.errorLogger().info("Google Cloud Processor ENABLED.");
        
        // create endpoint name/endpoint type map
        this.m_google_cloud_endpoint_type_list = new HashMap<>();
        
        // Observation notification topic
        this.m_google_cloud_observe_notification_topic = this.orchestrator().preferences().valueOf("google_cloud_observe_notification_topic", this.m_suffix);

        // if unified format enabled, observation == notify
        if (this.unifiedFormatEnabled()) {
            this.m_google_cloud_observe_notification_topic = this.m_google_cloud_observe_notification_topic.replace("observation", this.m_observation_key);
        }
        
        // Send CoAP commands back through mDS into the endpoint via these Topics... 
        this.m_google_cloud_coap_cmd_topic_get = this.orchestrator().preferences().valueOf("google_cloud_coap_cmd_topic", this.m_suffix).replace("__TOPIC_ROOT__", this.getTopicRoot()).replace("__COMMAND_TYPE__", "get");
        this.m_google_cloud_coap_cmd_topic_put = this.orchestrator().preferences().valueOf("google_cloud_coap_cmd_topic", this.m_suffix).replace("__TOPIC_ROOT__", this.getTopicRoot()).replace("__COMMAND_TYPE__", "put");
        this.m_google_cloud_coap_cmd_topic_post = this.orchestrator().preferences().valueOf("google_cloud_coap_cmd_topic", this.m_suffix).replace("__TOPIC_ROOT__", this.getTopicRoot()).replace("__COMMAND_TYPE__", "post");
        this.m_google_cloud_coap_cmd_topic_delete = this.orchestrator().preferences().valueOf("google_cloud_coap_cmd_topic", this.m_suffix).replace("__TOPIC_ROOT__", this.getTopicRoot()).replace("__COMMAND_TYPE__", "delete");

        // get our Google info
        this.m_app_name = this.orchestrator().preferences().valueOf("google_cloud_app_name",this.m_suffix);
        this.m_auth_json = this.orchestrator().preferences().valueOf("google_cloud_auth_json",this.m_suffix);
        
        // Log into Google Cloud
        //this.m_logged_in = this.googleCloudLogin(this.m_app_name, this.m_auth_json);
        
        // Create topics
        //this.googleCloudCreateTopics();
    }
    
    // get the endpoint type from the endpoint name
    @Override
    protected String getEndpointTypeFromEndpointName(String ep_name) {
        String t = super.getEndpointTypeFromEndpointName(ep_name);
        if (t != null) return t;
        return this.m_google_cloud_endpoint_type_list.get(ep_name);
    }

    // set the endpoint type from the endpoint name
    protected void setEndpointTypeFromEndpointName(String ep_name, String ep_type) {
        this.m_google_cloud_endpoint_type_list.put(ep_name, ep_type);
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
            this.errorLogger().info("processNewRegistration(Google Cloud) : Publishing new registration topic: " + topic + " message:" + message);
            this.sendMessage(topic, message);

            // send it also raw... over a subtopic
            topic = this.createNewRegistrationTopic((String) endpoint.get("ept"), (String) endpoint.get("ep"));
            message = this.jsonGenerator().generateJson(endpoint);

            // DEBUG
            this.errorLogger().info("processNewRegistration(Google Cloud) : Publishing new registration topic: " + topic + " message:" + message);
            this.sendMessage(topic, message);

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
                    this.errorLogger().info("processReRegistration(Google Cloud) : CoAP re-registration: " + endpoint + " Resource: " + resource);
                    if (this.subscriptionsList().containsSubscription(this.m_mds_domain, (String) endpoint.get("ep"), (String) endpoint.get("ept"), (String) resource.get("path")) == false) {
                        this.errorLogger().info("processReRegistration(Google Cloud) : CoAP re-registering OBS resources for: " + endpoint + " Resource: " + resource);
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
            this.m_google_cloud_endpoint_type_list.remove(deregistrations[i]);
        }
        return deregistrations;
    }
    
    // process an observation
    @Override
    public void processNotification(Map data) {
        // DEBUG
        //this.errorLogger().info("processNotification(Google Cloud)...");

        // get the list of parsed notifications
        List notifications = (List) data.get("notifications");
        for (int i = 0; notifications != null && i < notifications.size(); ++i) {
            Map notification = (Map) notifications.get(i);

            // decode the Payload...
            String b64_coap_payload = (String) notification.get("payload");
            String decoded_coap_payload = Utils.decodeCoAPPayload(b64_coap_payload);

            // DEBUG
            //this.errorLogger().info("processNotification(Google Cloud): Decoded Payload: " + decoded_coap_payload);
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

                // DEBUG
                this.errorLogger().info("processNotification(Google Cloud): Active subscription for ep_name: " + ep_name + " ep_type: " + ep_type + " uri: " + uri);
                this.errorLogger().info("processNotification(Google Cloud): Publishing notification: payload: " + coap_json + " topic: " + topic);

                // publish to Google Cloud...
                // XXXX this.mqtt().sendMessage(topic, coap_json);
            }
            else {
                // no active subscription present - so note but do not send
                this.errorLogger().info("processNotification(Google Cloud): no active subscription for ep_name: " + ep_name + " ep_type: " + ep_type + " uri: " + uri + "... dropping notification...");
            }
        }
    }
    
    // Create the authentication hash
    @Override
    public String createAuthenticationHash() {
        // just create a hash of the AUTH JSON... 
        return Utils.createHash(this.m_auth_json);
    }
    
    // record an async response to process later (Google Cloud Peer)
    private void recordAsyncResponse(String response, String coap_verb, String response_topic, String message, String ep_name, String uri) {
        this.asyncResponseManager().recordAsyncResponse(response, coap_verb, this, this, response_topic, null, message, ep_name, uri);
    }
    
    // GenericSender Implementation: send a message
    @Override
    public void sendMessage(String to, String message) {
        // send a message over Google Cloud...
        this.errorLogger().info("Sending Message to: " + to + " message: " + message);
    }
    
    // log into the Google Cloud as a Service Account
    private boolean googleCloudLogin(String appName,String auth_json) {
        boolean success = false;
        String edited_auth_json = null;
        
        try {
            // DEBUG
            this.errorLogger().info("googleCloudLogin(): logging in...");
            
            // remove \\00A0 as it can be copied during config setting of the auth json by the configurator...
            // hex(A0) = dec(160)... just replace with an ordinary space... that will make Google's JSON parser happy...
            edited_auth_json = Utils.my_replace(auth_json,(char)160,' ');
            
            // DEBUG
            //this.errorLogger().info("googleCloudLogin():AUTH:" + edited_auth_json);
            
            this.m_google_http = GoogleNetHttpTransport.newTrustedTransport();
            this.m_google_json = JacksonFactory.getDefaultInstance();
            
            // Build service account credential.
            this.m_credential = GoogleCredential.fromStream(new ByteArrayInputStream(edited_auth_json.getBytes()))
                                                .createScoped(Collections.singleton(PlusScopes.PLUS_ME));

            // Set up global Plus instance.
            this.m_plus = new Plus.Builder(this.m_google_http,this.m_google_json,this.m_credential)
                                        .setApplicationName(appName)
                                        .build();
            
            // success!
            success = true;
            
            // DEBUG
            this.errorLogger().info("googleCloudLogin(): LOGIN SUCCESS.");
        }
        catch (GeneralSecurityException | IOException ex) {
            // caught exception during login
            this.errorLogger().critical("googleCloudLogin(): Unable to log into Google Cloud: " + ex.getMessage(), ex);
            success = false;
        }
        
        // return our status
        return success;
    }
    
    // Create the GoogleCloud topics
    private void googleCloudCreateTopics() {
        if (this.m_pubsub != null) {
            // Create the Observation Topic
            this.errorLogger().info("googleCloudCreateTopics: Creating Observation Topic: " + this.m_google_cloud_observe_notification_topic);
            this.m_observe_topic = this.m_pubsub.create(TopicInfo.of(this.m_google_cloud_observe_notification_topic));
        }
        else {
            // no pubsub instance
            this.errorLogger().warning("googleCloudCreateTopics: no pubsub instance... unable to create topics");
        }
    }
}
