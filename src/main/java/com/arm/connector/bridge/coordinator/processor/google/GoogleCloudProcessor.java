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
import com.arm.connector.bridge.coordinator.processors.core.PeerProcessor;
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
public class GoogleCloudProcessor extends PeerProcessor implements PeerInterface, GenericSender {
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
        
        // Google Cloud peer PeerProcessor Announce
        this.errorLogger().info("Google Cloud Processor ENABLED.");
        
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
    
    // Create the authentication hash
    @Override
    public String createAuthenticationHash() {
        // just create a hash of the AUTH JSON... 
        return Utils.createHash(this.m_auth_json);
    }
    
    // intialize a listener for the peer
    @Override
    public void initListener() {
        // XXX to do
        this.errorLogger().info("initListener(Google Cloud): not implemented");
        
        // register "onMessageReceive()" to handle and process requests from Google Cloud
    }

    // stop the listener for a peer
    @Override
    public void stopListener() {
        // XXX to do
        this.errorLogger().info("stopListener(Google Cloud): not implemented");
        
        // stop the Google Cloud listener...
    }
    
    // GenericSender Implementation: send a message
    @Override
    public void sendMessage(String to, String message) {
        // send a message over Google Cloud...
        this.errorLogger().info("sendMessage(Google Cloud): Sending Message to: " + to + " message: " + message);
        
        // send the message over Google Cloud
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
