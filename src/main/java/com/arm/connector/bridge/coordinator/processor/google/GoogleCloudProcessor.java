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
import com.arm.connector.bridge.coordinator.processors.interfaces.SubscriptionProcessor;
import com.google.api.services.pubsub.model.ReceivedMessage;
import com.google.api.services.pubsub.model.Topic;
import com.google.api.services.pubsub.model.Subscription;
import com.google.api.client.googleapis.auth.oauth2.GoogleCredential;
import com.google.api.client.googleapis.util.Utils;
import com.google.api.client.http.HttpRequestInitializer;
import com.google.api.services.pubsub.Pubsub;
import com.google.api.services.pubsub.PubsubScopes;
import com.google.api.services.pubsub.model.AcknowledgeRequest;
import com.google.api.services.pubsub.model.PublishRequest;
import com.google.api.services.pubsub.model.PubsubMessage;
import com.google.api.services.pubsub.model.PullRequest;
import com.google.api.services.pubsub.model.PullResponse;
import com.google.common.collect.ImmutableList;
import java.io.ByteArrayInputStream;
import java.util.HashMap;

/**
 * Google Cloud peer processor
 *
 * @author Doug Anson
 */
public class GoogleCloudProcessor extends PeerProcessor implements PeerInterface, GenericSender, SubscriptionProcessor {
    private GoogleCredential m_credential = null;
    private Pubsub m_pubsub = null;
    private String m_app_name = null;
    private String m_auth_json = null;
    private boolean m_logged_in = false;
    private String m_google_cloud_topic_delimiter = null;
    private String m_google_cloud_observe_notification_topic = null;
    private String m_google_cloud_coap_cmd_topic_get = null;
    private String m_google_cloud_coap_cmd_topic_put = null;
    private String m_google_cloud_coap_cmd_topic_post = null;
    private String m_google_cloud_coap_cmd_topic_delete = null;
    
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
        
        // we need extended subscription handling
        this.addSubscriptionHandler(this);
        
        // initialize the topic root
        this.initTopicRoot("google_cloud_topic_root");
        
        // auto-subscribe behavior
        this.initAutoSubscribe("google_cloud_obs_auto_subscribe");
        
        // Google Cloud peer PeerProcessor Announce
        this.errorLogger().info("Google Cloud Processor ENABLED.");
        
        // Google Cloud has odd topics... that are not hierarchy-oriented... so we have delimit them... 
        this.m_google_cloud_topic_delimiter = this.orchestrator().preferences().valueOf("google_cloud_topic_delimiter",this.m_suffix);
        
        // Observation notification topic
        this.m_google_cloud_observe_notification_topic = this.orchestrator().preferences().valueOf("google_cloud_observe_notification_topic", this.m_suffix);

        // if unified format enabled, observation == notify
        if (this.unifiedFormatEnabled()) {
            this.m_google_cloud_observe_notification_topic = this.m_google_cloud_observe_notification_topic.replace("observation",this.m_observation_key);
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
        this.m_logged_in = this.googleCloudLogin(this.m_app_name, this.m_auth_json);
    }
    
    // Create the authentication hash
    @Override
    public String createAuthenticationHash() {
        // just create a hash of the AUTH JSON... 
        return com.arm.connector.bridge.core.Utils.createHash(this.m_auth_json);
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
    
    // create the topic from the values
    // FORMAT: __TOPIC_ROOT__/__COMMAND_TYPE__/__DEVICE_TYPE__/__EPNAME__/__URI__
    // URI has a leading slash already...
    private String createBaseTopic(String root,String cmd,String ep,String ept,String uri) {
        String base = root + "/" + cmd + "/" + ept + "/" + ep + uri;
        return base;
    }
    
    // additional subscription handling 
    @Override
    public void subscribe(String domain, String ep, String ept, String path) {
        // Topic created
        String topic = this.createBaseTopic(this.getTopicRoot(),this.m_observation_key,ep,ept,path);
        this.googleCloudCreateTopic(topic);
        
        // Subscription created
        String subscription = this.createBaseTopic(this.getTopicRoot(),this.m_observation_key,ep,ept,path);
        this.googleCloudCreateSubscription(topic,subscription);
    }

    // additional unsubscribe handling
    @Override
    public void unsubscribe(String domain, String ep, String ept, String path) {
        // Topic removed
        String topic = this.createBaseTopic(this.getTopicRoot(),this.m_observation_key,ep,ept,path);
        this.googleCloudRemoveTopic(topic);
        
        // Subscription removed
        String subscription = this.createBaseTopic(this.getTopicRoot(),this.m_observation_key,ep,ept,path);
        this.googleCloudRemoveSubscription(subscription);
    }
    
    // GenericSender Implementation: send a message
    @Override
    public void sendMessage(String topic, String message) {
        if (this.m_pubsub != null) {
            try {
                // ensure we have proper delimiting
                String goo_topic = this.convertTopicStructure(topic);
                
                // send the message over Google Cloud
                PubsubMessage psm = new PubsubMessage();
                psm.encodeData(message.getBytes("UTF-8"));

                PublishRequest publishRequest = new PublishRequest();
                publishRequest.setMessages(ImmutableList.of(psm));

                // send the message
                this.errorLogger().info("sendMessage(Google Cloud): Sending Message to: " + goo_topic + " message: " + message);
                this.m_pubsub.projects().topics().publish(goo_topic, publishRequest).execute();
            }
            catch (Exception ex) {
                // unable to send message... exception raised
                this.errorLogger().warning("sendMessage(Google Cloud): Unable to send message: " + ex.getMessage(),ex);
            }
        }
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
            edited_auth_json = com.arm.connector.bridge.core.Utils.my_replace(auth_json,(char)160,' ');
            
            // DEBUG
            //this.errorLogger().info("googleCloudLogin():AUTH:" + edited_auth_json);
            
            // Build service account credential.
            this.m_credential = GoogleCredential.fromStream(new ByteArrayInputStream(edited_auth_json.getBytes()));
            
            // add scopes
            if (this.m_credential.createScopedRequired()) {
                this.m_credential = this.m_credential.createScoped(PubsubScopes.all());
            }
            
            // Please use custom HttpRequestInitializer for automatic
            // retry upon failures.
            HttpRequestInitializer initializer = new RetryHttpInitializerWrapper(this.m_credential);
            this.m_pubsub = new Pubsub.Builder(Utils.getDefaultTransport(),Utils.getDefaultJsonFactory(), initializer)
                             .setApplicationName(this.m_app_name)
                             .build();

            // success!
            success = true;
            
            // DEBUG
            this.errorLogger().info("googleCloudLogin(): LOGIN SUCCESS.");
        }
        catch (Exception ex) {
            // caught exception during login
            this.errorLogger().critical("googleCloudLogin(): Unable to log into Google Cloud: " + ex.getMessage(), ex);
            success = false;
        }
        
        // return our status
        return success;
    }
    
    // Remove a Subscription
    private void googleCloudRemoveSubscription(String subscription) {
        if (this.m_pubsub != null) {
            try {
                // Create the google-compatiable subscription
                String goo_subscription = this.convertSubscriptionStructure(subscription);
                
                // remove the subscription
                this.errorLogger().info("googleCloudRemoveSubscription: removing subscription: " + goo_subscription + "...");
                this.m_pubsub.projects().subscriptions().delete(goo_subscription).execute();
            }
            catch (Exception ex) {
                this.errorLogger().warning("googleCloudRemoveSubscription: exception during subscription removal: " + ex.getMessage(),ex);
            }
        }
    }
    
    // Remote a Topic
    private void googleCloudRemoveTopic(String topic) {
        if (this.m_pubsub != null) {
            try {
                // Create the google-compatiable topic
                String goo_topic = this.convertTopicStructure(topic);
                
                // remove the topic
                this.errorLogger().info("googleCloudRemoveTopic: removing topic: " + goo_topic + "...");
                this.m_pubsub.projects().topics().delete(goo_topic).execute();
            }
            catch (Exception ex) {
                this.errorLogger().warning("googleCloudRemoveTopic: exception during topic removal: " + ex.getMessage(),ex);
            }
        }
    }
    
    // Create the GoogleCloud topics
    private Topic googleCloudCreateTopic(String topic) {
        if (this.m_pubsub != null) {
            try {
                // Create the google-compatiable topic
                String goo_topic = this.convertTopicStructure(topic);
                
                // Create the Topic
                this.googleCloudRemoveTopic(goo_topic);
                this.errorLogger().info("googleCloudCreateTopic: Creating Main Topic: " + goo_topic);
                return this.m_pubsub.projects().topics().create(goo_topic,new Topic()).execute();
            }
            catch (Exception ex) {
                // no pubsub instance
                this.errorLogger().warning("googleCloudCreateTopic: exception in topic creation: " + ex.getMessage(),ex);
            }
        }
        else {
            // no pubsub instance
            this.errorLogger().warning("googleCloudCreateTopic: no pubsub instance... unable to create topic");
        }
        return null;
    }
    
    // Create the GoogleCloud subscription
    private Subscription googleCloudCreateSubscription(String topic,String subscription) {
        if (this.m_pubsub != null) {
            try {
                // Create the google-compatiable topic
                String goo_topic = this.convertTopicStructure(topic);
                
                // Create the google-compatiable subscription
                String goo_subscription = this.convertSubscriptionStructure(subscription);
                
                // Create the Observation Subscription
                this.errorLogger().info("googleCloudCreateSubscription: Creating Subscription: " + goo_subscription);
                Subscription s = new Subscription().setTopic(goo_topic);
                this.googleCloudRemoveSubscription(goo_subscription);
                return this.m_pubsub.projects().subscriptions().create(goo_subscription,s).execute();
            }
            catch (Exception ex) {
                // no pubsub instance
                this.errorLogger().warning("googleCloudCreateSubscription: exception in subscription creation: " + ex.getMessage(),ex);
            }
        }
        else {
            // no pubsub instance
            this.errorLogger().warning("googleCloudCreateSubscription: no pubsub instance... unable to create subscription");
        }
        return null;
    }
    
    // convert the format "a/b/c" to "a.b.c" since Google PubSub Cloud Topics/Subscriptions can have "/" in them... 
    private String convertStructure(String data,String type) {
        return "projects/" + this.m_app_name + "/" + type + "/" + com.arm.connector.bridge.core.Utils.my_replace(data,'/',this.m_google_cloud_topic_delimiter.charAt(0));
    }
    
    // convert the format "a/b/c" to "a.b.c" since Google PubSub Cloud Topics can have "/" in them... 
    private String convertTopicStructure(String topic) {
        return this.convertStructure(topic,"topics");
    }
    
    // convert the format "a/b/c" to "a.b.c" since Google PubSub Cloud Subscriptions can have "/" in them...
    private String convertSubscriptionStructure(String topic) {
        return this.convertStructure(topic,"subscriptions");
    }
}
