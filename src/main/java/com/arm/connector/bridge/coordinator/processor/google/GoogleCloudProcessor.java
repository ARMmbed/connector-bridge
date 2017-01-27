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
import com.google.api.services.pubsub.model.Topic;
import com.google.api.services.pubsub.model.Subscription;
import com.google.api.client.googleapis.auth.oauth2.GoogleCredential;
import com.google.api.client.googleapis.util.Utils;
import com.google.api.client.http.HttpRequestInitializer;
import com.google.api.services.pubsub.Pubsub;
import com.google.api.services.pubsub.PubsubScopes;
import com.google.api.services.pubsub.model.PublishRequest;
import com.google.api.services.pubsub.model.PubsubMessage;
import com.google.common.collect.ImmutableList;
import java.io.ByteArrayInputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

/**
 * Google Cloud PubSub peer processor
 *
 * @author Doug Anson
 */
public class GoogleCloudProcessor extends PeerProcessor implements PeerInterface, GenericSender, SubscriptionProcessor, GoogleCloudReceiveThread.ReceiveListener {
    private GoogleCredential m_credential = null;
    private Pubsub m_pubsub = null;
    private String m_app_name = null;
    private String m_auth_json = null;
    private boolean m_logged_in = false;
    private String m_google_cloud_topic_slash_delimiter = null;
    private String m_google_cloud_topic_segment_delimiter = null;
    private int m_sleep_time = 0;
    private int m_max_messages = 0;
    private ArrayList<HashMap<String,Object>> m_receivers = null;

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
        
        // Google Cloud peer PeerProcessor Announce
        this.errorLogger().info("Google Cloud Processor ENABLED.");
        
        // get the parameters for the receiver thread
        this.m_sleep_time = this.orchestrator().preferences().intValueOf("google_cloud_sleep_time",this.m_suffix);
        this.m_max_messages = this.orchestrator().preferences().intValueOf("google_cloud_max_messages",this.m_suffix);
        
        // we need extended subscription handling
        this.addSubscriptionHandler(this);
        
        // initialize the receivers list
        this.m_receivers = new ArrayList<>();
        
        // initialize the topic root
        this.initTopicRoot("google_cloud_topic_root");
        
        // auto-subscribe behavior
        this.initAutoSubscribe("google_cloud_obs_auto_subscribe");     
        
        // Google Cloud has odd topics... that are not hierarchy-oriented... so we have delimit them... 
        this.m_google_cloud_topic_slash_delimiter = this.orchestrator().preferences().valueOf("google_cloud_topic_slash_delimiter",this.m_suffix);
        this.m_google_cloud_topic_segment_delimiter = this.orchestrator().preferences().valueOf("google_cloud_topic_segment_delimiter",this.m_suffix);
        
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
    
    // message receiver
    @Override
    public void onMessageReceive(String topic, String message) {
        // DEBUG
        this.errorLogger().info("onMessageReceive: topic: " + topic + " message: " + message);
        
        // call the superclass to process it
        super.onMessageReceive(topic, message);
    }
    
    //
    // create the topic/subscription from the values (connector-bridge format)
    // FORMAT: <TOPIC_ROOT>/<COMMAND_TYPE>/<ENDPOINT_TYPE>/<ENDPOINT_NAME><URI>
    // URI has a leading slash already...
    // COMNAND_TYPE is either "notify" ("observations" for legacy) or "request/endpoints"
    //
    private String createBaseTopicAndSubscriptionStructure(String root,String cmd,String ep,String ept,String uri) {
        String base = root + "/" + cmd + "/" + ept + "/" +ep + uri;
        return base;
    }
    
    // pull the ith substring from the topic if it exists 
    private String getTopicSubstring(String topic,int index) {
        if (topic != null) {
            String list[] = topic.split(this.m_google_cloud_topic_segment_delimiter);
            
            // validate
            if (list != null && list.length > index) {
                return list[index];
            }
        }
        return null;
    }
    
    // get the observability of a given resource
    @Override
    protected boolean isObservableResource(Map resource) {
        // we have to make (fake) everything as observable so that we can setup the appropriate subscriptions
        return true;
    }
    
    // get specific description
    private int getSubscription(String subscription) {
        int index = -1;
        
        if (subscription != null && subscription.length() > 0) {
            for(int i=0;i<this.m_receivers.size() && index < 0;++i) {
                HashMap<String,Object> entry = this.m_receivers.get(i);
                String t_subscription = (String)entry.get("subscription");
                if (t_subscription.equalsIgnoreCase(subscription) == true) {
                    index = i;
                }
            }
        }
        return index;
    }
    
    // subscription in list?
    private boolean subscribed(String subscription) {
        return (this.getSubscription(subscription) >= 0);
    }
    
    // add subscription
    private void addSubscription(String subscription) {
        if (this.subscribed(subscription) == false) {
            HashMap<String,Object> entry = new HashMap<>();
            entry.put("subscription", subscription);
            GoogleCloudReceiveThread receiver = new GoogleCloudReceiveThread(this,this.m_pubsub,this.m_sleep_time,this.m_max_messages,this.connectorSubscriptionToGoogleSubscription(subscription));
            receiver.start_listening();
            entry.put("receiver",receiver);
            this.m_receivers.add(entry);
        }
    }
    
    // remove subscription
    public void removeSubscription(String subscription) {
        int index = this.getSubscription(subscription);
        if (index >= 0) {
            HashMap<String,Object> entry = this.m_receivers.get(index);
            GoogleCloudReceiveThread receiver = (GoogleCloudReceiveThread)entry.get("receiver");
            receiver.stop_listening();
            this.m_receivers.remove(entry);
        }
    }
    
    // get the endpoint name from the topic (request topic sent) 
    // format: <google preamble>/mbed%request%<endpoint_type>%<endpoint name>%<URI> POSITION SENSITIVE
    @Override
    public String getEndpointNameFromTopic(String topic) {
        return this.getTopicElement(topic,3);
    }
    
    // get the endpoint type from the topic (request topic sent) 
    // format: <google preamble>/mbed%request%<endpoint_type>%<endpoint name>%<URI> POSITION SENSITIVE
    @Override
    public String getEndpointTypeFromTopic(String topic) {
        return this.getTopicElement(topic,2);
    }

    // get the resource URI from the topic (request topic sent) 
    // format: <google preamble>/mbed%request%<endpoint_type>%<endpoint name>%<URI> POSITION SENSITIVE
    @Override
    public String getResourceURIFromTopic(String topic) {
        return this.getTopicElement(topic,4).replace(this.m_google_cloud_topic_slash_delimiter,"/");
    }
    
    // subscribe (cmd)
    private void subscribe(String domain, String ep, String ept, String path, String cmd) {
        this.subscribe(domain,ep,ept,path,cmd,true);
    }
    
    // subscribe (cmd), optional listener
    private void subscribe(String domain, String ep, String ept, String path, String cmd,boolean enable_listener) {
        // Topic created
        String topic = this.createBaseTopicAndSubscriptionStructure(this.getTopicRoot(),cmd,ep,ept,path);
        this.googleCloudCreateTopic(topic);
        
        // Subscription created
        String subscription = this.createBaseTopicAndSubscriptionStructure(this.getTopicRoot(),cmd,ep,ept,path);
        this.googleCloudCreateSubscription(topic,subscription);
        
        // add a listener (if requested)
        if (enable_listener == true) {
            this.addSubscription(subscription);
        }
    }
    
    // unsubscribe (cmd)
    void unsubscribe(String domain, String ep, String ept, String path, String cmd) {
        // Subscription removed
        String subscription = this.createBaseTopicAndSubscriptionStructure(this.getTopicRoot(),cmd,ep,ept,path);
        this.googleCloudRemoveSubscription(subscription);
        this.removeSubscription(subscription);
        
        // Topic removed
        String topic = this.createBaseTopicAndSubscriptionStructure(this.getTopicRoot(),cmd,ep,ept,path);
        this.googleCloudRemoveTopic(topic);
    } 
    
    // create the "request" token 
    // format: <topic_root>/request/endpoints/<ep_type>/<endpoint name>/<URI> POSITION SENSITIVE
    private String createRequestToken() {
        return "request" + this.m_google_cloud_topic_segment_delimiter + "endpoints";
    }
    
    // additional subscription handling 
    // format: <topic_root>/request/endpoints/<ep_type>/<endpoint name>/<URI> POSITION SENSITIVE
    @Override
    public void subscribe(String domain, String ep, String ept, String path, boolean is_observable) {
        // subscribe to notifications (no listener)
        this.subscribe(domain, ep, ept, path, this.m_observation_key,false);
        
        // also subscribe to CoAP request for: get, put, post, delete processing (listen on these...)
        this.subscribe(domain, ep, ept, path, this.createRequestToken(),true);
        
        // also setup the CoAP command response topic (no listener)
        this.subscribe(domain, ep, ept, path, "cmd-response", false);
    }
    
    // unsubscribe handling
    // format: <topic_root>/request/endpoints/<ep_type>/<endpoint name>/<URI> POSITION SENSITIVE
    @Override
    public void unsubscribe(String domain, String ep, String ept, String path) {
        // unsubscribe to notifications
        this.unsubscribe(domain, ep, ept, path, this.m_observation_key);
        
        // also unsubscribe from CoAP request for get, put, post, delete processing
        this.unsubscribe(domain, ep, ept, path, this.createRequestToken());
        
        // also remove the CoAP command response topic
        this.unsubscribe(domain, ep, ept, path, "cmd-response");
    }
    
    // GenericSender Implementation: send a message
    @Override
    public void sendMessage(String topic, String message) {
        if (this.m_pubsub != null && topic != null) {
            if (topic.contains("new_registration") == true) {
                // ignore new_registration requests... not used in Google Cloud
                this.errorLogger().info("sendMessage(GoogleCloud): ignoring new_registration message type (not used...OK)");
            }
            else {
                try {
                    // ensure we have proper delimiting
                    String goo_topic = this.connectorTopicToGoogleTopic(topic);

                    // DEBUG
                    //this.errorLogger().info("sendMessage(GoogleCloud): orig topic: " + topic + " converted: " + goo_topic);

                    // send the message over Google Cloud
                    PubsubMessage psm = new PubsubMessage();
                    psm.encodeData(message.getBytes("UTF-8"));

                    PublishRequest publishRequest = new PublishRequest();
                    publishRequest.setMessages(ImmutableList.of(psm));

                    // send the message
                    this.errorLogger().info("sendMessage(Google Cloud): Sending message to: " + goo_topic + " message: " + message);
                    this.m_pubsub.projects().topics().publish(goo_topic, publishRequest).execute();
                }
                catch (Exception ex) {
                    // unable to send message... exception raised
                    this.errorLogger().warning("sendMessage(Google Cloud): Unable to send message: " + ex.getMessage(),ex);
                }
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
            edited_auth_json = com.arm.connector.bridge.core.Utils.replaceAllCharOccurances(auth_json,(char)160,' ');
            
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
                String goo_subscription = this.connectorSubscriptionToGoogleSubscription(subscription);
                
                // remove the subscription
                this.errorLogger().info("googleCloudRemoveSubscription: removing subscription: " + goo_subscription + "...");
                this.m_pubsub.projects().subscriptions().delete(goo_subscription).execute();
            }
            catch (Exception ex) {
                // DEBUG
                // this.errorLogger().info("googleCloudRemoveSubscription: exception during subscription removal: " + ex.getMessage());
            }
        }
    }
    
    // Remote a Topic
    private void googleCloudRemoveTopic(String topic) {
        if (this.m_pubsub != null) {
            try {
                // Create the google-compatiable topic
                String goo_topic = this.connectorTopicToGoogleTopic(topic);
                
                // remove the topic
                this.errorLogger().info("googleCloudRemoveTopic: removing topic: " + goo_topic + "...");
                this.m_pubsub.projects().topics().delete(goo_topic).execute();
            }
            catch (Exception ex) {
                // DEBUG
                //this.errorLogger().info("googleCloudRemoveTopic: exception during topic removal: " + ex.getMessage());
            }
        }
    }
    
    // Create the GoogleCloud topics
    private Topic googleCloudCreateTopic(String topic) {
        if (this.m_pubsub != null) {
            try {
                // XXX remove any old topic
                //this.googleCloudRemoveTopic(topic);
                
                // Create the google-compatiable topic
                String goo_topic = this.connectorTopicToGoogleTopic(topic);
                
                // Create the Topic
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
                // XXX remove any old subscription
                //this.googleCloudRemoveSubscription(subscription);
                
                // Create the google-compatiable topic
                String goo_topic = this.connectorTopicToGoogleTopic(topic);
                
                // Create the google-compatiable subscription
                String goo_subscription = this.connectorSubscriptionToGoogleSubscription(subscription);
                
                // Create the Observation Subscription
                this.errorLogger().info("googleCloudCreateSubscription: Creating Subscription: " + goo_subscription);
                Subscription s = new Subscription().setTopic(goo_topic);
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
    
    // create preamble
    private String createPreamble(String type) {
        return "projects/" + this.m_app_name + "/" + type + "/";
    }

    // Connector-bridge topic format conversion TO Google Cloud PubSub format
    private String convertToPubSubFormat(String data,String type,int num_occurances) {
        // create the preamble
        String preamble = this.createPreamble(type);
        
        // prevent having the base entered twice... 
        if (data != null && data.contains(preamble) == true) {
            // DEBUG
            this.errorLogger().info("convertToPubSubFormat: Already converted to Google: " + data);
            
            // already formatted... just return
            return data;
        }

        // convert the segment delimiter in the original format
        String t1 = com.arm.connector.bridge.core.Utils.replaceCharOccurances(data, '/', this.m_google_cloud_topic_segment_delimiter.charAt(0),num_occurances);

        // now replace the URI delimiter and prefix with the preamble
        String result = preamble + t1.replace('/', this.m_google_cloud_topic_slash_delimiter.charAt(0));
        
        // DEBUG
        this.errorLogger().info("convertToPubSubFormat: Connector: " + data + " Google: " + result);
        
        // return the result
        return result;
    }

    // Connector-bridge topic format conversion FROM Google Cloud PubSub format
    private String convertToConnectorFormat(String data,String type) {
        // create the preamble
        String preamble = this.createPreamble(type);
   
        // prevent having the base entered twice... 
        if (data != null && data.contains(preamble) == false) {
            // DEBUG
            this.errorLogger().info("convertToConnectorFormat: Already converted to Connector: " + data);
        
            // already formatted... just return
            return data;
        }

        // remove the preamble
        String t1 = data.replace(preamble, "");
        
        // replace double segment and slash with just a segment... (URI boundary)
        String uri_boundary = this.m_google_cloud_topic_segment_delimiter + this.m_google_cloud_topic_slash_delimiter;
        String t2 = t1.replace(uri_boundary,this.m_google_cloud_topic_segment_delimiter);

        // replace the segment delimiter with '/'
        String t3 = com.arm.connector.bridge.core.Utils.replaceAllCharOccurances(t2,this.m_google_cloud_topic_segment_delimiter.charAt(0),'/');

        // replace the URI delimiter
        String result = t3.replace(this.m_google_cloud_topic_slash_delimiter.charAt(0), '/');
        
        // DEBUG
        this.errorLogger().info("convertToConnectorFormat: Google: " + data + " Connector: " + result);
        
        // return the result
        return result;
    }
    
    // convert the format "a/b/c" to "a.b.c" since Google PubSub Cloud Subscriptions can have "/" in them...
    private String connectorSubscriptionToGoogleSubscription(String subscription) {
        // DEBUG
        this.errorLogger().info("connectorSubscriptionToGoogleSubscription: input subscription: " + subscription);
        
        // convert and return
        return this.convertToPubSubFormat(subscription,"subscriptions",3);
    }

    // convert a message-born notification topic to Google-format
    // format:  mbed/notify/<endpoint_type>/<endpoint_name><uri>
    private String connectorTopicToGoogleTopic(String topic) {
        // DEBUG
        this.errorLogger().info("connectorTopicToGoogleTopic: input topic: " + topic);
        
        // convert and return
        return this.convertToPubSubFormat(topic,"topics",3);
    }

    // convert a subscription in google format to a topic in connector-bridge format...
    @Override
    public String connectorTopicFromGoogleSubscription(String goo_subscription) {
        // DEBUG
        this.errorLogger().info("connectorTopicFromGoogleSubscription: input subscription: " + goo_subscription);
        
        // convert and return
        return this.convertToConnectorFormat(goo_subscription,"subscriptions");
    }
}
