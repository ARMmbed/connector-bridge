/**
 * @file    orchestrator.java
 * @brief   orchestrator for the connector bridge
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

package com.arm.connector.bridge.coordinator;

import com.arm.connector.bridge.console.ConsoleManager;
import com.arm.connector.bridge.coordinator.processors.arm.GenericMQTTProcessor;
import com.arm.connector.bridge.coordinator.processors.ibm.WatsonIoTPeerProcessorFactory;
import com.arm.connector.bridge.coordinator.processors.ms.MSIoTHubPeerProcessorFactory;
import com.arm.connector.bridge.coordinator.processors.interfaces.MDSInterface;
import com.arm.connector.bridge.coordinator.processors.arm.MDSProcessor;
import com.arm.connector.bridge.coordinator.processors.aws.AWSIoTPeerProcessorFactory;
import com.arm.connector.bridge.coordinator.processors.interfaces.AsyncResponseProcessor;
import com.arm.connector.bridge.coordinator.processors.interfaces.PeerInterface;
import com.arm.connector.bridge.coordinator.processors.sample.Sample3rdPartyProcessor;
import com.arm.connector.bridge.core.ErrorLogger;
import com.arm.connector.bridge.json.JSONGenerator;
import com.arm.connector.bridge.json.JSONParser;
import com.arm.connector.bridge.json.JSONGeneratorFactory;
import com.arm.connector.bridge.preferences.PreferenceManager;
import com.arm.connector.bridge.transport.HttpTransport;
import java.util.ArrayList;
import java.util.Map;

import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

/**
 * This the primary orchestrator for the connector bridge
 * @author Doug Anson
 */
public class Orchestrator implements MDSInterface, PeerInterface {
    private HttpServlet              m_servlet = null;
    
    private ErrorLogger              m_error_logger = null;
    private PreferenceManager        m_preference_manager = null;
    
    // mDS processor
    private MDSInterface             m_mds_rest_processor = null;
    
    // Peer processor list
    private ArrayList<PeerInterface> m_peer_processor_list = null;
    
    private ConsoleManager           m_console_manager = null;

    private HttpTransport            m_http = null;
    
    private JSONGeneratorFactory     m_json_factory = null;
    private JSONGenerator            m_json_generator = null;
    private JSONParser               m_json_parser = null;
    private boolean                  m_listeners_initialized = false;
    
    private final String             m_mds_non_domain = null;
    private String                   m_mds_domain = null;
    
    public Orchestrator(ErrorLogger error_logger,PreferenceManager preference_manager, String domain) {
        // save the error handler
        this.m_error_logger = error_logger;
        this.m_preference_manager = preference_manager;
                      
        // MDS domain is required 
        if (domain != null && domain.equalsIgnoreCase(this.preferences().valueOf("mds_def_domain")) == false) {
            this.m_mds_domain = domain;
        }
          
        // JSON Factory
        this.m_json_factory = JSONGeneratorFactory.getInstance();
        
        // create the JSON Generator
        this.m_json_generator = this.m_json_factory.newJsonGenerator();

        // create the JSON Parser
        this.m_json_parser = this.m_json_factory.newJsonParser();
        
        // build out the HTTP transport
        this.m_http = new HttpTransport(this.m_error_logger,this.m_preference_manager);
                        
        // REQUIRED: We always create the mDS REST processor
        this.m_mds_rest_processor = new MDSProcessor(this,this.m_http);
        
        // initialize our peer processor list
        this.initPeerProcessorList();
                
        // create the console manager
        this.m_console_manager = new ConsoleManager(this);
    }
    
    // initialize our peer processor
    private void initPeerProcessorList() {
        // initialize the list
        this.m_peer_processor_list = new ArrayList<>();
        
        // add peer processors
        if (this.ibmPeerEnabled()) {
            // IBM/MQTT: create the MQTT processor manager
            this.errorLogger().info("Orchestrator: adding IBM Watson IoT MQTT Processor");
            this.m_peer_processor_list.add(WatsonIoTPeerProcessorFactory.createPeerProcessor(this,this.m_http));
        }
        if (this.msPeerEnabled()) {
            // MS IoTHub/MQTT: create the MQTT processor manager
            this.errorLogger().info("Orchestrator: adding MS IoTHub MQTT Processor");
            this.m_peer_processor_list.add(MSIoTHubPeerProcessorFactory.createPeerProcessor(this,this.m_http));
        }
        if (this.awsPeerEnabled()) {
            // AWS IoT/MQTT: create the MQTT processor manager
            this.errorLogger().info("Orchestrator: adding AWS IoT MQTT Processor");
            this.m_peer_processor_list.add(AWSIoTPeerProcessorFactory.createPeerProcessor(this,this.m_http));
        }
        if (this.genericMQTTPeerEnabled()) {
            // Create the sample peer processor...
            this.errorLogger().info("Orchestrator: adding Generic MQTT Processor");
            this.m_peer_processor_list.add(GenericMQTTProcessor.createPeerProcessor(this,this.m_http));
        }
        if (this.samplePeerEnabled()) {
            // Create the sample peer processor...
            this.errorLogger().info("Orchestrator: adding 3rd Party Sample REST Processor");
            this.m_peer_processor_list.add(Sample3rdPartyProcessor.createPeerProcessor(this,this.m_http));
        }
    }
    
    // use IBM peer processor?
    private Boolean ibmPeerEnabled() {
        return (this.preferences().booleanValueOf("enable_iotf_addon") || this.preferences().booleanValueOf("enable_starterkit_addon"));
    }
    
    // use MS peer processor?
    private Boolean msPeerEnabled() {
        return (this.preferences().booleanValueOf("enable_iot_event_hub_addon"));
    }
    
    // use AWS peer processor?
    private Boolean awsPeerEnabled() {
        return (this.preferences().booleanValueOf("enable_aws_iot_gw_addon"));
    }
    
    // use sample 3rd Party peer processor?
    private Boolean samplePeerEnabled() {
        return this.preferences().booleanValueOf("enable_3rd_party_rest_processor");
    }
    
    // use generic MQTT peer processor?
    private Boolean genericMQTTPeerEnabled() {
        return this.preferences().booleanValueOf("enable_generic_mqtt_processor");
    }
    
    // are the listeners active?
    public boolean peerListenerActive() { 
        return this.m_listeners_initialized; 
    }
    
    // initialize peer listener
    public void initPeerListener() {
        if (!this.m_listeners_initialized) {
            // MQTT Listener
            for(int i=0;i<this.m_peer_processor_list.size();++i) {
                this.m_peer_processor_list.get(i).initListener();
            }
            this.m_listeners_initialized = true;
        }
    }
    
    // stop the peer listener
    public void stopPeerListener() {
        if (this.m_listeners_initialized) {
            // MQTT Listener
            for(int i=0;i<this.m_peer_processor_list.size();++i) {
                this.m_peer_processor_list.get(i).stopListener();
            }
            this.m_listeners_initialized = false;
        }
    }
    
    // initialize the mDS webhook
    public void initMDSWebhook() {
        if (this.m_mds_rest_processor != null) {
            // set the webhook
            this.m_mds_rest_processor.setNotificationCallbackURL();
            
            // begin validation polling
            this.beginValidationPolling();
        }
    }
    
    // reset mDS webhook
    public void resetMDSWebhook() {
        // REST (mDS)
        if (this.m_mds_rest_processor != null) {
            this.m_mds_rest_processor.resetNotificationCallbackURL();
        }
    }
    
    // process the mDS notification
    public void processNotification(HttpServletRequest request, HttpServletResponse response) {
        // process the received REST message
        //this.errorLogger().info("events (REST-" + request.getMethod() + "): " + request.getRequestURI());
        this.mds_rest_processor().processMDSMessage(request, response);
    }
    
    // process the Console request
    public void processConsole(HttpServletRequest request, HttpServletResponse response) {
        // process the received REST message
        //this.errorLogger().info("console (REST-" + request.getMethod() + "): " + request.getServletPath());
        this.console_manager().processConsole(request, response);
    }
    
    // set the HttpServlet
    private void setServlet(HttpServlet servlet) {
        this.m_servlet = servlet;
    }

    // get the HttpServlet
    public HttpServlet getServlet() {
        return this.m_servlet;
    }
    
    // get the ErrorLogger
    public ErrorLogger errorLogger() {
        return this.m_error_logger;
    }
    
    // get he Preferences manager
    public final PreferenceManager preferences() { 
        return this.m_preference_manager; 
    }
    
    // get the Peer processor
    public ArrayList<PeerInterface> peer_processor_list() {
        return this.m_peer_processor_list;
    }
    
    // get the mDS processor
    public MDSInterface mds_rest_processor() {
        return this.m_mds_rest_processor;
    }
    
    // get the console manager
    public ConsoleManager console_manager() {
        return this.m_console_manager;
    }
    
    // get our mDS domain
    public String getDomain() {
        return this.m_mds_domain;
    }
    
    // get the JSON parser instance
    public JSONParser getJSONParser() {
        return this.m_json_parser;
    }
    
    // get the JSON generation instance
    public JSONGenerator getJSONGenerator() {
        return this.m_json_generator;
    }
   
    // get our ith peer processor
    private PeerInterface peerProcessor(int index) {
        if (index >= 0 && this.m_peer_processor_list != null && index < this.m_peer_processor_list.size()) {
            return this.m_peer_processor_list.get(index); 
        }
        return null;
    }
    
    // MDSInterface Orchestration
    @Override
    public void processMDSMessage(HttpServletRequest request, HttpServletResponse response) {
        this.mds_rest_processor().processMDSMessage(request, response);
    }

    @Override
    public void processDeregistrations(String[] deregistrations) {
        this.mds_rest_processor().processDeregistrations(deregistrations);
    }

    @Override
    public String subscribeToEndpointResource(String uri, Map options, Boolean init_webhook) {
        return this.mds_rest_processor().subscribeToEndpointResource(uri, options, init_webhook);
    }

    @Override
    public String subscribeToEndpointResource(String ep_name, String uri, Boolean init_webhook) {
        return this.mds_rest_processor().subscribeToEndpointResource(ep_name, uri, init_webhook);
    }

    @Override
    public String unsubscribeFromEndpointResource(String uri, Map options) {
        return this.mds_rest_processor().unsubscribeFromEndpointResource(uri, options);
    }

    @Override
    public String performDeviceDiscovery(Map options) {
        return this.mds_rest_processor().performDeviceDiscovery(options);
    }

    @Override
    public String performDeviceResourceDiscovery(String uri) {
        return this.mds_rest_processor().performDeviceResourceDiscovery(uri);
    }

    @Override
    public String processEndpointResourceOperation(String verb, String uri, Map options) {
        return this.mds_rest_processor().processEndpointResourceOperation(verb, uri, options);
    }

    @Override
    public String processEndpointResourceOperation(String verb, String ep_name, String uri) {
        return this.mds_rest_processor().processEndpointResourceOperation(verb, ep_name, uri);
    }

    @Override
    public String processEndpointResourceOperation(String verb, String ep_name, String uri, String value, String options) {
        return this.mds_rest_processor().processEndpointResourceOperation(verb, ep_name, uri, value, options);
    }

    @Override
    public void setNotificationCallbackURL() {
        this.mds_rest_processor().setNotificationCallbackURL();
    }

    @Override
    public void resetNotificationCallbackURL() {
        this.mds_rest_processor().resetNotificationCallbackURL();
    }
    
    @Override
    public void pullDeviceMetadata(Map endpoint,AsyncResponseProcessor processor) {
        this.mds_rest_processor().pullDeviceMetadata(endpoint,processor);
    }

    // PeerInterface Orchestration
    @Override
    public String createAuthenticationHash() {
        String hash = "";
        for(int i=0;this.m_peer_processor_list != null && i<this.m_peer_processor_list.size();++i) {
            hash += this.peerProcessor(i).createAuthenticationHash();
        }
        return hash;
    }
    
    @Override
    public void recordAsyncResponse(String response,String uri,Map ep,AsyncResponseProcessor processor) {
        for(int i=0;this.m_peer_processor_list != null && i<this.m_peer_processor_list.size();++i) {
            this.peerProcessor(i).recordAsyncResponse(response, uri, ep, processor);
        }
    }

    @Override
    public void processNewRegistration(Map message) {
        for(int i=0;this.m_peer_processor_list != null && i<this.m_peer_processor_list.size();++i) {
            this.peerProcessor(i).processNewRegistration(message);
        }
    }

    @Override
    public void processReRegistration(Map message) {
        for(int i=0;this.m_peer_processor_list != null && i<this.m_peer_processor_list.size();++i) {
            this.peerProcessor(i).processReRegistration(message);
        }
    }

    @Override
    public String[] processDeregistrations(Map message) {
        ArrayList<String> deregistrations = new ArrayList<>();
        for(int i=0;this.m_peer_processor_list != null && i<this.m_peer_processor_list.size();++i) {
            String[] ith_deregistrations = this.peerProcessor(i).processDeregistrations(message);
            for(int j=0;ith_deregistrations != null && j<ith_deregistrations.length;++j) {
                boolean add = deregistrations.add(ith_deregistrations[j]);
            }
        }
        String[] dereg_str_array = new String[deregistrations.size()];
        return deregistrations.toArray(dereg_str_array); 
    }

    @Override
    public void processRegistrationsExpired(Map message) {
        for(int i=0;this.m_peer_processor_list != null && i<this.m_peer_processor_list.size();++i) {
            this.peerProcessor(i).processRegistrationsExpired(message);
        }
    }

    @Override
    public void processAsyncResponses(Map message) {
        for(int i=0;this.m_peer_processor_list != null && i<this.m_peer_processor_list.size();++i) {
            this.peerProcessor(i).processAsyncResponses(message);
        }
    }

    @Override
    public void processNotification(Map message) {
        for(int i=0;this.m_peer_processor_list != null && i<this.m_peer_processor_list.size();++i) {
            this.peerProcessor(i).processNotification(message);
        }
    }

    @Override
    public void initListener() {
        for(int i=0;this.m_peer_processor_list != null && i<this.m_peer_processor_list.size();++i) {
            this.peerProcessor(i).initListener();
        }
    }

    @Override
    public void stopListener() {
        for(int i=0;this.m_peer_processor_list != null && i<this.m_peer_processor_list.size();++i) {
            this.peerProcessor(i).stopListener();
        }
    }

    @Override
    public void beginValidationPolling() {
        this.mds_rest_processor().beginValidationPolling();
    }
}
