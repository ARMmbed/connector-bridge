/**
 * @file    Sample3rdPartyProcessor.java
 * @brief Stubbed out Sample 3rd Party Peer Processor
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
package com.arm.connector.bridge.coordinator.processors.sample;

import com.arm.connector.bridge.coordinator.Orchestrator;
import com.arm.connector.bridge.coordinator.processors.core.Processor;
import com.arm.connector.bridge.coordinator.processors.interfaces.PeerInterface;
import com.arm.connector.bridge.core.Utils;
import com.arm.connector.bridge.transport.HttpTransport;
import java.util.Map;

/**
 * Sample 3rd Party peer processor (derived from Processor.. may want to switch to GenericMQTTProcessor)
 *
 * @author Doug Anson
 */
public class Sample3rdPartyProcessor extends Processor implements PeerInterface {

    private HttpTransport m_http = null;

    // (OPTIONAL) Factory method for initializing the Sample 3rd Party peer
    public static Sample3rdPartyProcessor createPeerProcessor(Orchestrator manager, HttpTransport http) {
        // create me
        Sample3rdPartyProcessor me = new Sample3rdPartyProcessor(manager, http);

        // return me
        return me;
    }

    // constructor
    public Sample3rdPartyProcessor(Orchestrator orchestrator, HttpTransport http) {
        this(orchestrator, http, null);
    }

    // constructor
    public Sample3rdPartyProcessor(Orchestrator orchestrator, HttpTransport http, String suffix) {
        super(orchestrator,suffix);
        this.m_http = http;
        this.m_mds_domain = orchestrator.getDomain();

        // Sample 3rd Party peer Processor Announce
        this.errorLogger().info("Sample 3rd Party peer Processor ENABLED.");
    }
    
    //
    // OVERRIDES for Sample3rdPartyProcessor...
    //
    // These methods are stubbed out by default... but need to be implemented in derived classes.
    // They are the "responders" to mDS events for devices
    //
    
    // process a received new registration/registration update/deregistration, 
    @Override
    protected void processRegistration(Map data, String key) {
        // XXX TO DO 
        this.errorLogger().info("processRegistration(BASE): key: " + key + " data: " + data);
    }
    
    // process a reregistration
    @Override
    public void processReRegistration(Map message) {
        // XXX to do
        this.errorLogger().info("processReRegistration(BASE): message: " + message);
    }

    // process a deregistration
    @Override
    public String[] processDeregistrations(Map message) {
        // XXX to do
        this.errorLogger().info("processDeregistrations(BASE): message: " + message);
        return null;
    }
    
    // process an observation/notification
    @Override
    public void processNotification(Map data) {
        // XXXX TO DO
        this.errorLogger().info("processNotification(BASE): data: " + data);
    }

    // initialize any Sample 3rd Party peer listeners
    @Override
    public void initListener() {
        // XXX to do
        this.errorLogger().info("initListener(Sample)");
    }

    // stop our Sample 3rd Party peer listeners
    @Override
    public void stopListener() {
        // XXX to do
        this.errorLogger().info("stopListener(Sample)");
    }
    
    // Create the authentication hash
    @Override
    public String createAuthenticationHash() {
        // just create a hash of something unique to the peer side... 
        String peer_secret = "";
        return Utils.createHash(peer_secret);
    }
}
