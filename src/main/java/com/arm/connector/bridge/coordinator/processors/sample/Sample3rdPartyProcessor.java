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
import com.arm.connector.bridge.coordinator.processors.interfaces.AsyncResponseProcessor;
import com.arm.connector.bridge.coordinator.processors.interfaces.PeerInterface;
import com.arm.connector.bridge.transport.HttpTransport;
import java.util.Map;

/**
 * Sample 3rd Party peer processor
 *
 * @author Doug Anson
 */
public class Sample3rdPartyProcessor extends Processor implements PeerInterface {

    private HttpTransport m_http = null;
    private String m_suffix = null;

    // Factory method for initializing the Sample 3rd Party peer
    public static Sample3rdPartyProcessor createPeerProcessor(Orchestrator manager, HttpTransport http) {
        // create me
        Sample3rdPartyProcessor me = new Sample3rdPartyProcessor(manager, http);

        // initialize me
        // return me
        return me;
    }

    // constructor
    public Sample3rdPartyProcessor(Orchestrator orchestrator, HttpTransport http) {
        this(orchestrator, http, null);
    }

    // constructor
    public Sample3rdPartyProcessor(Orchestrator orchestrator, HttpTransport http, String suffix) {
        super(orchestrator, null);
        this.m_http = http;
        this.m_mds_domain = orchestrator.getDomain();
        this.m_suffix = suffix;

        // Sample 3rd Party peer Processor Announce
        this.errorLogger().info("Sample 3rd Party peer Processor ENABLED.");
    }

    // process a received new registration
    @Override
    public void processNewRegistration(Map message) {
        // XXX to do
        this.errorLogger().info("processNewRegistration(Sample): message: " + message);
    }

    // process a received new registration
    @Override
    public void processReRegistration(Map message) {
        // XXX to do
        this.errorLogger().info("processReRegistration(Sample): message: " + message);
    }

    // process a received new registration
    @Override
    public String[] processDeregistrations(Map message) {
        // XXX to do
        this.errorLogger().info("processDeregistrations(Sample): message: " + message);
        return null;
    }

    // process mds registrations-expired messages 
    @Override
    public void processRegistrationsExpired(Map message) {
        this.errorLogger().info("processRegistrationsExpired(Sample): message: " + message);
        this.processDeregistrations(message);
    }

    // process a received new registration
    @Override
    public void processAsyncResponses(Map data) {
        // XXX to do
        this.errorLogger().info("processAsyncResponses(Sample): data: " + data);
    }

    // process a received new registration/registration update/deregistration, 
    protected void processRegistration(Map data, String key) {
        // XXX TO DO 
        this.errorLogger().info("processRegistration(Sample): key: " + key + " data: " + data);
    }

    // process an observation
    @Override
    public void processNotification(Map data) {
        // XXXX TO DO
        this.errorLogger().info("processNotification(Sample): data: " + data);
    }

    // Create the authentication hash
    @Override
    public String createAuthenticationHash() {
        // XXX TO DO
        this.errorLogger().info("createAuthenticationHash(Sample)");
        return "";
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

    @Override
    public void recordAsyncResponse(String response, String uri, Map ep, AsyncResponseProcessor processor) {
        // XXX TO DO
        this.errorLogger().info("recordAsyncResponse(Sample): response: " + response + " URI: " + uri + " EP: " + ep);
    }
}
