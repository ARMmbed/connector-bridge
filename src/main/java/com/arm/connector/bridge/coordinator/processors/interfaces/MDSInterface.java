/**
 * @file    MDSInterface.java
 * @brief mDS Peer Interface for the connector bridge
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
package com.arm.connector.bridge.coordinator.processors.interfaces;

import java.util.Map;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

/**
 * This interface defines the exposed methods of the mDS processor that can be used by a given peer processor.
 *
 * @author Doug Anson
 */
public interface MDSInterface {

    // process notifications
    public void processMDSMessage(HttpServletRequest request, HttpServletResponse response);

    // process de-registrations
    public void processDeregistrations(String[] deregistrations);

    // process resource subscription request
    public String subscribeToEndpointResource(String uri, Map options, Boolean init_webhook);

    public String subscribeToEndpointResource(String ep_name, String uri, Boolean init_webhook);

    // process resource un-subscribe request
    public String unsubscribeFromEndpointResource(String uri, Map options);

    // perform device discovery
    public String performDeviceDiscovery(Map options);

    // perform device resource discovery
    public String performDeviceResourceDiscovery(String uri);

    // perform CoAP operations on endpoint resources
    public String processEndpointResourceOperation(String verb, String uri, Map options);

    public String processEndpointResourceOperation(String verb, String ep_name, String uri);

    public String processEndpointResourceOperation(String verb, String ep_name, String uri, String value, String options);

    // Webhook management
    public void setNotificationCallbackURL();

    public void resetNotificationCallbackURL();

    // Device Metadata extraction
    public void pullDeviceMetadata(Map endpoint, AsyncResponseProcessor processor);

    // Begin mDS/mDS webhook and subscription validation polling...
    public void beginValidationPolling();

    // create the mDS/mDC URI for subscriptions
    public String createSubscriptionURI(String ep_name, String resource_uri);
}
