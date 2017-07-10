/**
 * @file    StarterKitMQTTProcessor.java
 * @brief IBM StarterKit MQTT Peer Processor
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
package com.arm.connector.bridge.coordinator.processors.ibm;

import com.arm.connector.bridge.coordinator.processors.arm.GenericMQTTProcessor;
import com.arm.connector.bridge.coordinator.Orchestrator;
import com.arm.connector.bridge.coordinator.processors.interfaces.PeerInterface;
import com.arm.connector.bridge.core.Utils;
import com.arm.connector.bridge.transport.HttpTransport;
import com.arm.connector.bridge.transport.MQTTTransport;
import java.util.List;
import java.util.Map;
import org.fusesource.mqtt.client.QoS;

/**
 * IBM StarterKit MQTT Peer Processor
 *
 * @author Doug Anson
 */
public class StarterKitMQTTProcessor extends GenericMQTTProcessor implements PeerInterface {
    private String m_mqtt_ip_address = null;
    private String m_starterkit_observe_notification_topic = null;
    private String m_starterkit_device_type = null;
    private String m_starterkit_device_data_key = null;
    private String m_auth_hash = null;

    // constructor (singleton)
    public StarterKitMQTTProcessor(Orchestrator manager, HttpTransport http) {
        this(manager, null, http);
    }

    // constructor (with suffix for preferences)
    public StarterKitMQTTProcessor(Orchestrator manager, String suffix, HttpTransport http) {
        super(manager, null, suffix, http);

        // StarterKit Announce
        this.errorLogger().info("IBM Watson IoT (StarterKit) Processor ENABLED.");

        // nullify the clientID - we handle this it differently in StarterKit...
        this.m_client_id = "error";

        // get our defaults
        this.m_starterkit_device_type = this.orchestrator().preferences().valueOf("starterkit_device_type", this.m_suffix);
        this.m_mqtt_port = this.orchestrator().preferences().intValueOf("starterkit_mqtt_port", this.m_suffix);

        // get our configured device data key 
        this.m_starterkit_device_data_key = "d";

        // our MQTT IP Address is also different - quickstart.messaging.internetofthings.ibmcloud.com - 184.172.124.189
        this.m_mqtt_ip_address = this.orchestrator().preferences().valueOf("starterkit_mqtt_ip_address", this.m_suffix);

        // we use a canned topic for notifications
        this.m_starterkit_observe_notification_topic = this.orchestrator().preferences().valueOf("starterkit_notification_topic", this.m_suffix);

        // add the transport
        this.initMQTTTransportList();

        // DEBUG
        //this.errorLogger().info("StarterKit Credentials: Username: " + this.m_mqtt.getUsername() + " PW: " + this.m_mqtt.getPassword());
    }
    
    // OVERRIDE: process a received new registration for StarterKit
    protected void processRegistration(Map data, String key, boolean new_starterkit_connection) {
        List endpoints = (List) data.get(key);
        for (int i = 0; endpoints != null && i < endpoints.size(); ++i) {
            Map endpoint = (Map) endpoints.get(i);
            List resources = (List) endpoint.get("resources");
            for (int j = 0; resources != null && j < resources.size(); ++j) {
                Map resource = (Map) resources.get(j);

                // re-subscribe
                if (this.subscriptionsManager().containsSubscription(this.m_mds_domain, (String) endpoint.get("ep"), (String) endpoint.get("ept"), (String) resource.get("path"))) {
                    // if we arrived here from a re-registration handler, we may need to re-generate the starterkit conneciton...its likely been deleted.
                    if (new_starterkit_connection == true) {
                        // we arrived from a re-registration and the starterkit connection has been deleted
                        if (this.addNewMQTTConnection((String) endpoint.get("ep")) == true) {
                            // we added a new starterkit connection... succeeded!  lets announce and re-subscribe to the rsource
                            this.errorLogger().info("processRegistration: re-establishing QuickStart connection... SUCCESS. Re-subscribing...");

                            // re-subscribe to this resource
                            this.orchestrator().subscribeToEndpointResource((String) endpoint.get("ep"), (String) resource.get("path"), false);

                            // SYNC: here we dont have to worry about Sync options - we simply dispatch the subscription to mDS and setup for it...
                            this.subscriptionsManager().removeSubscription(this.m_mds_domain, (String) endpoint.get("ep"), (String) endpoint.get("ept"), (String) resource.get("path"));
                            this.subscriptionsManager().addSubscription(this.m_mds_domain, (String) endpoint.get("ep"), (String) endpoint.get("ept"), (String) resource.get("path"), this.isObservableResource(resource));
                        }
                        else {
                            // we were not able to recreate the starterkit connection... so just bail... we'll try again... 
                            this.errorLogger().info("processRegistration: re-establishing QuickStart connection... failed!");
                            this.subscriptionsManager().removeSubscription(this.m_mds_domain, (String) endpoint.get("ep"), (String) endpoint.get("ept"), (String) resource.get("path"));
                        }
                    }
                    else {
                        // we do not need to re-create the starterkit connection... it already exists... so just handle the subscription check..
                        this.orchestrator().subscribeToEndpointResource((String) endpoint.get("ep"), (String) resource.get("path"), false);

                        // SYNC: here we dont have to worry about Sync options - we simply dispatch the subscription to mDS and setup for it... 
                        this.subscriptionsManager().removeSubscription(this.m_mds_domain, (String) endpoint.get("ep"), (String) endpoint.get("ept"), (String) resource.get("path"));
                        this.subscriptionsManager().addSubscription(this.m_mds_domain, (String) endpoint.get("ep"), (String) endpoint.get("ept"), (String) resource.get("path"), this.isObservableResource(resource));
                    }
                }

                // auto-subscribe
                else if (this.isObservableResource(resource) && this.m_auto_subscribe_to_obs_resources == true) {
                    // create a specific StarterKit MQTT connection with a clientID having the endpoint name as its device_id
                    if (this.addNewMQTTConnection((String) endpoint.get("ep")) == true) {
                        // auto-subscribe to observable resources... if enabled.
                        this.orchestrator().subscribeToEndpointResource((String) endpoint.get("ep"), (String) resource.get("path"), false);

                        // SYNC: here we dont have to worry about Sync options - we simply dispatch the subscription to mDS and setup for it...
                        this.subscriptionsManager().removeSubscription(this.m_mds_domain, (String) endpoint.get("ep"), (String) endpoint.get("ept"), (String) resource.get("path"));
                        this.subscriptionsManager().addSubscription(this.m_mds_domain, (String) endpoint.get("ep"), (String) endpoint.get("ept"), (String) resource.get("path"), this.isObservableResource(resource));
                    }
                }
            }
        }
    }

    // OVERRIDE: process a re-registration in StarterKit
    @Override
    public void processReRegistration(Map data) {
        List notifications = (List) data.get("reg-updates");
        for (int i = 0; notifications != null && i < notifications.size(); ++i) {
            Map entry = (Map) notifications.get(i);
            this.errorLogger().info("StarterKit: CoAP re-registration: " + entry);
            boolean existing_connection = this.hasMQTTConnection((String) entry.get("ep"));
            if (existing_connection == false) {
                this.removeMQTTConnection((String) entry.get("ep"));
                this.processRegistration(data, "reg-updates", true);
            }
        }
    }

    // OVERRIDE: handle de-registrations for StarterKit
    @Override
    public String[] processDeregistrations(Map parsed) {
        String[] deregistration = super.processDeregistrations(parsed);
        for (int i = 0; deregistration != null && i < deregistration.length; ++i) {
            // DEBUG
            this.errorLogger().info("StarterKit : CoAP de-registration: " + deregistration[i]);

            // Simply remove the StarterKit MQTT Connection... 
            this.removeMQTTConnection(deregistration[i]);
        }
        return deregistration;
    }
    
    // OVERRIDE: process notification for StarterKit
    @Override
    public void processNotification(Map data) {
        // DEBUG
        //this.errorLogger().info("processsNotification(StarterKit)...");

        // get the list of parsed notifications
        List notifications = (List) data.get("notifications");
        for (int i = 0; notifications != null && i < notifications.size(); ++i) {
            // we have to process the payload... this may be dependent on being a string core type... 
            Map notification = (Map) notifications.get(i);

            // decode the Payload...
            String b64_coap_payload = (String) notification.get("payload");
            String decoded_coap_payload = Utils.decodeCoAPPayload(b64_coap_payload);

            // Try a JSON parse... if it succeeds, assume the payload is a composite JSON value...
            Map json_parsed = this.tryJSONParse(decoded_coap_payload);
            if (json_parsed != null) {
                // add in a JSON object payload value directly... 
                notification.put("value", json_parsed);
            }
            else {
                // add in a decoded payload value as a string type...
                notification.put("value", decoded_coap_payload);
            }

            // StarterKit
            notification.put("myName", (String) notification.get("ep"));

            // we will send the raw CoAP JSON... StarterKit can parse that... 
            String coap_raw_json = this.jsonGenerator().generateJson(notification);

            // strip off []...
            String coap_json_stripped = this.stripArrayChars(coap_raw_json);

            // encapsulate into a coap/device packet...
            String starterkit_coap_json = coap_json_stripped;
            if (this.m_starterkit_device_data_key != null && this.m_starterkit_device_data_key.length() > 0) {
                starterkit_coap_json = "{ \"" + this.m_starterkit_device_data_key + "\":" + coap_json_stripped + "}";
            }

            // DEBUG
            this.errorLogger().info("StarterKit: CoAP notification: " + starterkit_coap_json);

            // build out the clientID
            String clientID = this.createStarterKitClientID((String) notification.get("ep"), m_mds_domain);

            // send to StarterKit...
            if (this.mqtt(clientID) != null) {
                this.mqtt(clientID).sendMessage(this.m_starterkit_observe_notification_topic, starterkit_coap_json, QoS.AT_MOST_ONCE);
            }
            else {
                this.errorLogger().info("StarterKit: CoAP notification: clientID: " + clientID + " connection reset");
                this.removeMQTTConnection((String) notification.get("ep"));
            }
        }
    }
    
    // we override the authentication hash creation and set it the same for all...
    @Override
    public String createAuthenticationHash() {
        if (this.m_auth_hash == null) {
            // we create a pretend device
            String ep_name = "_fake_device_";

            // create the clientID
            String clientID = this.createStarterKitClientID(ep_name, this.m_mds_domain);

            // Create the MQTT Transport
            MQTTTransport mqtt = new MQTTTransport(this.errorLogger(), this.preferences());
            mqtt.setClientID(clientID);

            // add the transport
            this.addMQTTTransport(clientID, mqtt);

            // we use no username/password in StarterKit mode
            this.mqtt(clientID).setUsername("off");
            this.mqtt(clientID).setPassword("off");

            // now get the hash
            this.m_auth_hash = this.mqtt(clientID).createAuthenticationHash();

            // delete the device
            this.removeMQTTConnection(ep_name);
        }

        // return the hash
        return this.m_auth_hash;
    }
    
    // OVERRIDE: (Listening) Topics for StarterKit vs. stock MQTT...
    @Override
    @SuppressWarnings("empty-statement")
    protected void subscribeToMQTTTopics() {
        // not used for StarterKit - we only see notifications, no flow to the device...
        ;
    }

    // create the StarterKit compatible clientID
    private String createStarterKitClientID(String device_id, String domain) {
        //
        // StarterKit clientID format:  "d:<org>:<type>:<device id>"
        // Where:
        // org - "quickstart" 
        // type - we list it as "iotsample-mbed"
        // deviceID - we bring in the custom name
        //
        String device_type = this.prefValue("starterkit_device_type", this.m_suffix);
        return "d:quickstart:" + device_type + ":" + device_id;
    }

    // add new MQTT Connection for a specific device
    private boolean addNewMQTTConnection(String ep_name) {
        // create the clientID
        String clientID = this.createStarterKitClientID(ep_name, this.m_mds_domain);

        // DEBUG
        this.errorLogger().info("StarterKit: Adding StarterKit Connection for clientID=" + clientID + "...");

        // Create the MQTT Transport
        MQTTTransport mqtt = new MQTTTransport(this.errorLogger(), this.preferences());
        mqtt.setClientID(clientID);

        // add the transport
        this.addMQTTTransport(clientID, mqtt);

        // we use no username/password in StarterKit mode
        this.mqtt(clientID).setUsername("off");
        this.mqtt(clientID).setPassword("off");

        // return connection status
        this.errorLogger().info("StarterKit: Connection to StarterKit for clientID=" + clientID + "...");
        return this.connect(clientID);
    }

    // remove an existing MQTT Connection as a device has de-registered
    private void removeMQTTConnection(String ep_name) {
        // create the clientID
        String clientID = this.createStarterKitClientID(ep_name, this.m_mds_domain);

        // DEBUG
        this.errorLogger().info("StarterKit: Removing StarterKit Connection for clientID=" + clientID + "...");

        // disconnect it
        this.disconnect(clientID);

        // remove the MQTT connection
        this.remove(clientID);
    }

    // do we have an existing MQTT Connection to this device?
    private boolean hasMQTTConnection(String ep_name) {
        boolean has_connection = false;

        // create the clientID
        String clientID = this.createStarterKitClientID(ep_name, this.m_mds_domain);

        // do we have a connection?
        if (this.mqtt(clientID) != null) {
            // is it connected?
            has_connection = this.mqtt(clientID).isConnected();
        }

        // DEBUG
        if (has_connection == true) {
            // DEBUG
            this.errorLogger().info("StarterKit: StarterKit Connection exists for clientID=" + clientID);
        }
        else {
            // DEBUG
            this.errorLogger().info("StarterKit: StarterKit Connection does not exist for clientID=" + clientID + " (OK).");
        }

        // return the connection state
        return has_connection;
    }
    
    // OVERRIDE: Connection to StarterKit vs. stock MQTT...
    @Override
    protected boolean connectMQTT() {
        // we simply return true
        return true;
    }
    
    // connect
    private boolean connect(String clientID) {
        // if not connected attempt
        if (!this.isConnected(clientID)) {
            if (this.mqtt(clientID).connect(this.m_mqtt_ip_address, this.m_mqtt_port, clientID, true)) {
                this.orchestrator().errorLogger().info("StarterKit: connection (" + clientID + ") completed successfully");
            }
        }
        else {
            // already connected
            this.orchestrator().errorLogger().info("StarterKit: Already connected (OK)...");
        }

        // return our connection status
        this.orchestrator().errorLogger().info("StarterKit: Connection status (" + clientID + "): " + this.isConnected(clientID));
        return this.isConnected(clientID);
    }

    // disconnect
    private void disconnect(String clientID) {
        if (this.isConnected(clientID)) {
            this.mqtt(clientID).disconnect();
        }
    }

    // are we connected
    private boolean isConnected(String clientID) {
        if (this.mqtt(clientID) != null) {
            return this.mqtt(clientID).isConnected();
        }
        return false;
    }
}
