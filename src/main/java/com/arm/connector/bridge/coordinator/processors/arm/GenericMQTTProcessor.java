/**
 * @file    GenericMQTTProcessor.java
 * @brief Generic MQTT peer processor for connector bridge
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
package com.arm.connector.bridge.coordinator.processors.arm;

import com.arm.connector.bridge.coordinator.Orchestrator;
import com.arm.connector.bridge.coordinator.processors.core.PeerProcessor;
import com.arm.connector.bridge.coordinator.processors.interfaces.PeerInterface;
import com.arm.connector.bridge.transport.HttpTransport;
import com.arm.connector.bridge.transport.MQTTTransport;
import com.arm.connector.bridge.core.Transport;
import com.arm.connector.bridge.core.TransportReceiveThread;
import java.util.HashMap;
import org.fusesource.mqtt.client.QoS;
import org.fusesource.mqtt.client.Topic;

/**
 * Generic MQTT peer processor
 *
 * @author Doug Anson
 */
public class GenericMQTTProcessor extends PeerProcessor implements Transport.ReceiveListener, PeerInterface {
    protected TransportReceiveThread m_mqtt_thread = null;
    protected String m_mqtt_host = null;
    protected int m_mqtt_port = 0;
    protected String m_client_id = null;
    private HashMap<String, MQTTTransport> m_mqtt = null;
    private HttpTransport m_http = null;
    protected boolean m_use_clean_session = false;
    private String m_device_data_key = null;

    // Factory method for initializing the Sample 3rd Party peer
    public static GenericMQTTProcessor createPeerProcessor(Orchestrator manager, HttpTransport http) {
        return new GenericMQTTProcessor(manager, new MQTTTransport(manager.errorLogger(), manager.preferences()), http);
    }
    
    // constructor (singleton)
    public GenericMQTTProcessor(Orchestrator orchestrator, MQTTTransport mqtt, HttpTransport http) {
        this(orchestrator, mqtt, null, http);
    }

    // constructor (suffix for preferences)
    public GenericMQTTProcessor(Orchestrator orchestrator, MQTTTransport mqtt, String suffix, HttpTransport http) {
        super(orchestrator, suffix);

        // HTTP support if we need it
        this.m_http = http;

        // MQTT transport list
        this.m_mqtt = new HashMap<>();
        
        // initialize the topic root (MQTT)
        this.initTopicRoot("mqtt_mds_topic_root");

        // Get the device data key if one exists
        this.m_device_data_key = orchestrator.preferences().valueOf("mqtt_device_data_key", this.m_suffix);

        // build out our configuration
        this.m_mqtt_host = orchestrator.preferences().valueOf("mqtt_address", this.m_suffix);
        this.m_mqtt_port = orchestrator.preferences().intValueOf("mqtt_port", this.m_suffix);

        // establish the MQTT mDS request tag
        this.initRequestTag("mds_mqtt_request_tag");
        
        // clean session
        this.m_use_clean_session = this.orchestrator().preferences().booleanValueOf("mqtt_clean_session", this.m_suffix);

        // assign our MQTT transport if we have one...
        if (mqtt != null) {
            this.m_client_id = mqtt.getClientID();
            this.addMQTTTransport(this.m_client_id, mqtt);
        }

        // auto-subscribe behavior
        this.initAutoSubscribe("mqtt_obs_auto_subscribe");

        // setup our MQTT listener if we have one...
        if (mqtt != null) {
            // MQTT PeerProcessor listener thread setup
            this.m_mqtt_thread = new TransportReceiveThread(this.mqtt());
            this.m_mqtt_thread.setOnReceiveListener(this);
        }
    }

    // create our MQTT-based authentication hash
    @Override
    public String createAuthenticationHash() {
        return this.mqtt().createAuthenticationHash();
    }
    
    // start our MQTT listener
    @Override
    public void initListener() {
        // connect and begin listening for requests (wildcard based on request TAG and domain)
        if (this.connectMQTT()) {
            this.subscribeToMQTTTopics();
            if (this.m_mqtt_thread != null) {
                this.m_mqtt_thread.start();
            }
        }
    }

    // stop our MQTT listener
    @Override
    public void stopListener() {
        if (this.mqtt() != null) {
            this.mqtt().disconnect();
        }
    }
    
    // GenericSender Implementation: send a message
    @Override
    public void sendMessage(String to, String message) {
        // send a message over Google Cloud...
        this.errorLogger().info("sendMessage(MQTT-STD): Sending Message to: " + to + " message: " + message);
        
        // send the message over MQTT
        this.mqtt().sendMessage(to, message);
    }

    // OVERRIDE: Connection stock MQTT...
    protected boolean connectMQTT() {
        return this.mqtt().connect(this.m_mqtt_host, this.m_mqtt_port, null, true);
    }

    // OVERRIDE: Topics for stock MQTT...
    protected void subscribeToMQTTTopics() {
        String request_topic_str = this.getTopicRoot() + this.getRequestTag() + this.getDomain() + "/#";
        this.errorLogger().info("subscribeToMQTTTopics(MQTT-STD): listening on REQUEST topic: " + request_topic_str);
        Topic request_topic = new Topic(request_topic_str, QoS.AT_LEAST_ONCE);
        Topic[] topic_list = {request_topic};
        this.mqtt().subscribe(topic_list);
    }

    // get HTTP if needed
    protected HttpTransport http() {
        return this.m_http;
    }

    // get our defaulted reply topic (defaulted)
    public String getReplyTopic(String ep_name, String ep_type, String def) {
        return def;
    }

    // add a MQTT transport instance
    protected void addMQTTTransport(String clientID, MQTTTransport mqtt) {
        this.m_mqtt.put(clientID, mqtt);
    }

    // initialize the MQTT transport instance list
    protected void initMQTTTransportList() {
        this.closeMQTTTransportList();
        this.m_mqtt.clear();
    }

    // PROTECTED: get the MQTT transport for the default clientID
    protected MQTTTransport mqtt() {
        return this.mqtt(this.m_client_id);
    }

    // PROTECTED: get the MQTT transport for a given clientID
    protected MQTTTransport mqtt(String clientID) {
        return this.m_mqtt.get(clientID);
    }

    // PROTECTED: remove MQTT Transport for a given clientID
    protected void remove(String clientID) {
        this.m_mqtt.remove(clientID);
    }

    // close the tranports in the list
    @SuppressWarnings("empty-statement")
    private void closeMQTTTransportList() {
        for (String key : this.m_mqtt.keySet()) {
            try {
                MQTTTransport mqtt = this.m_mqtt.get(key);
                if (mqtt != null) {
                    if (mqtt.isConnected()) {
                        mqtt.disconnect(true);
                    }
                }
            }
            catch (Exception ex) {
                // silent
                ;
            }
        }
    }
}
