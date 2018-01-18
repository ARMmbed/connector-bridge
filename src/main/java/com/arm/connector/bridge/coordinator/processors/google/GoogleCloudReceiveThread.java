/**
 * @file    GoogleCloudReceiveThread.java
 * @brief Google Cloud PubSub Receive Thread
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
package com.arm.connector.bridge.coordinator.processors.google;

import com.arm.connector.bridge.core.Transport;
import com.google.api.services.pubsub.Pubsub;
import com.google.api.services.pubsub.model.AcknowledgeRequest;
import com.google.api.services.pubsub.model.PubsubMessage;
import com.google.api.services.pubsub.model.PullRequest;
import com.google.api.services.pubsub.model.PullResponse;
import com.google.api.services.pubsub.model.ReceivedMessage;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

/**
 * Google Cloud PubSub Receive Thread
 * @author Doug Anson
 */
public class GoogleCloudReceiveThread extends Thread implements Transport.ReceiveListener, Serializable {
    private GoogleCloudReceiveThread.ReceiveListener m_receiver = null;
    private int m_sleep_time = 0;
    private int m_max_messages = 0;
    private boolean m_running = false;
    private Pubsub m_pubsub = null;
    private String m_goo_subscription = null;
    
    private static final long serialVersionUID = 7526472295622776148L;
    
    // ReceiveListener class for GoogleCloud PubSub callback event processing
    public interface ReceiveListener {

        /**
         * on message receive, this will be callback to the registered listener
         * @param topic
         * @param message
         */
        public void onMessageReceive(String topic, String message);
        
        /**
         * reverse format topic to compatible forward slashes...
         * @param goo_subscription subscription in google format
         * @return reverse format original topic in connector-bridge format
         */
        public String connectorTopicFromGoogleSubscription(String goo_subscription);
    }
    
    // Constructor
    public GoogleCloudReceiveThread(GoogleCloudReceiveThread.ReceiveListener receiver,Pubsub pubsub,int sleep_time,int max_messages,String goo_subscription) {
        this.m_receiver = receiver;
        this.m_pubsub = pubsub;
        this.m_sleep_time = sleep_time;
        this.m_max_messages = max_messages;
        this.m_goo_subscription = goo_subscription;
    }
    
    // message receiver handler
    @Override
    public void onMessageReceive(String topic, String message) {
        if (this.m_receiver != null) {
            this.m_receiver.onMessageReceive(topic, message);
        }
    }
    
    // get our running state
    public boolean isRunning() {
        return this.m_running;
    }
    
    // start receiving
    public void start_listening() {
        if (this.m_running == false) {
            this.start();
        }
    }
    // stop receiving
    public void stop_listening() {
        this.m_running = false;
    }
    
    // run method for the receive thread
    @Override
    public void run() {
        this.m_running = true;
        this.listenOnSubscription(this.m_goo_subscription);
    }

    // main thread loop
    @SuppressWarnings("empty-statement")
    private void listenOnSubscription(String goo_subscription) {
        // loop..
        while (this.m_running) {
            // pull new messages from Google Cloud's PubSub
            try {
                PullRequest pullRequest = new PullRequest()
                        .setReturnImmediately(false)
                        .setMaxMessages(this.m_max_messages);

                // DEBUG
                //System.out.println("Quering subscription for messages: " + goo_subscription + "...");

                PullResponse pullResponse = this.m_pubsub.projects().subscriptions()
                        .pull(goo_subscription, pullRequest)
                        .execute();

                List<ReceivedMessage> receivedMessages = pullResponse.getReceivedMessages();
                List<String> ackIds = new ArrayList<>();

                // DEBUG
                //System.out.println("Number of received messages: " + receivedMessages.size());

                if (receivedMessages != null) {
                    // Now process the messages we have...
                    for (ReceivedMessage receivedMessage : receivedMessages) {
                        // save the ACK ID
                        ackIds.add(receivedMessage.getAckId());

                        // parse the payload
                        PubsubMessage pubsubMessage = receivedMessage.getMessage();
                        String message = new String(pubsubMessage.decodeData(),"UTF-8");

                        // create a compatible topic with forward slashes...
                        String topic = this.m_receiver.connectorTopicFromGoogleSubscription(goo_subscription);

                        // call on receive
                        this.onMessageReceive(topic, message);  
                    }

                    // ack the messages
                    AcknowledgeRequest ackRequest = new AcknowledgeRequest();
                            ackRequest.setAckIds(ackIds);
                            this.m_pubsub.projects().subscriptions()
                                    .acknowledge(goo_subscription, ackRequest)
                                    .execute();
                }
                else {
                    // debug
                    //System.out.println("WARN: GoogleCloudReceiveThread: no messages received (OK).");
                }
            }
            catch (com.google.api.client.googleapis.json.GoogleJsonResponseException ex) {
                // debug
                //System.out.println("WARN: GoogleCloudReceiveThread: exception(GoogleJsonResponseException) during message receive: " + ex.getMessage() + " subscription: " + goo_subscription);
            }
            catch (IOException | NullPointerException ex) {
                // debug
                // System.out.println("WARN: GoogleCloudReceiveThread: exception during message receive: " + ex.getMessage() + " subscription: " + goo_subscription);
            }

            // sleep for a bit...
            try {
                Thread.sleep(this.m_sleep_time);
            }
            catch (InterruptedException ex) {
                // silent
                ;
            }
        }
    }
    
    /**
     * Always treat de-serialization as a full-blown constructor, by
     * validating the final state of the de-serialized object.
     */
    private void readObject(ObjectInputStream aInputStream) throws ClassNotFoundException, IOException {
        //always perform the default de-serialization first
        aInputStream.defaultReadObject();
        
        // XXX TODO custom readObject() tasks...
    }

    /**
     * This is the default implementation of writeObject.
     * Customise if necessary.
     */
    private void writeObject(ObjectOutputStream aOutputStream) throws IOException {
        //perform the default serialization for all non-transient, non-static fields
        aOutputStream.defaultWriteObject();
        
        // XXX TODO custom writeObject() tasks...
    }
}
