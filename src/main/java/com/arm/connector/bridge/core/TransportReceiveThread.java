/**
 * @file    TransportReceiveThread.java
 * @brief Generic transport receive thread base class
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
package com.arm.connector.bridge.core;

/**
 * Receive Thread for inbound message processing
 *
 * @author Doug Anson
 */
public class TransportReceiveThread extends Thread implements Transport.ReceiveListener {

    private boolean m_running = false;
    private Transport m_transport = null;
    private Transport.ReceiveListener m_listener = null;

    /**
     * Constructor
     *
     * @param mqtt
     */
    public TransportReceiveThread(Transport mqtt) {
        this.m_transport = mqtt;
        this.m_transport.setOnReceiveListener(this);
        this.m_running = false;
        this.m_listener = null;
    }

    /**
     * get our running state
     *
     * @return
     */
    public boolean isRunning() {
        return this.m_running;
    }

    /**
     * get our connection status
     *
     * @return
     */
    public boolean isConnected() {
        return this.m_transport.isConnected();
    }

    /**
     * disconnect
     */
    public void disconnect() {
        if (this.m_transport != null) {
            this.m_transport.disconnect();
        }
    }

    /**
     * run method for the receive thread
     */
    @Override
    public void run() {
        if (!this.m_running) {
            this.m_running = true;
            this.listenerThreadLoop();
        }
    }

    /**
     * main thread loop
     */
    @SuppressWarnings("empty-statement")
    private void listenerThreadLoop() {
        int sleep_time = ((this.m_transport.preferences().intValueOf("mqtt_receive_loop_sleep")) * 1000);
        while (this.m_running && this.m_transport.isConnected() == true) {
            // receive and process...
            this.m_transport.receiveAndProcess();

            // sleep for a bit...
            try {
                Thread.sleep(sleep_time);
            }
            catch (InterruptedException ex) {
                // silent
                ;
            }
        }
    }

    /**
     * set the receive listener
     *
     * @param listener
     */
    public void setOnReceiveListener(Transport.ReceiveListener listener) {
        this.m_listener = listener;
        this.m_transport.setOnReceiveListener(this);
    }

    /**
     * callback on Message Receive events
     *
     * @param message
     */
    @Override
    public void onMessageReceive(String topic, String message) {
        if (this.m_listener != null) {
            this.m_listener.onMessageReceive(topic, message);
        }
    }
}
