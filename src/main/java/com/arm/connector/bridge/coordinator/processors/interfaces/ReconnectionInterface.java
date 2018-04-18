/*
 * Copyright 2018 douans01.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.arm.connector.bridge.coordinator.processors.interfaces;

import com.arm.connector.bridge.transport.MQTTTransport;

/**
 * Interface to undergo a network re-connection cycle
 * @author Doug Anson
 */
public interface ReconnectionInterface {
    // start the listener Thread for the MQTT connection handler
    public void startListenerThread(String ep_name,MQTTTransport mqtt);
    
    // finish a reconnection sequence/cycle
    public void finishReconnection(String ep_name,String ep_type,MQTTTransport mqtt,ReconnectionInterface ri);
}
