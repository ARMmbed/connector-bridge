/**
 * @file    MQTTTransport.java
 * @brief   MQTT Transport Support 
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

package com.arm.connector.bridge.transport;

import com.arm.connector.bridge.core.Transport;
import com.arm.connector.bridge.core.ErrorLogger;
import com.arm.connector.bridge.core.Utils;
import com.arm.connector.bridge.preferences.PreferenceManager;
import java.io.EOFException;
import java.io.FileInputStream;
import java.io.IOException;
import java.security.KeyManagementException;
import java.security.KeyStore;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.PrivateKey;
import java.security.PublicKey;
import java.security.SecureRandom;
import java.security.UnrecoverableKeyException;
import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;
import javax.net.ssl.KeyManager;
import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManager;
import javax.net.ssl.TrustManagerFactory;
import javax.net.ssl.X509TrustManager;
import org.bouncycastle.jce.provider.BouncyCastleProvider;
import org.fusesource.mqtt.client.BlockingConnection;
import org.fusesource.mqtt.client.MQTT;
import org.fusesource.mqtt.client.QoS;
import org.fusesource.mqtt.client.Topic;

/**
 * MQTT Transport Support
 *
 * @author Doug Anson
 */
public class MQTTTransport extends Transport {

    private static MQTTTransport m_self = null;
    private BlockingConnection m_connection = null;
    private byte[] m_qoses = null;
    private String m_suffix = null;
    private String m_username = null;
    private String m_password = null;
    private String m_host_url = null;
    private int m_sleep_time = 0; 
    private String m_client_id = null;
 
    private String m_connect_host = null;
    private int m_connect_port = 0;
    private String m_connect_client_id = null;
    private boolean m_connect_clean_session = false;
    
    private int m_num_retries = 0;
    private int m_max_retries = 10;
    private Topic[] m_subscribe_topics = null;
    private String[] m_unsubscribe_topics = null;
    private boolean m_forced_ssl = false;
    
    private boolean m_use_pki = false;
    private String m_pki_priv_key = null;
    private String m_pki_pub_key = null;
    private String m_pki_cert = null;
    private SSLContext m_ssl_context = null;
    
    private String m_keystore_pw = null;
    private String m_base_dir = null;
    private String m_keystore_filename = null;
    private String m_keystore_basename = null;
    private X509Certificate m_cert = null;
    private PublicKey m_pubkey = null;
    private PrivateKey m_privkey = null;
    private boolean m_set_mqtt_version = true;
    
    /**
     * Instance Factory
     * @param error_logger
     * @param preference_manager
     * @return
     */
    public static Transport getInstance(ErrorLogger error_logger, PreferenceManager preference_manager) {
        if (MQTTTransport.m_self == null) // create our MQTT transport
        {
            MQTTTransport.m_self = new MQTTTransport(error_logger, preference_manager);
        }
        return MQTTTransport.m_self;
    }
    
    /**
     * Constructor
     * @param error_logger
     * @param preference_manager
     * @param suffix
     */
    public MQTTTransport(ErrorLogger error_logger, PreferenceManager preference_manager,String suffix) {
        super(error_logger,preference_manager);
        this.m_use_pki = false;
        this.m_forced_ssl = false;
        this.m_ssl_context = null;
        this.m_host_url = null;
        this.m_suffix = suffix;
        this.m_keystore_filename = null;
        this.m_set_mqtt_version = true;
        
        this.setUsername(this.prefValue("mqtt_username",this.m_suffix));
        this.setPassword(this.prefValue("mqtt_password",this.m_suffix));
        this.m_max_retries = this.preferences().intValueOf("mqtt_connect_retries",this.m_suffix);
        this.m_sleep_time = ((this.preferences().intValueOf("mqtt_receive_loop_sleep",this.m_suffix))*1000);
        this.m_keystore_pw = this.preferences().valueOf("mqtt_keystore_pw",this.m_suffix);
        this.m_base_dir = this.preferences().valueOf("mqtt_keystore_basedir",this.m_suffix);
        this.m_keystore_basename = this.preferences().valueOf("mqtt_keystore_basename",this.m_suffix);
    }

    /**
     * Constructor
     * @param error_logger
     * @param preference_manager
     */
    public MQTTTransport(ErrorLogger error_logger, PreferenceManager preference_manager) {
        super(error_logger, preference_manager);
        this.m_use_pki = false;
        this.m_forced_ssl = false;
        this.m_ssl_context = null;
        this.m_host_url = null;
        this.m_suffix = null;
        this.m_keystore_filename = null;
        this.m_set_mqtt_version = true;
                
        this.setUsername(this.prefValue("mqtt_username",this.m_suffix));
        this.setPassword(this.prefValue("mqtt_password",this.m_suffix));
        this.m_max_retries = this.preferences().intValueOf("mqtt_connect_retries",this.m_suffix);
        this.m_sleep_time = ((this.preferences().intValueOf("mqtt_receive_loop_sleep",this.m_suffix))*1000);
        this.m_keystore_pw = this.preferences().valueOf("mqtt_keystore_pw",this.m_suffix);
        this.m_base_dir = this.preferences().valueOf("mqtt_keystore_basedir",this.m_suffix);
        this.m_keystore_basename = this.preferences().valueOf("mqtt_keystore_basename",this.m_suffix);
    }
    
    // disable setting of MQTT version
    public void enableMQTTVersionSet(boolean set_mqtt_version) {
        this.m_set_mqtt_version = set_mqtt_version;
    }
    
    // reset to using username/passwords
    public void useUserPass() {
        this.useUserPass(null,null,null);
    }
    
    // reset to using username/passwords
    public void useUserPass(String username,String pw,String client_id) {
        this.useUserPass(username,pw,client_id,false);
    }
    
    // reset to using username/passwords
    public void useUserPass(String username,String pw,String client_id,boolean use_ssl) {
        this.m_use_pki = false;
        this.m_ssl_context = null;
        this.m_forced_ssl = use_ssl;
        this.setUsername(username);
        this.setPassword(pw);
        this.setClientID(client_id);
    }
    
    // utilize PKI certs
    public void enablePKI(String priv_key,String pub_key,String certificate,String id) {
        this.m_pki_priv_key = priv_key;
        this.m_pki_pub_key = pub_key;
        this.m_pki_cert = certificate;
        if (this.initializeSSLContext(id) == true) {
            this.m_use_pki = true;
            this.m_forced_ssl = true;
        }
        else {
            this.m_pki_priv_key = null;
            this.m_pki_pub_key = null;
            this.m_pki_cert = null;
            this.m_ssl_context = null;
        }
    }
    
    // create the key manager
    private KeyManager[] createKeyManager(String keyStoreType) {
        KeyManager[] kms = null;
        FileInputStream fs = null;
       
        try {
            KeyManagerFactory   kmf = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
            KeyStore ks = KeyStore.getInstance(keyStoreType);
            fs = new FileInputStream(this.m_keystore_filename);
            ks.load(fs, this.m_keystore_pw.toCharArray());
            kmf.init(ks,this.m_keystore_pw.toCharArray());
            kms = kmf.getKeyManagers();
        }
        catch (NoSuchAlgorithmException | KeyStoreException | IOException | CertificateException | UnrecoverableKeyException ex) {
            this.errorLogger().warning("createKeyManager: Exception in creating the KeyManager list",ex);
        }
        
        try {
            if (fs != null) fs.close();
        }
        catch (Exception ex) {
            // silent
        }
        
        return kms;
    }
    
    // create the trust manager
    private TrustManager[] createTrustManager() {
        TrustManager tm[] = new TrustManager[1];
        tm[0] = new MQTTTrustManager(this.m_keystore_filename,this.m_keystore_pw);
        return tm;
    }
    
    // create keystore
    private String initializeKeyStore(String id) {
        // create our credentials
        this.m_cert = Utils.createX509CertificateFromPEM(this.errorLogger(),this.m_pki_cert,"X509");
        this.m_privkey = Utils.createPrivateKeyFromPEM(this.errorLogger(),this.m_pki_priv_key,"RSA");
        
        // also hold onto the public key
        this.m_pubkey = Utils.createPublicKeyFromPEM(this.errorLogger(),this.m_pki_pub_key,"RSA");
        
        // set our keystore PW
        this.m_keystore_pw = Utils.generateKeystorePassword(this.m_keystore_pw,id);

        // create the keystore
        return Utils.createKeystore(this.errorLogger(),this.m_base_dir,id,this.m_keystore_basename,this.m_cert,this.m_privkey,this.m_keystore_pw);
    }
    
    // initialize the SSL context
    private boolean initializeSSLContext(String id) {
        try {
            // enable proper parsing of the PKCS#1 private key from AWS... not sure why they dont just send it as PKCS#8...
            java.security.Security.addProvider(new BouncyCastleProvider());
            
            // initialize the keystores...
            this.m_keystore_filename = this.initializeKeyStore(id);
            if (this.m_keystore_filename != null) {
                // create our SSL context - FYI: AWS IoT requires TLS v1.2
                this.m_ssl_context = SSLContext.getInstance("TLSv1.2");
                
                // initialize the SSL context with our KeyManager and our TrustManager
                KeyManager km[] = this.createKeyManager("JKS");
                TrustManager tm[] = this.createTrustManager();
                this.m_ssl_context.init(km,tm,new SecureRandom());
                return true;
            }
        }
        catch (NoSuchAlgorithmException | KeyManagementException ex) {
            // exception caught
            this.errorLogger().critical("MQTTTransport: initializeSSLContext(PKI) failed. PKI DISABLED",ex);
        }
        return false;
    }
     
    // PUBLIC: Create the authentication hash
    public String createAuthenticationHash() {
        return Utils.createHash(this.getUsername() + "_" + this.getPassword() + "_" + this.prefValue("mqtt_client_id",this.m_suffix));
    }
        
    // PRIVATE: Username/PW for MQTT connection
    private String getUsername() {
        return this.m_username;
    }

    // PRIVATE: Username/PW for MQTT connection
    private String getPassword() {
        return this.m_password;
    }
    
    // PUBLIC: Get the client ID
    public String getClientID() {
        return this.m_client_id;
    }
    
    // PUBLIC: Set the client ID
    public void setClientID(String clientID) {
        this.m_client_id = clientID;
    }
    
     /**
     * Set the MQTT Username
     * @param username
     */
    public final void setUsername(String username) {
        this.m_username = username;
    }

    /**
     * Set the MQTT Password
     * @param password
     */
    public final void setPassword(String password) {
        this.m_password = password;
    }

    /**
     * Are we connected to a MQTT broker?
     * @return
     */
    @Override
    public boolean isConnected() {
        if (this.m_connection != null) {
            return this.m_connection.isConnected();
        }
        //this.errorLogger().warning("WARNING: MQTT connection instance is NULL...");
        return super.isConnected();
    }
    
    /**
     * Connect to the MQTT broker
     * @param host
     * @param port
     * @return
     */
    @Override
    public boolean connect(String host, int port) {
        return this.connect(host,port,this.prefValue("mqtt_client_id",this.m_suffix), this.prefBoolValue("mqtt_clean_session",this.m_suffix));
    }
    
    /**
     * Connect to the MQTT broker
     * @param host
     * @param port
     * @param clientID
     * @return
     */
    public boolean connect(String host, int port, String clientID) {
        return this.connect(host,port,clientID,this.prefBoolValue("mqtt_clean_session",this.m_suffix));
    }
    /**
     * Connect to the MQTT broker
     * @param host
     * @param port
     * @param clientID
     * @param clean_session
     * @return
     */
    public boolean connect(String host, int port, String clientID,boolean clean_session) {
        int sleep_time = this.prefIntValue("mqtt_retry_sleep",this.m_suffix);
        int num_tries = this.prefIntValue("mqtt_connect_retries",this.m_suffix);
        for(int i=0;i<num_tries && !this.m_connected; ++i) {
            try {                
                // MQTT endpoint 
                MQTT endpoint = new MQTT();
                
                // build out the URL connection string
                String url = this.setupHostURL(host, port);
                
                // setup default clientID
                if (clientID == null || clientID.length() <= 0) {
                    clientID = this.prefValue("mqtt_client_id",this.m_suffix);
                }
                
                // DEBUG
                this.errorLogger().info("MQTTTransport: URL: [" + url + "] clientID: [" + clientID + "] clean: " + clean_session);
 
                // setup the hostname & port
                endpoint.setHost(url);
                
                // set the MQTT version
                String mqtt_version = this.prefValue("mqtt_version",this.m_suffix);
                if (mqtt_version != null && this.m_set_mqtt_version == true) {
                    endpoint.setVersion(mqtt_version);
                }
                
                if (this.m_use_pki == true) {
                    // DEBUG
                    this.errorLogger().info("MQTTTransport: PKI Cert/Keys Used");
                    
                    // PKI
                    endpoint.setSslContext(this.m_ssl_context);
                }
                else {
                    // non-PKI: configure credentials
                    String username = this.getUsername();
                    String pw = this.getPassword();
                    
                    // DEBUG
                    this.errorLogger().info("MQTTTransport: Username: [" + username + "] pw: [" + pw + "] Used");
                
                    if (username != null && username.length() > 0 && username.equalsIgnoreCase("off") == false) {
                        endpoint.setUserName(username);
                    }
                    if (pw != null && pw.length() > 0 && pw.equalsIgnoreCase("off") == false) {
                        endpoint.setPassword(pw);
                    }
                }
                
                // configure options... 
                if (clientID != null && clientID.length() > 0 && clientID.equalsIgnoreCase("off") == false) {
                    endpoint.setClientId(clientID);
                }
                else if (clean_session == false) {
                    String def_client_id = this.prefValue("mqtt_default_client_id",this.m_suffix);
                    if (def_client_id != null && def_client_id.equalsIgnoreCase("off") == false) {
                        // set a defaulted clientID
                        endpoint.setClientId(def_client_id);
                    }
                }
                endpoint.setCleanSession(clean_session);
                String will = this.prefValue("mqtt_will_message",this.m_suffix);
                if (will != null && will.length() > 0 && will.equalsIgnoreCase("off") == false) {
                    endpoint.setWillMessage(will);
                }
                String will_topic = this.prefValue("mqtt_will_topic",this.m_suffix);
                if (will_topic != null && will_topic.length() > 0 && will_topic.equalsIgnoreCase("off") == false) {
                    endpoint.setWillTopic(will_topic);
                }
                int trafficClass = this.prefIntValue("mqtt_traffic_class",this.m_suffix);
                if (trafficClass >= 0) {
                    endpoint.setTrafficClass(trafficClass);
                }
                int reconnectAttempts = this.prefIntValue("mqtt_reconnect_retries_max",this.m_suffix);
                if (reconnectAttempts >= 0) {
                    endpoint.setReconnectAttemptsMax(reconnectAttempts);
                }
                long reconnectDelay = (long)this.prefIntValue("mqtt_reconnect_delay",this.m_suffix);
                if (reconnectDelay >= 0) {
                    endpoint.setReconnectDelay(reconnectDelay);
                }
                long reconnectDelayMax = (long)this.prefIntValue("mqtt_reconnect_delay_max",this.m_suffix);
                if (reconnectDelayMax >= 0) {
                    endpoint.setReconnectDelayMax(reconnectDelayMax);
                }
                float backoffMultiplier = this.prefFloatValue("mqtt_backoff_multiplier",this.m_suffix);
                if (backoffMultiplier >= 0) {
                    endpoint.setReconnectBackOffMultiplier(backoffMultiplier);
                }
                short keepAlive = (short)this.prefIntValue("mqtt_keep_alive",this.m_suffix);
                if (keepAlive >= 0) {
                    endpoint.setKeepAlive(keepAlive);
                }
                if (endpoint.getClientId() != null) {
                    this.m_client_id = endpoint.getClientId().toString();
                }
                
                try {
                    // connect MQTT...
                    this.m_endpoint = endpoint;
                    this.m_connection = endpoint.blockingConnection();
                    if (this.m_connection != null) {
                        // attempt connection
                        this.m_connection.connect();

                        // sleep for a short bit...
                        try {
                            Thread.sleep(sleep_time);
                        }
                        catch (InterruptedException ex) {
                            this.errorLogger().critical("MQTTTransport(connect): sleep interrupted",ex);
                        }

                        // check our connection status
                        this.m_connected = this.m_connection.isConnected();

                        // DEBUG
                        if (this.m_connected == true) {
                            this.errorLogger().warning("MQTTTransport: Connection to: " + url + " successful");
                            this.m_connect_host = host;
                            this.m_connect_port = port;
                            if (endpoint.getClientId() != null) {
                                this.m_client_id = endpoint.getClientId().toString();
                            }
                            else {
                                this.m_client_id = null;
                            }
                            this.m_connect_client_id = this.m_client_id;
                            this.m_connect_clean_session = clean_session;
                        }
                        else {
                            this.errorLogger().warning("MQTTTransport: Connection to: " + url + " FAILED");
                        }
                    }
                    else {
                        this.errorLogger().warning("WARNING: MQTT connection instance is NULL. connect() failed");
                    }
                }
                catch (Exception ex) {
                    this.errorLogger().warning("MQTTTransport: Exception during connect()",ex);
                    
                    // DEBUG
                    this.errorLogger().warning("MQTT: URL: " + url);
                    this.errorLogger().warning("MQTT: clientID: " + this.m_client_id);
                    this.errorLogger().warning("MQTT: clean_session: " + clean_session);
                    if (this.m_use_pki == false) {
                        this.errorLogger().warning("MQTT: username: " + this.getUsername());
                        this.errorLogger().warning("MQTT: password: " + this.getPassword());
                    }
                    this.errorLogger().warning("MQTT: host: " + host);
                    this.errorLogger().warning("MQTT: port: " + port);
                    
                    // sleep for a short bit...
                    try {
                        Thread.sleep(sleep_time);
                    }
                    catch (InterruptedException ex2) {
                        this.errorLogger().critical("MQTTTransport(connect): sleep interrupted",ex2);
                    }
                }
            }
            catch (Exception ex) {
                this.errorLogger().critical("MQTTTransport(connect): Exception occured", ex);
                this.m_connected = false;
            }
            
            // if we have not yet connected... sleep a bit more and retry...
            if (this.m_connected == false) {
                try {
                    Thread.sleep(sleep_time);
                }
                catch (InterruptedException ex) {
                    this.errorLogger().critical("MQTTTransport(retry): sleep interrupted",ex);
                }
            }
        }
        
        // return our connection status
        return this.m_connected;
    }

    /**
     * Main handler for receiving and processing MQTT Messages (called repeatedly by TransportReceiveThread...)
     * @return true - processed (or empty), false - failure
     */
    @Override
    public boolean receiveAndProcess() {
        // DEBUG
        //this.errorLogger().info("MQTTTransport: in receiveAndProcess()...");
        if (this.isConnected()) {
            try {
                // receive the MQTT message and process it...
                this.errorLogger().info("MQTTTransport: in receiveAndProcess(). Calling receiveAndProcessMessage()...");
                this.receiveAndProcessMessage();
            }
            catch (Exception ex) {
                // note
                this.errorLogger().info("MQTTTransport: caught Exception in recieveAndProcess(): " + ex.getMessage());
                return false;
            }
            return true;
        }
        else {
            this.errorLogger().info("MQTTTransport: not connected (OK)");
            return true;
        }
    }
    
    // reset our MQTT connection... sometimes it goes wonky...
    private void resetConnection() {
        try {
            // disconnect()...
            ++this.m_num_retries;
            this.disconnect(false);
            
            // sleep a bit
            Thread.sleep(this.m_sleep_time);
            
            // reconnect()...
            if (this.reconnect() == true) {
                // DEBUG
                this.errorLogger().info("resetConnection: SUCCESS.");
                    
                // reconnected OK...
                this.m_num_retries = 0;
                
                // resubscribe
                if (this.m_subscribe_topics != null) {
                    // DEBUG
                    this.errorLogger().info("resetConnection: SUCCESS. re-subscribing...");
                    this.subscribe(this.m_subscribe_topics);
                }
            }
            else {
                // DEBUG
                this.errorLogger().info("resetConnection: FAILURE num_tries = " + this.m_num_retries);
            }
        }
        catch (Exception ex) {
            this.errorLogger().info("resetConnection: Exception: " + ex.getMessage(),ex);
        }
    }
    
    // have we exceeded our retry count?
    private Boolean retriesExceeded() {
        return (this.m_num_retries >= this.m_max_retries);
    }

    // subscribe to specific topics 
    public void subscribe(Topic[] list) {
        if (this.m_connection != null) {
            try {
                // DEBUG
                this.errorLogger().info("MQTTTransport: Subscribing to " + list.length + " topics...");
                
                // subscribe
                this.m_subscribe_topics = list;
                this.m_unsubscribe_topics = null;
                this.m_qoses = this.m_connection.subscribe(list);
                
                // DEBUG
                this.errorLogger().info("MQTTTransport: Subscribed to  " + list.length + " SUCCESSFULLY");
            }
            catch (Exception ex) {
                if (this.retriesExceeded()) {
                    // unable to subscribe to topic (final)
                    this.errorLogger().critical("MQTTTransport: unable to subscribe to topic (final)", ex);
                }
                else {
                    // unable to subscribe to topic
                    this.errorLogger().warning("MQTTTransport: unable to subscribe to topic (" + this.m_num_retries + " of " + this.m_max_retries + ")", ex);

                    // attempt reset
                    this.resetConnection();
                }
            }
        }
        else {
            // unable to subscribe - not connected... 
            this.errorLogger().warning("MQTTTransport: unable to subscribe. Connection is missing and/or NULL");
        }
    }
    
    // unsubscribe from specific topics
    public void unsubscribe(String[] list) {
        if (this.m_connection != null) {
            try {
                this.m_subscribe_topics = null;
                this.m_unsubscribe_topics = list;
                this.m_connection.unsubscribe(list);
                //this.errorLogger().info("MQTTTransport: Unsubscribed from TOPIC(s): " + list.length);
            }
            catch (Exception ex) {
                if (this.retriesExceeded()) {
                    // unable to unsubscribe from topic (final)
                    this.errorLogger().critical("MQTTTransport: unable to unsubscribe from topic (final)", ex);
                }
                else {
                    // unable to subscribe to topic
                    this.errorLogger().warning("MQTTTransport: unable to unsubscribe to topic (" + this.m_num_retries + " of " + this.m_max_retries + ")", ex);

                    // attempt reset
                    this.resetConnection();

                    // recall
                    this.unsubscribe(list);
                }
            }
        }
        else {
            // unable to subscribe - not connected... 
            this.errorLogger().warning("MQTTTransport: unable to unsubscribe. Connection is missing and/or NULL");
        }
    }

    /**
     * Publish a MQTT message
     * @param topic
     * @param message
     */
    @Override
    public void sendMessage(String topic,String message) {
        this.sendMessage(topic,message,QoS.AT_LEAST_ONCE);
    }

    /**
     * Publish a MQTT message
     * @param topic
     * @param message
     * @param qos
     * @return send status
     */
    public boolean sendMessage(String topic,String message,QoS qos) {
        boolean sent = false;
        if (this.m_connection.isConnected() == true && message != null) {
            try {
                // DEBUG
                this.errorLogger().info("sendMessage: message: " + message + " Topic: " + topic);
                this.m_connection.publish(topic, message.getBytes(), qos, false);
                
                // DEBUG
                this.errorLogger().info("sendMessage(MQTT): message sent. SUCCESS");
                sent = true;
            }
            catch (EOFException ex) {
                if (this.retriesExceeded()) {
                    // unable to send (EOF) - final
                    this.errorLogger().critical("sendMessage:EOF on message send... resetting MQTT (final): " + message, ex);
                }
                else {
                    // unable to send (EOF) - final
                    this.errorLogger().warning("sendMessage:EOF on message send... resetting MQTT (" + this.m_num_retries + " of " + this.m_max_retries + "): " + message, ex);

                    // reset the connection
                    this.resetConnection();

                    // resend
                    if (this.m_connection.isConnected() == true) {
                        this.errorLogger().info("sendMessage: retrying send() after EOF/reconnect....");
                        sent = this.sendMessage(topic,message,qos);
                    }
                    else {
                        // unable to send (not connected)
                        this.errorLogger().warning("sendMessage: NOT CONNECTED after EOF/reconnect. Unable to send message: " + message);
                    }
                }
            }
            catch (Exception ex) {
                // unable to send (general fault)
                this.errorLogger().critical("sendMessage: unable to send message: " + message, ex);
            }
        }
        else if (message != null) {
            // unable to send (not connected)
            this.errorLogger().warning("sendMessage: NOT CONNECTED. Unable to send message: " + message);
            
            // reset the connection
            this.resetConnection();

            // resend
            if (this.m_connection.isConnected() == true) {
                this.errorLogger().info("sendMessage: retrying send() after EOF/reconnect....");
                sent = this.sendMessage(topic,message,qos);
            }
            else {
                // unable to send (not connected)
                this.errorLogger().warning("sendMessage: NOT CONNECTED after EOF/reconnect. Unable to send message: " + message);
            }
        }
        else {
            // unable to send (empty message)
            this.errorLogger().warning("sendMessage: EMPTY MESSAGE. Not sent (OK)");
            sent = true;
        }
        
        // return the status
        return sent;
    }
    
    // get the next MQTT message
    private MQTTMessage getNextMessage() throws Exception {
        MQTTMessage message = null;
        message = new MQTTMessage(this.m_connection.receive());
        message.ack();
        return message;
    }

    /**
     * Receive and process a MQTT Message
     * @return
     */
    public MQTTMessage receiveAndProcessMessage() {
        MQTTMessage message = null;
        try {
            // DEBUG
            //this.errorLogger().info("receiveMessage: getting next MQTT message...");
            message = this.getNextMessage();
            if (this.m_listener != null && message != null) {
                // call the registered listener to process the received message
                this.errorLogger().info("receiveMessage: processing message: " + message);
                //this.errorLogger().info("receiveAndProcessMessage(MQTT Transport): Topic: " + message.getTopic() + " message: " + message.getMessage());
                this.m_listener.onMessageReceive(message.getTopic(),message.getMessage());
            }
            else if (this.m_listener != null) {
                // no listener
                this.errorLogger().critical("receiveMessage: Not processing message: " + message + ". Listener is NULL");
            }
            else {
                // no message
                this.errorLogger().critical("receiveMessage: Not processing NULL message");
            }
        }
        catch (Exception ex) {
            if (this.retriesExceeded()) {
                // unable to receiveMessage - final
                this.errorLogger().critical("receiveMessage: unable to receive message (final)", ex);
            }
            else {
                // unable to receiveMessage - final
                this.errorLogger().critical("receiveMessage: unable to receive message (" + this.m_num_retries + " of " + this.m_max_retries + "): " + ex.getMessage(), ex);

                // reset the connection
                this.resetConnection();

                // re-receive
                if (this.isConnected()) {
                    this.errorLogger().info("receiveMessage: retrying receive() after EOF/reconnect...");
                    this.receiveAndProcessMessage();
                }
            }
        }
        return message;
    }

    /**
     * Disconnect from MQTT broker
     */
    @Override
    public void disconnect() {
        this.disconnect(true);
    }
    
    // Disconnect from MQTT broker
    public void disconnect(boolean clear_creds) {
        // DEBUG
        this.errorLogger().info("MQTT: disconnecting from MQTT Broker.. ");
        
        // disconnect... 
        try {
            if (this.m_connection != null) {
                this.m_connection.disconnect();
            }
        }
        catch (Exception ex) {
            // unable to send
            this.errorLogger().warning("MQTT: exception during disconnect(). ", ex);
        }
        
        // DEBUG
        this.errorLogger().info("MQTT: disconnected from MQTT Broker. Cleaning up...");
        
        // clean up...
        super.disconnect();
        this.m_endpoint = null;
        this.m_connection = null;
        
        // clear the cached creds 
        if (clear_creds == true) {
            this.m_connect_host = null;
            this.m_connect_port = 0;
            this.m_connect_client_id = null;
            if (this.m_use_pki == true && this.m_keystore_filename != null) {
                Utils.deleteKeystore(this.errorLogger(),this.m_keystore_filename,this.m_keystore_basename);
            }
            this.m_keystore_filename = null;
        }
    }
    
    private boolean reconnect() {
        if (this.m_connect_host != null) {
            // attempt reconnect with cached creds...
            return this.connect(this.m_connect_host,this. m_connect_port, this.m_connect_client_id, this.m_connect_clean_session);
        }
        else {
            // no initial connect() has succeeded... so no cached creds available
            this.errorLogger().warning("reconnect: unable to reconnect() prior to initial connect() success...");
            return false;
        }
    }
    
    // force use of SSL
    public void forceSSLUsage(boolean forced_ssl) {
        this.m_forced_ssl = forced_ssl;
    }
    
    // setup the MQTT host URL
    private String setupHostURL(String host, int port) {
        if (this.m_host_url == null) {
            boolean secured = this.prefBoolValue("mqtt_use_ssl",this.m_suffix);
            
            // override
            if (this.m_forced_ssl == true) {
                // override
                this.errorLogger().info("MQTT: OVERRIDE use of SSL enabled");
                secured = this.m_forced_ssl;
            }
            
            // PREFIX determination
            String prefix = "tcp://";
            if (secured) {
                prefix = "ssl://";
                port += 7000;           // 1883 --> 8883
            }
            this.m_host_url = prefix + host + ":" + port;
        }
        return this.m_host_url;
    }
    
    // Internal MQTT Trust manager
    class MQTTTrustManager implements X509TrustManager { 
        private KeyStore m_keystore = null;
        private String  m_keystore_filename = null;
        private String  m_keystore_pw = null;
        
        // constructor
        public MQTTTrustManager(String keystore_filename,String pw) {
            super();
            this.m_keystore_filename = keystore_filename;
            this.m_keystore_pw = pw;
            this.initializeTrustManager();
        }
        
        // intialize the Trust Manager
        private void initializeTrustManager() {
            try {
                KeyStore myTrustStore;
                try (FileInputStream myKeys = new FileInputStream(this.m_keystore_filename)) {
                    myTrustStore = KeyStore.getInstance(KeyStore.getDefaultType());
                    myTrustStore.load(myKeys, this.m_keystore_pw.toCharArray());
                }
                TrustManagerFactory tmf = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
                tmf.init(myTrustStore);
            }
            catch (KeyStoreException | IOException | NoSuchAlgorithmException | CertificateException ex) {
                errorLogger().warning("MQTTTrustManager:initializeTrustManager: FAILED to initialize",ex);
            }
        }
        
        @Override
        public void checkClientTrusted(X509Certificate[] x509Certificates, String s) throws CertificateException {
        }

        @Override
        public void checkServerTrusted(X509Certificate[] x509Certificates, String s) throws CertificateException {
        }   
        
        @Override
        public X509Certificate[] getAcceptedIssuers() {
            return new X509Certificate[0];
        }
    }
}
