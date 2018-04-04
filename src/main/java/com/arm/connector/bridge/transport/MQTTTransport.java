/**
 * @file    MQTTTransport.java
 * @brief MQTT Transport Support
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
package com.arm.connector.bridge.transport;

import com.arm.connector.bridge.coordinator.processors.interfaces.GenericSender;
import com.arm.connector.bridge.core.Transport;
import com.arm.connector.bridge.core.ErrorLogger;
import com.arm.connector.bridge.core.Utils;
import com.arm.connector.bridge.preferences.PreferenceManager;
import java.io.FileInputStream;
import java.io.IOException;
import java.math.BigInteger;
import java.net.URISyntaxException;
import java.security.InvalidKeyException;
import java.security.KeyManagementException;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.security.KeyStore;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.NoSuchProviderException;
import java.security.PrivateKey;
import java.security.PublicKey;
import java.security.SecureRandom;
import java.security.SignatureException;
import java.security.UnrecoverableKeyException;
import java.security.cert.CertificateEncodingException;
import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;
import java.util.Date;
import javax.net.ssl.KeyManager;
import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManager;
import javax.net.ssl.TrustManagerFactory;
import javax.net.ssl.X509TrustManager;
import javax.security.auth.x500.X500Principal;
import org.bouncycastle.asn1.x509.ExtendedKeyUsage;
import org.bouncycastle.asn1.x509.KeyPurposeId;
import org.bouncycastle.asn1.x509.X509Extensions;
import org.bouncycastle.asn1.x509.X509Name;
import org.bouncycastle.jce.provider.BouncyCastleProvider;
import org.bouncycastle.x509.X509V3CertificateGenerator;
import org.fusesource.mqtt.client.BlockingConnection;
import org.fusesource.mqtt.client.MQTT;
import org.fusesource.mqtt.client.QoS;
import org.fusesource.mqtt.client.Topic;

/**
 * MQTT Transport Support
 *
 * @author Doug Anson
 */
public class MQTTTransport extends Transport implements GenericSender {
    // should never be used 
    private static final String KEYSTORE_PW_DEFAULT = "Ub12u&hF83hf&t121dfjKr0";
    private static volatile MQTTTransport m_self = null;
    private BlockingConnection m_connection = null;
    private byte[] m_qoses = null;
    private String m_suffix = null;
    private String m_username = null;
    private String m_password = null;
    private String m_host_url = null;
    private int m_sleep_time = 0;
    private String m_client_id = null;
    private String m_mqtt_version = null;

    private String m_connect_host = null;
    private int m_connect_port = 0;
    private String m_connect_client_id = null;
    private boolean m_connect_clean_session = false;

    private int m_num_retries = 0;
    private int m_max_retries = 10;
    private boolean m_set_mqtt_version = true;
    private Topic[] m_subscribe_topics = null;
    private String[] m_unsubscribe_topics = null;
    
    private boolean m_mqtt_import_keystore = false;
    private boolean m_mqtt_no_client_creds = false;
    
    // SSL Switches/Context
    private boolean m_use_ssl_connection = false;
    private SSLContext m_ssl_context = null;
    private boolean m_mqtt_use_ssl = false;
    private boolean m_ssl_context_initialized = false;
    private boolean m_no_tls_certs_or_keys = false;
    
    // X.509 Support
    private boolean m_use_x509_auth = false;
    private String m_pki_priv_key = null;
    private String m_pki_pub_key = null;
    private String m_pki_cert = null;
    private String m_keystore_pw = null;
    private String m_base_dir = null;
    private String m_keystore_filename = null;
    private String m_keystore_basename = null;
    private X509Certificate m_cert = null;
    private PublicKey m_pubkey = null;
    private PrivateKey m_privkey = null;
    private String m_pubkey_pem_filename = null;
    
    // Debug X.509 Auth Creds
    private boolean m_debug_creds = false;

    /**
     * Instance Factory
     *
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
     *
     * @param error_logger
     * @param preference_manager
     * @param suffix
     */
    public MQTTTransport(ErrorLogger error_logger, PreferenceManager preference_manager, String suffix) {
        super(error_logger, preference_manager);
        this.m_use_x509_auth = false;
        this.m_use_ssl_connection = false;
        this.m_ssl_context = null;
        this.m_host_url = null;
        this.m_suffix = suffix;
        this.m_keystore_filename = null;
        this.m_set_mqtt_version = true;
        
        this.m_mqtt_use_ssl = this.prefBoolValue("mqtt_use_ssl", this.m_suffix);
        this.m_debug_creds = this.prefBoolValue("mqtt_debug_creds", this.m_suffix);
        this.m_mqtt_import_keystore = this.prefBoolValue("mqtt_import_keystore", this.m_suffix);
        this.m_mqtt_no_client_creds = this.prefBoolValue("mqtt_no_client_creds", this.m_suffix);
        this.setMQTTVersion(this.prefValue("mqtt_version", this.m_suffix));
        this.setUsername(this.prefValue("mqtt_username", this.m_suffix));
        this.setPassword(this.prefValue("mqtt_password", this.m_suffix));
        this.m_max_retries = this.preferences().intValueOf("mqtt_connect_retries", this.m_suffix);
        this.m_sleep_time = ((this.preferences().intValueOf("mqtt_receive_loop_sleep", this.m_suffix)) * 1000);
        this.m_keystore_pw = this.preferences().valueOf("mqtt_keystore_pw", this.m_suffix);
        this.m_base_dir = this.preferences().valueOf("mqtt_keystore_basedir", this.m_suffix);
        this.m_keystore_basename = this.preferences().valueOf("mqtt_keystore_basename", this.m_suffix);
        this.m_pubkey_pem_filename = this.preferences().valueOf("mqtt_pubkey_pem_filename", this.m_suffix);
        
        // default pubkey filename if utilized
        if (this.m_pubkey_pem_filename == null || this.m_pubkey_pem_filename.length() == 0) {
            this.m_pubkey_pem_filename = Utils.DEFAULT_PUBKEY_PEM_FILENAME;
        }
        
        // sync our acceptance of self-signed client creds
        this.noSelfSignedCertsOrKeys(this.m_mqtt_no_client_creds);
        
        // should never be used
        if (this.m_keystore_pw == null) {
            // default the keystore pw
            this.m_keystore_pw = MQTTTransport.KEYSTORE_PW_DEFAULT;
        }
    }

    /**
     * Constructor
     *
     * @param error_logger
     * @param preference_manager
     */
    public MQTTTransport(ErrorLogger error_logger, PreferenceManager preference_manager) {
        super(error_logger, preference_manager);
        this.m_use_x509_auth = false;
        this.m_use_ssl_connection = false;
        this.m_ssl_context = null;
        this.m_host_url = null;
        this.m_suffix = null;
        this.m_keystore_filename = null;
        this.m_set_mqtt_version = true;

        this.m_mqtt_use_ssl = this.prefBoolValue("mqtt_use_ssl", this.m_suffix);
        this.m_debug_creds = this.prefBoolValue("mqtt_debug_creds", this.m_suffix);
        this.m_mqtt_import_keystore = this.prefBoolValue("mqtt_import_keystore", this.m_suffix);
        this.m_mqtt_no_client_creds = this.prefBoolValue("mqtt_no_client_creds", this.m_suffix);
        this.setMQTTVersion(this.prefValue("mqtt_version", this.m_suffix));
        this.setUsername(this.prefValue("mqtt_username", this.m_suffix));
        this.setPassword(this.prefValue("mqtt_password", this.m_suffix));
        this.m_max_retries = this.preferences().intValueOf("mqtt_connect_retries", this.m_suffix);
        this.m_sleep_time = ((this.preferences().intValueOf("mqtt_receive_loop_sleep", this.m_suffix)) * 1000);
        this.m_keystore_pw = this.preferences().valueOf("mqtt_keystore_pw", this.m_suffix);
        this.m_base_dir = this.preferences().valueOf("mqtt_keystore_basedir", this.m_suffix);
        this.m_keystore_basename = this.preferences().valueOf("mqtt_keystore_basename", this.m_suffix);
        this.m_pubkey_pem_filename = this.preferences().valueOf("mqtt_pubkey_pem_filename", this.m_suffix);
        
        // default pubkey filename if utilized
        if (this.m_pubkey_pem_filename == null || this.m_pubkey_pem_filename.length() == 0) {
            this.m_pubkey_pem_filename = Utils.DEFAULT_PUBKEY_PEM_FILENAME;
        }
        
        // sync our acceptance of self-signed client creds
        this.noSelfSignedCertsOrKeys(this.m_mqtt_no_client_creds);
        
        // should never be used
        if (this.m_keystore_pw == null) {
            // default the keystore pw
            this.m_keystore_pw = MQTTTransport.KEYSTORE_PW_DEFAULT;
        }
    }

    // disable/enable setting of MQTT version
    public void enableMQTTVersionSet(boolean set_mqtt_version) {
        this.m_set_mqtt_version = set_mqtt_version;
    }
    
    // if SSL is used, dont provide any certs/keys 
    public void noSelfSignedCertsOrKeys(boolean no_tls_certs_or_keys) {
        this.m_no_tls_certs_or_keys = no_tls_certs_or_keys;
    }

    // pre-plumb TLS/X.509 certs and keys
    public void prePlumbTLSCertsAndKeys(String priv_key, String pub_key, String certificate, String id) {
        this.m_pki_priv_key = priv_key;
        this.m_pki_pub_key = pub_key;
        this.m_pki_cert = certificate;
        if (this.initializeSSLContext(id) == true) {
            this.m_use_x509_auth = true;
            this.m_use_ssl_connection = true;
        }
        else {
            // unable to initialize the SSL context... so error out
            this.errorLogger().critical("prePlumbTLSCerts: Unable to initialize SSL Context for ID: " + id);
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
            KeyManagerFactory kmf = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
            KeyStore ks = KeyStore.getInstance(keyStoreType);
            fs = new FileInputStream(this.m_keystore_filename);
            ks.load(fs, this.m_keystore_pw.toCharArray());
            kmf.init(ks, this.m_keystore_pw.toCharArray());
            kms = kmf.getKeyManagers();
        }
        catch (NoSuchAlgorithmException | KeyStoreException | IOException | CertificateException | UnrecoverableKeyException ex) {
            this.errorLogger().warning("createKeyManager: Exception in creating the KeyManager list", ex);
        }

        try {
            if (fs != null) {
                fs.close();
            }
        }
        catch (IOException ex) {
            // silent
        }

        return kms;
    }

    // create the trust manager
    private TrustManager[] createTrustManager() {
        TrustManager tm[] = new TrustManager[1];
        tm[0] = new MQTTTrustManager(this.m_keystore_filename, this.m_keystore_pw);
        return tm;
    }
    
    // Init a self-signed certificate
    private String initSelfSignedCert(KeyPair keys) {
        String cert = null;
        
        try {
            // build a certificate generator
            X509V3CertificateGenerator certGen = new X509V3CertificateGenerator();
            X500Principal dnName = new X500Principal("cn=127.0.0.1");

            // add some options
            certGen.setSerialNumber(BigInteger.valueOf(System.currentTimeMillis()));
            certGen.setSubjectDN(new X509Name("dc=ARM"));
            certGen.setIssuerDN(dnName); // use the same
            
            // yesterday
            certGen.setNotBefore(new Date(System.currentTimeMillis() - 24 * 60 * 60 * 1000));
            
            // in 2 years
            certGen.setNotAfter(new Date(System.currentTimeMillis() + 2 * 365 * 24 * 60 * 60 * 1000));
            certGen.setPublicKey(keys.getPublic());
            certGen.setSignatureAlgorithm("SHA256WithRSAEncryption");
            certGen.addExtension(X509Extensions.ExtendedKeyUsage, true, new ExtendedKeyUsage(KeyPurposeId.id_kp_timeStamping));

            // finally, sign the certificate with the private key of the same KeyPair
            X509Certificate x509 = certGen.generate(keys.getPrivate(),"BC");
            
            // encode the certificate into a PEM string
            cert = Utils.convertX509ToPem(x509);
        }
        catch (IllegalArgumentException | IllegalStateException | InvalidKeyException | NoSuchAlgorithmException | NoSuchProviderException | SignatureException | CertificateEncodingException ex) {
            // unable to init self-signed cert
            this.errorLogger().critical("MQTT: initSelfSignedCert: Exception caught: " + ex.getMessage());
        }
        return cert;
    }
    
    // create our own key material if we dont have it already
    private void initKeyMaterial() {
        try {
            KeyPairGenerator keyGen = KeyPairGenerator.getInstance("RSA");
            keyGen.initialize(1024);
            KeyPair keys= keyGen.generateKeyPair();
            
            // Create the key material
            this.m_pki_priv_key = Utils.convertPrivKeyToPem(keys);
            this.m_pki_pub_key = Utils.convertPubKeyToPem(keys);

            // create the certificate
            this.m_pki_cert = this.initSelfSignedCert(keys);
        }
        catch (NoSuchAlgorithmException ex) {
            // unable to init key material
            this.errorLogger().critical("MQTT: initKeyMaterial: Exception caught: " + ex.getMessage());
        }
    }
    
    // reuse an exsiting keystore 
    private String useExistingKeyStore(String id,String pw) {
        // create our filename from the ID 
        String filename =  Utils.makeKeystoreFilename(this.m_base_dir, id, this.m_keystore_basename);
        String pubkey_pem = Utils.makePubkeyFilename(this.m_base_dir, id, this.m_pubkey_pem_filename);
        
        // now read from the Keystore
        if (this.m_pki_cert == null || this.m_pki_priv_key == null || this.m_pki_pub_key == null) {
            this.m_pki_cert = Utils.readCertFromKeystoreAsPEM(this.errorLogger(),filename,pw);
            this.m_pki_priv_key = Utils.readPrivKeyFromKeystoreAsPEM(this.errorLogger(),filename,pw);
            this.m_pki_pub_key = Utils.readPubKeyAsPEM(this.errorLogger(),pubkey_pem);
        }
        
        // display creds if debugging
        if (this.m_debug_creds == true) {
            // Creds DEBUG
            this.errorLogger().info("MQTT: PRIV: " + this.m_pki_priv_key);
            this.errorLogger().info("MQTT: PUB: " + this.m_pki_pub_key);
            this.errorLogger().info("MQTT: CERT: " + this.m_pki_cert);
        }
        
        // return the filename
        return filename;
    }

    // create keystore
    private String initializeKeyStore(String id) {
        // create self-signed creds if we dont have them but need SSL
        if (this.m_pki_cert == null || this.m_pki_priv_key == null || this.m_pki_pub_key == null) {
            this.initKeyMaterial();
        }
        
        // display creds if debugging
        if (this.m_debug_creds == true) {
            // Creds DEBUG
            this.errorLogger().info("MQTT: PRIV: " + this.m_pki_priv_key);
            this.errorLogger().info("MQTT: PUB: " + this.m_pki_pub_key);
            this.errorLogger().info("MQTT: CERT: " + this.m_pki_cert);
        }
            
        // create our credentials
        this.m_cert = Utils.createX509CertificateFromPEM(this.errorLogger(), this.m_pki_cert, "X509");
        this.m_privkey = Utils.createPrivateKeyFromPEM(this.errorLogger(), this.m_pki_priv_key, "RSA");

        // also hold onto the public key
        this.m_pubkey = Utils.createPublicKeyFromPEM(this.errorLogger(), this.m_pki_pub_key, "RSA");

        // set our keystore PW
        this.m_keystore_pw = Utils.generateKeystorePassword(this.m_keystore_pw, id);

        // create the keystore
        return Utils.createKeystore(this.errorLogger(), this.m_base_dir, id, this.m_keystore_basename, this.m_cert, this.m_privkey, this.m_pubkey, this.m_keystore_pw);
    }

    // initialize the SSL context
    private boolean initializeSSLContext(String id) {
        if (this.m_ssl_context_initialized == false) {
            try {
                // enable the Bouncy Castle SSL provider
                java.security.Security.addProvider(new BouncyCastleProvider());

                // do we want self-signed certs and keys?
                if (this.m_no_tls_certs_or_keys == true) {
                    // just create our SSL context... use defaults for everything
                    this.m_ssl_context = SSLContext.getInstance("TLSv1.2");
                    this.m_ssl_context.init(null, null, new SecureRandom());
                    this.m_ssl_context_initialized = true;
                }
                else {
                    if (this.m_mqtt_import_keystore == false) {
                        // initialize the keystores with certs/key...
                        this.m_keystore_filename = this.initializeKeyStore(id);
                    }
                    else {
                        // use existing keystore 
                        this.m_keystore_filename = this.useExistingKeyStore(id,this.m_keystore_pw);
                    }
                    
                    if (this.m_keystore_filename != null) {
                        // create our SSL context - FYI: AWS IoT requires TLS v1.2
                        this.m_ssl_context = SSLContext.getInstance("TLSv1.2");

                        // initialize the SSL context with our KeyManager and our TrustManager
                        KeyManager km[] = this.createKeyManager("JKS");
                        TrustManager tm[] = this.createTrustManager();
                        this.m_ssl_context.init(km, tm, new SecureRandom());
                        this.m_ssl_context_initialized = true;
                    }
                    else {
                        // unable to create the filename
                        this.errorLogger().critical("MQTTTransport: initializeSSLContext(SSL) failed. unable to init keystore");
                    }
                }
            }
            catch (NoSuchAlgorithmException | KeyManagementException ex) {
                // exception caught
                this.errorLogger().critical("MQTTTransport: initializeSSLContext(SSL) failed. SSL DISABLED", ex);
            }
        }
        return this.m_ssl_context_initialized;
    }

    // PUBLIC: Create the authentication hash
    public String createAuthenticationHash() {
        return Utils.createHash(this.getUsername() + "_" + this.getPassword() + "_" + this.prefValue("mqtt_client_id", this.m_suffix));
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
     *
     * @param username
     */
    public final void setUsername(String username) {
        this.m_username = username;
    }

    /**
     * Set the MQTT Password
     *
     * @param password
     */
    public final void setPassword(String password) {
        this.m_password = password;
    }
    
    /**
     * Set the MQTT Version
     *
     * @param version
     */
    public final void setMQTTVersion(String version) {
        this.m_mqtt_version = version;
    }

    /**
     * Are we connected to a MQTT broker?
     *
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
     *
     * @param host
     * @param port
     * @return
     */
    @Override
    public boolean connect(String host, int port) {
        return this.connect(host, port, this.prefValue("mqtt_client_id", this.m_suffix), this.prefBoolValue("mqtt_clean_session", this.m_suffix));
    }

    /**
     * Connect to the MQTT broker
     *
     * @param host
     * @param port
     * @param clientID
     * @return
     */
    public boolean connect(String host, int port, String clientID) {
        return this.connect(host, port, clientID, this.prefBoolValue("mqtt_clean_session", this.m_suffix));
    }

     /**
     * Connect to the MQTT broker
     *
     * @param host
     * @param port
     * @param clientID
     * @param clean_session
     * @return
     */
    public boolean connect(String host, int port, String clientID, boolean clean_session) {
        return this.connect(host,port,clientID,clean_session,null);
    }
    
    /**
     * Connect to the MQTT broker
     *
     * @param host
     * @param port
     * @param clientID
     * @param clean_session
     * @param id 
     * @return
     */
    public boolean connect(String host, int port, String clientID, boolean clean_session, String id) {
        int sleep_time = this.prefIntValue("mqtt_retry_sleep", this.m_suffix);
        int num_tries = this.prefIntValue("mqtt_connect_retries", this.m_suffix);
        
        // build out the URL connection string
        String url = this.setupHostURL(host, port, id);

        // setup default clientID
        if (clientID == null || clientID.length() <= 0) {
            clientID = this.prefValue("mqtt_client_id", this.m_suffix);
        }
        String def_client_id = this.prefValue("mqtt_default_client_id", this.m_suffix);

        // DEBUG
        this.errorLogger().info("MQTTTransport: Connection URL: [" + url + "]");
        
        // loop until connected
        for (int i = 0; i < num_tries && !this.m_connected; ++i) {
            try {
                // MQTT endpoint 
                MQTT endpoint = new MQTT();
                if (endpoint != null) {
                    // set the target URL for our MQTT connection
                    endpoint.setHost(url);

                    // MQTT version
                    if (this.m_mqtt_version != null && this.m_set_mqtt_version == true) {
                        endpoint.setVersion(this.m_mqtt_version);
                    }

                    // SSL Context for secured MQTT connections
                    if (this.m_ssl_context_initialized == true) {
                        // DEBUG
                        this.errorLogger().info("MQTTTransport: SSL Used... setting SSL context...");

                        // SSL Context should be set
                        endpoint.setSslContext(this.m_ssl_context);
                    }

                    // configure credentials
                    String username = this.getUsername();
                    String pw = this.getPassword();

                    if (username != null && username.length() > 0 && username.equalsIgnoreCase("off") == false) {
                        endpoint.setUserName(username);
                        this.errorLogger().info("MQTTTransport: Username: [" + username + "] used");
                    }
                    else {
                        this.errorLogger().info("MQTTTransport: Anonymous username used");
                    }
                    if (pw != null && pw.length() > 0 && pw.equalsIgnoreCase("off") == false) {
                        endpoint.setPassword(pw);
                        this.errorLogger().info("MQTTTransport: pw: [" + pw + "] used");
                    }
                    else {
                        this.errorLogger().info("MQTTTransport: Anonymous pw used");
                    }

                    // Client ID 
                    if (clientID != null && clientID.length() > 0 && clientID.equalsIgnoreCase("off") == false) {
                        endpoint.setClientId(clientID);
                        this.errorLogger().info("MQTTTransport: Client ID: [" + clientID + "] used");
                    }
                    else if (clean_session == false) {
                        if (def_client_id != null && def_client_id.equalsIgnoreCase("off") == false) {
                            // set a defaulted clientID
                            endpoint.setClientId(def_client_id);
                            this.errorLogger().info("MQTTTransport: Client ID (default for clean): [" + def_client_id + "] used");
                        }
                        else {
                            // non-clean session specified, but no clientID was given...
                            this.errorLogger().warning("MQTTTransport: ERROR: Non-clean session requested but no ClientID specified");
                        }
                    }
                    else {
                        // no client ID used... clean session specified (OK)
                        this.errorLogger().info("MQTTTransport: No ClientID being used (clean session)");
                    }

                    // set Clean Session...
                    endpoint.setCleanSession(clean_session);

                    // Will Message...
                    String will = this.prefValue("mqtt_will_message", this.m_suffix);
                    if (will != null && will.length() > 0 && will.equalsIgnoreCase("off") == false) {
                        endpoint.setWillMessage(will);
                    }

                    // Will Topic...
                    String will_topic = this.prefValue("mqtt_will_topic", this.m_suffix);
                    if (will_topic != null && will_topic.length() > 0 && will_topic.equalsIgnoreCase("off") == false) {
                        endpoint.setWillTopic(will_topic);
                    }

                    // Traffic Class...
                    int trafficClass = this.prefIntValue("mqtt_traffic_class", this.m_suffix);
                    if (trafficClass >= 0) {
                        endpoint.setTrafficClass(trafficClass);
                    }

                    // Reconnect Attempts...
                    int reconnectAttempts = this.prefIntValue("mqtt_reconnect_retries_max", this.m_suffix);
                    if (reconnectAttempts >= 0) {
                        endpoint.setReconnectAttemptsMax(reconnectAttempts);
                    }

                    // Reconnect Delay...
                    long reconnectDelay = (long) this.prefIntValue("mqtt_reconnect_delay", this.m_suffix);
                    if (reconnectDelay >= 0) {
                        endpoint.setReconnectDelay(reconnectDelay);
                    }

                    // Reconnect Max Delay...
                    long reconnectDelayMax = (long) this.prefIntValue("mqtt_reconnect_delay_max", this.m_suffix);
                    if (reconnectDelayMax >= 0) {
                        endpoint.setReconnectDelayMax(reconnectDelayMax);
                    }

                    // Reconnect back-off multiplier
                    float backoffMultiplier = this.prefFloatValue("mqtt_backoff_multiplier", this.m_suffix);
                    if (backoffMultiplier >= 0) {
                        endpoint.setReconnectBackOffMultiplier(backoffMultiplier);
                    }

                    // Keep-Alive...
                    short keepAlive = (short) this.prefIntValue("mqtt_keep_alive", this.m_suffix);
                    if (keepAlive >= 0) {
                        endpoint.setKeepAlive(keepAlive);
                    }

                    // record the ClientID for later...
                    if (endpoint.getClientId() != null) {
                        this.m_client_id = endpoint.getClientId().toString();
                    }

                    // OK... now lets try to connect to the broker...
                    try {
                        // connecting...
                        this.m_endpoint = endpoint;
                        this.m_connection = endpoint.blockingConnection();
                        if (this.m_connection != null) {
                            // attempt connection (if we have a failure, this.m_connection will go NULL)
                            this.m_connection.connect();

                            // sleep for a short bit...
                            try {
                                Thread.sleep(sleep_time);
                            }
                            catch (InterruptedException ex) {
                                this.errorLogger().critical("MQTTTransport(connect): sleep interrupted", ex);
                            }

                            // check our connection status
                            this.m_connected = false;
                            if (this.m_connection != null) {
                                this.m_connected = this.m_connection.isConnected();
                            }
                            
                            // DEBUG
                            if (this.m_connected == true) {
                                this.errorLogger().info("MQTTTransport: Connection to: " + url + " successful");
                                this.m_connect_host = host;
                                this.m_connect_port = port;
                                if (endpoint != null && endpoint.getClientId() != null) {
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
                        this.errorLogger().warning("MQTTTransport: Exception during connect()", ex);

                        // DEBUG
                        this.errorLogger().warning("MQTT: URL: " + url);
                        this.errorLogger().warning("MQTT: clientID: " + this.m_client_id);
                        this.errorLogger().warning("MQTT: clean_session: " + clean_session);
                        if (this.m_debug_creds == true) {
                            this.errorLogger().warning("MQTT: username: " + this.getUsername());
                            this.errorLogger().warning("MQTT: password: " + this.getPassword());

                            if (this.m_pki_priv_key != null) {
                                this.errorLogger().info("MQTT: PRIV: " + this.m_pki_priv_key);
                            }

                            if (this.m_pki_pub_key != null) {
                                this.errorLogger().info("MQTT: PUB: " + this.m_pki_pub_key);
                            }
                            if (this.m_pki_cert != null) {
                                this.errorLogger().info("MQTT: CERT: " + this.m_pki_cert);
                            }
                        }

                        // sleep for a short bit...
                        try {
                            Thread.sleep(sleep_time);
                        }
                        catch (InterruptedException ex2) {
                            this.errorLogger().critical("MQTTTransport(connect): sleep interrupted", ex2);
                        }
                    }
                }
                else {
                    // cannot create an instance of the MQTT client
                    this.errorLogger().warning("MQTTTransport(connect): unable to create instance of MQTT client");
                    this.m_connected = false;
                }
            }
            catch (URISyntaxException ex) {
                this.errorLogger().critical("MQTTTransport(connect): URI Syntax exception occured", ex);
                this.m_connected = false;
            }

            // if we have not yet connected... sleep a bit more and retry...
            if (this.m_connected == false) {
                try {
                    Thread.sleep(sleep_time);
                }
                catch (InterruptedException ex) {
                    this.errorLogger().critical("MQTTTransport(retry): sleep interrupted", ex);
                }
            }
        }

        // return our connection status
        return this.m_connected;
    }

    /**
     * Main handler for receiving and processing MQTT Messages (called repeatedly by TransportReceiveThread...)
     *
     * @return true - processed (or empty), false - failure
     */
    @Override
    public boolean receiveAndProcess() {
        // DEBUG
        //this.errorLogger().info("MQTTTransport: in receiveAndProcess()...");
        if (this.isConnected()) {
            try {
                // receive the MQTT message and process it...
                //this.errorLogger().info("MQTTTransport: in receiveAndProcess(). Calling receiveAndProcessMessage()...");
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
        // disconnect()...
        ++this.m_num_retries;
        this.disconnect(false);
        
        // sleep a bit...
        try {
            // sleep a bit
            Thread.sleep(this.m_sleep_time);
        }
        catch(InterruptedException ex2) {
            // silent
        }
        
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
            this.errorLogger().info("MQTTTransport: unable to subscribe. Connection is missing and/or NULL");
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
                    this.errorLogger().info("MQTTTransport: unable to unsubscribe from topic (final)", ex);
                }
                else {
                    // unable to subscribe to topic
                    this.errorLogger().info("MQTTTransport: unable to unsubscribe to topic (" + this.m_num_retries + " of " + this.m_max_retries + ")", ex);

                    // attempt reset
                    this.resetConnection();

                    // recall
                    this.unsubscribe(list);
                }
            }
        }
        else {
            // unable to subscribe - not connected... 
            this.errorLogger().info("MQTTTransport: unable to unsubscribe. Connection is missing and/or NULL");
        }
    }

    /**
     * Publish a MQTT message
     *
     * @param topic
     * @param message
     */
    @Override
    public void sendMessage(String topic, String message) {
        this.sendMessage(topic, message, QoS.AT_LEAST_ONCE);
    }

    /**
     * Publish a MQTT message
     *
     * @param topic
     * @param message
     * @param qos
     * @return send status
     */
    public boolean sendMessage(String topic, String message, QoS qos) {
        boolean sent = false;
        if (this.m_connection != null && this.m_connection.isConnected() == true && message != null) {
            try {
                // DEBUG
                this.errorLogger().info("sendMessage: message: " + message + " Topic: " + topic);
                this.m_connection.publish(topic, message.getBytes(), qos, false);

                // DEBUG
                this.errorLogger().info("sendMessage(MQTT): message sent. SUCCESS");
                sent = true;
            }
            catch (Exception ex) {
                if (this.retriesExceeded()) {
                    // unable to send (EOF) - final
                    this.errorLogger().critical("sendMessage:Exception in sendMessage... resetting MQTT (final): " + message, ex);
                    
                    // disconnect
                    this.disconnect(false);
                }
                else {
                    // unable to send (EOF) - final
                    this.errorLogger().warning("sendMessage:Exception in sendMessage... resetting MQTT (" + this.m_num_retries + " of " + this.m_max_retries + "): " + message, ex);

                    // reset the connection
                    this.resetConnection();

                    // resend
                    if (this.m_connection.isConnected() == true) {
                        this.errorLogger().info("sendMessage: retrying send() afterreconnect....");
                        sent = this.sendMessage(topic, message, qos);
                    }
                    else {
                        // unable to send (not connected)
                        this.errorLogger().warning("sendMessage: NOT CONNECTED after reconnect. Unable to send message: " + message);
                    }
                }
            }
        }
        else if (this.m_connection != null && message != null) {
            // unable to send (not connected)
            this.errorLogger().warning("sendMessage: NOT CONNECTED. Unable to send message: " + message);

            // reset the connection
            this.resetConnection();

            // resend
            if (this.m_connection != null && this.m_connection.isConnected() == true) {
                this.errorLogger().info("sendMessage: retrying send() after EOF/reconnect....");
                sent = this.sendMessage(topic, message, qos);
            }
            else {
                // unable to send (not connected)
                this.errorLogger().warning("sendMessage: NOT CONNECTED after EOF/reconnect. Unable to send message: " + message);
            }
        }
        else if (message != null) {
            // unable to send (not connected)
            this.errorLogger().warning("sendMessage: NOT CONNECTED (no handle). Unable to send message: " + message);

            // reset the connection
            this.resetConnection();

            // resend
            if (this.m_connection != null && this.m_connection.isConnected() == true) {
                this.errorLogger().info("sendMessage: retrying send() after EOF/reconnect....");
                sent = this.sendMessage(topic, message, qos);
            }
            else {
                // unable to send (not connected)
                this.errorLogger().warning("sendMessage: NOT CONNECTED (no handle) after EOF/reconnect. Unable to send message: " + message);
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
    private MQTTMessage getNextMessage() {
        try {
            if (this.m_connection != null) {
                MQTTMessage message = new MQTTMessage(this.m_connection.receive());
                if (message != null) {
                    message.ack();
                }
                return message;
            }
        }
        catch (Exception ex) {
            // getNextMessage failed
            this.errorLogger().warning("MQTT: getNextMessage failed with exception: " + ex.getMessage());
        }
        return null;
    }

    /**
     * Receive and process a MQTT Message
     *
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
                this.m_listener.onMessageReceive(message.getTopic(), message.getMessage());
            }
            else if (this.m_listener != null) {
                // no listener
                this.errorLogger().warning("receiveMessage: Not processing message: " + message + ". Listener is NULL");
            }
            else {
                // no message - just skip
                this.errorLogger().info("receiveMessage: Not processing NULL message");
            }
        }
        catch (Exception ex) {
            if (this.retriesExceeded()) {
                // unable to receiveMessage - final
                this.errorLogger().warning("receiveMessage: unable to receive message (final)", ex);
            }
            else {
                // unable to receiveMessage - final
                this.errorLogger().warning("receiveMessage: unable to receive message (" + this.m_num_retries + " of " + this.m_max_retries + "): " + ex.getMessage(), ex);

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
        this.errorLogger().info("MQTT: disconnected. Cleaning up...");

        // clean up...
        super.disconnect();
        this.m_endpoint = null;
        this.m_connection = null;

        // clear the cached creds 
        if (clear_creds == true) {
            this.m_connect_host = null;
            this.m_connect_port = 0;
            this.m_connect_client_id = null;
            if (this.m_use_x509_auth == true && this.m_keystore_filename != null) {
                Utils.deleteKeystore(this.errorLogger(), this.m_keystore_filename, this.m_keystore_basename);
            }
            this.m_keystore_filename = null;
        }
    }

    private boolean reconnect() {
        if (this.m_connect_host != null) {
            // attempt reconnect with cached creds...
            return this.connect(this.m_connect_host, this.m_connect_port, this.m_connect_client_id, this.m_connect_clean_session);
        }
        else {
            // no initial connect() has succeeded... so no cached creds available
            this.errorLogger().info("reconnect: unable to reconnect() prior to initial connect() success...");
            return false;
        }
    }

    // force use of SSL
    public void useSSLConnection(boolean use_ssl_connection) {
        this.m_use_ssl_connection = use_ssl_connection;
    }
    
    // setup the MQTT host URL
    private String setupHostURL(String host, int port,String id) {
        if (this.m_host_url == null) {
            // SSL override check...
            if (this.m_use_ssl_connection == true && this.m_mqtt_use_ssl == false) {
                // force use of SSL
                this.errorLogger().info("MQTT: OVERRIDE use of SSL enabled");
                this.m_mqtt_use_ssl = true;
            }

            // MQTT prefix determination (non-SSL)
            String prefix = "tcp://";
            
            // adjust for SSL usage when needed
            if (this.m_mqtt_use_ssl == true) {
                // initialize our SSL context
                port += 7000;           // take mqtt_port 1883 and add 7000 --> 8883
                prefix = "ssl://";      // SSL used... 
                
                // by default, we use a fixed ID for the ssl context... this may be overriden
                if (id == null) {
                    id = host.replace(".","_") + "_" + port;
                }
                
                // initialize the SSl context if not already done...
                if (this.initializeSSLContext(id) == false) {
                    // unable to initialize the SSL context
                    this.errorLogger().warning("MQTT: Unable to initialize SSL context ID: " + id);
                }
            }
            
            // complete the URL construction
            this.m_host_url = prefix + host + ":" + port;
        }
        return this.m_host_url;
    }

    // Internal MQTT Trust manager
    class MQTTTrustManager implements X509TrustManager {

        private KeyStore m_keystore = null;
        private String m_keystore_filename = null;
        private String m_keystore_pw = null;

        // constructor
        public MQTTTrustManager(String keystore_filename, String pw) {
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
                errorLogger().warning("MQTTTrustManager:initializeTrustManager: FAILED to initialize", ex);
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
