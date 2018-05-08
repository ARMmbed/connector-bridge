/**
 * @file    MDSProcessor.java
 * @brief mDS Peer Processor for the connector bridge
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
import com.arm.connector.bridge.servlet.Manager;
import com.arm.connector.bridge.coordinator.processors.core.Processor;
import com.arm.connector.bridge.coordinator.processors.interfaces.AsyncResponseProcessor;
import com.arm.connector.bridge.core.Utils;
import com.arm.connector.bridge.transport.HttpTransport;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import com.arm.connector.bridge.coordinator.processors.interfaces.mbedDeviceServerInterface;
import com.mbed.lwm2m.LWM2MResource;

/**
 * mDS/mDC Peer processor for the connector bridge
 *
 * @author Doug Anson
 */
public class mbedDeviceServerProcessor extends Processor implements mbedDeviceServerInterface, AsyncResponseProcessor {
    private HttpTransport m_http = null;
    private String m_mds_host = null;
    private int m_mds_port = 0;
    private String m_mds_username = null;
    private String m_mds_password = null;
    private String m_content_type = null;
    private String m_api_token = null;
    private boolean m_use_api_token = false;
    private String m_mds_gw_callback = null;
    private String m_default_mds_uri = null;
    private String m_default_gw_uri = null;
    private boolean m_use_https_dispatch = false;
    private String m_mds_version = null;
    private boolean m_mds_gw_use_ssl = false;
    private boolean m_mds_use_ssl = false;
    private boolean m_using_callback_webhooks = false;
    private boolean m_disable_sync = false;
    private boolean m_skip_validation = false;

    // device metadata resource URI from configuration
    private String m_device_manufacturer_res = null;
    private String m_device_serial_number_res = null;
    private String m_device_model_res = null;
    private String m_device_class_res = null;
    private String m_device_description_res = null;
    private String m_device_firmware_info_res = null;
    private String m_device_hardware_info_res = null;
    private String m_device_descriptive_location_res = null;
    private int m_webhook_validator_poll_ms = -1;
    private WebhookValidator m_webhook_validator = null;
    private boolean m_webhook_validator_enable = false;

    private String m_device_attributes_path = null;
    private String m_device_attributes_content_type = null;

    private String m_default_rest_version = "2";
    private boolean m_use_rest_versions = false;
    private String m_rest_version = null;

    // Long Poll vs Webhook usage
    private boolean m_mds_enable_long_poll = false;
    private String m_mds_long_poll_uri = null;
    private String m_mds_long_poll_url = null;
    private LongPollProcessor m_long_poll_processor = null;
    
    // Config: remove a device if it deregisters (default FALSE)
    private boolean m_mds_remove_on_deregistration = false;
    
    // Integrating with mbed Cloud? (default FALSE)
    private boolean m_mbed_cloud_integration = false;

    // constructor
    @SuppressWarnings("empty-statement")
    public mbedDeviceServerProcessor(Orchestrator orchestrator, HttpTransport http) {
        super(orchestrator, null);
        this.m_http = http;
        this.m_mds_domain = orchestrator.getDomain();
        this.m_mds_host = orchestrator.preferences().valueOf("mds_address");
        this.m_mds_port = orchestrator.preferences().intValueOf("mds_port");
        this.m_mds_username = orchestrator.preferences().valueOf("mds_username");
        this.m_mds_password = orchestrator.preferences().valueOf("mds_password");
        this.m_content_type = orchestrator.preferences().valueOf("mds_content_type");
        this.m_mds_gw_callback = orchestrator.preferences().valueOf("mds_gw_callback");
        this.m_use_https_dispatch = this.prefBoolValue("mds_use_https_dispatch");
        this.m_mds_version = this.prefValue("mds_version");
        this.m_mds_gw_use_ssl = this.prefBoolValue("mds_gw_use_ssl");
        this.m_use_api_token = this.prefBoolValue("mds_use_api_token");
        this.m_use_rest_versions = this.prefBoolValue("mds_enable_rest_versions");
        this.m_mds_enable_long_poll = this.prefBoolValue("mds_enable_long_poll");
        this.m_mds_long_poll_uri = this.prefValue("mds_long_poll_uri");
        this.m_rest_version = this.prefValueWithDefault("mds_rest_version", this.m_default_rest_version).replace("v", "").replace("//", "");
        if (this.m_use_api_token == true) {
            this.m_api_token = this.orchestrator().preferences().valueOf("mds_api_token");
        }
        
        // adjust mds_username
        try {
            Double ver = Double.valueOf(this.m_mds_version);
            if (ver > 3.0) {
                // v3.0+ on-prem mDS uses "Basic domain/user:pw"
                String domain = this.getDomain(true).replace("/", "");
                this.m_mds_username = domain + "/" + this.m_mds_username;

                // DEBUG
                this.errorLogger().info("mbedDeviceServerProcessor Updated username: " + this.m_mds_username);
            }
        }
        catch (NumberFormatException ex) {
            // parsing error... fail silently...
            ;
        }

        // get the device attributes path
        this.m_device_attributes_path = orchestrator.preferences().valueOf("mds_device_attributes_path");

        // get the device attributes content type
        this.m_device_attributes_content_type = orchestrator.preferences().valueOf("mds_device_attributes_content_type");

        // validation check override
        this.m_skip_validation = orchestrator.preferences().booleanValueOf("mds_skip_validation_override");
        if (this.m_skip_validation == true) {
            orchestrator.errorLogger().info("mbedDeviceServerProcessor: Validation Skip Override ENABLED");
        }

        // initialize our webhook validator
        this.m_webhook_validator = null;
        this.m_webhook_validator_poll_ms = 0;
        this.m_webhook_validator_enable = orchestrator.preferences().booleanValueOf("mds_webhook_validator_enable");

        // initialize the default type of URI for contacting us (GW) - this will be sent to mDS for the webhook URL
        this.setupConnectorURI();

        // initialize the default type of URI for contacting mDS
        this.setupDeviceServerURI();

        // OVEERRIDE - long polling vs. Webhook
        this.longPollOverrideSetup();

        // if using webhooks, we can optionally validate the webhook setting periodically in case it gets reset to nothing...
        if (this.m_webhook_validator_enable == true) {
            // enabling webhook/subscription validation
            this.m_webhook_validator_poll_ms = orchestrator.preferences().intValueOf("mds_webhook_validator_poll_ms");
            this.m_webhook_validator = new WebhookValidator(this, this.m_webhook_validator_poll_ms);

            // DEBUG
            orchestrator.errorLogger().warning("mbedDeviceServerProcessor: webhook/subscription validator ENABLED (interval: " + this.m_webhook_validator_poll_ms + "ms)");
        }

        // Announce version supported
        if (this.m_use_rest_versions == true) {
            // we are versioning our REST calls
            orchestrator.errorLogger().warning("mbedDeviceServerProcessor: Versioning of REST calls ENABLED (" + "v" + this.m_rest_version + ")");
        }
        else {
            // we are not versioning our REST calls
            orchestrator.errorLogger().warning("mbedDeviceServerProcessor: Versioning of REST calls DISABLED");
        }

        // configure the callback type based on the version of mDS (only if not using long polling)
        if (this.longPollEnabled() == false) {
            this.setupWebhookType();
        }

        // sanity check the configured mDS AUTH type
        this.sanityCheckAuthType();

        // disable sync usage if with Connector
        if (this.bridgingToConnector() == true) {
            this.errorLogger().info("mbedDeviceServerProcessor: Using mbed Device Connector. Sync=true DISABLED");
            this.m_disable_sync = true;
        }

        // init the device metadata resource URI's
        this.initDeviceMetadataResourceURIs();
        
         // remove on deregistration - mbed Cloud Override
        if (this.m_mds_host != null && this.m_mds_host.contains("mbedcloud.com")) {
            // override - disable remove on deregistration
            orchestrator.errorLogger().warning("mbedDeviceServerProcessor: mbed Cloud Integration ");
            
            // adjust
            this.m_mbed_cloud_integration = true;
        }
        
        // configuration for allowing de-registration messages to remove device shadows...or not.
        this.m_mds_remove_on_deregistration = this.prefBoolValue("mds_remove_on_deregistration");
        if (this.m_mds_remove_on_deregistration == true) {
            orchestrator.errorLogger().warning("mbedDeviceServerProcessor: device removal on deregistration ENABLED");
        }
        else {
            orchestrator.errorLogger().warning("mbedDeviceServerProcessor: device removal on deregistration DISABLED");
        }
    }
    
    // using mbed Cloud?
    @Override
    public boolean usingMbedCloud() {
        return this.m_mbed_cloud_integration;
    }
    
    // device removal on deregistration?
    @Override
    public boolean deviceRemovedOnDeRegistration() {
        return this.m_mds_remove_on_deregistration;
    }

    // using SSL or not for the webhook management set/get
    public boolean usingSSLInWebhookEstablishment() {
        return this.m_use_https_dispatch;
    }

    // Long polling enabled or disabled?
    private boolean longPollEnabled() {
        return (this.m_mds_enable_long_poll == true && this.m_mds_long_poll_uri != null && this.m_mds_long_poll_uri.length() > 0);
    }

    // get the long polling URL
    public String longPollURL() {
        return this.m_mds_long_poll_url;
    }

    // override use of long polling vs. webhooks for notifications
    private void longPollOverrideSetup() {
        if (this.longPollEnabled()) {
            // DEBUG
            this.errorLogger().warning("mbedDeviceServerProcessor: Long Poll Override ENABLED. Using Long Polling (webhook DISABLED)");

            // disable webhook validation
            this.m_webhook_validator_enable = false;

            // override use of long polling vs webhooks for notifications
            this.m_mds_long_poll_url = this.constructLongPollURL();

            // start the Long polling thread...
            this.startLongPolling();
        }
    }

    // build out the long poll URL
    private String constructLongPollURL() {
        String url = this.createBaseURL() + "/" + this.m_mds_long_poll_uri;
        this.errorLogger().info("constructLongPollURL: Long Poll URL: " + url);
        return url;
    }

    // start the long polling thread
    private void startLongPolling() {
        if (this.m_long_poll_processor == null) {
            this.m_long_poll_processor = new LongPollProcessor(this);
            this.m_long_poll_processor.startPolling();
        }
    }

    /**
     * start validation polling
     */
    @Override
    public void beginValidationPolling() {
        if (this.m_webhook_validator != null) {
            this.m_webhook_validator.startPolling();
        }
    }

    // initialize the device metadata resource URIs
    private void initDeviceMetadataResourceURIs() {
        this.m_device_manufacturer_res = this.prefValue("mds_device_manufacturer_res");
        this.m_device_serial_number_res = this.prefValue("mds_device_serial_number_res");
        this.m_device_model_res = this.prefValue("mds_device_model_res");
        this.m_device_class_res = this.prefValue("mds_device_class_res");
        this.m_device_description_res = this.prefValue("mds_device_description_res");
        this.m_device_firmware_info_res = this.prefValue("mds_device_firmware_info_res");
        this.m_device_hardware_info_res = this.prefValue("mds_device_hardware_info_res");
        this.m_device_descriptive_location_res = this.prefValue("mds_device_descriptive_location_res");
    }

    // mDS requires use of SSL (mDC)
    private Boolean requireSSL() {
        return this.m_mds_use_ssl;
    }

    // mDS using callback webhook vs. push-url
    private boolean usingWebhookCallbacks() {
        return (this.m_mds_gw_callback.equalsIgnoreCase("callback"));
    }

    // setup the connector bridge URI
    private void setupConnectorURI() {
        this.m_default_gw_uri = "http://";
        if (this.m_mds_gw_use_ssl) {
            this.m_default_gw_uri = "https://";
        }
    }

    // setup the mbed device server default URI
    @SuppressWarnings("empty-statement")
    private void setupDeviceServerURI() {
        this.m_default_mds_uri = "http://";
        try {
            Double ver = Double.valueOf(this.m_mds_version);
            if (ver >= Manager.MDS_NON_DOMAIN_VER_BASE && this.m_use_api_token && this.m_use_https_dispatch == true) {
                // we are using mDS Connector... 
                this.m_default_mds_uri = "https://";
                this.m_mds_port = 443;

                // we assume mDS is Connector and thus requires use of SSL throughout.
                this.m_mds_use_ssl = true;
            }
        }
        catch (NumberFormatException ex) {
            // silent
            ;
        }
    }

    // set the callback type we are using
    @SuppressWarnings("empty-statement")
    private void setupWebhookType() {
        try {
            Double ver = Double.valueOf(this.m_mds_version);
            if (ver >= Manager.MDS_NON_DOMAIN_VER_BASE) {
                if (this.m_mds_gw_callback.equalsIgnoreCase("push-url") == true) {
                    this.m_mds_gw_callback = "callback";     // force use of callback... push-url no longer used
                }
            }
        }
        catch (NumberFormatException ex) {
            // silent
            ;
        }

        // set the boolean checker...
        this.m_using_callback_webhooks = (this.m_mds_gw_callback.equalsIgnoreCase("callback") == true);
    }

    // our the mDS notifications coming in over the webhook validatable?
    private Boolean validatableNotifications() {
        return this.m_using_callback_webhooks;
    }

    // sanity check the authentication type
    private void sanityCheckAuthType() {
        // sanity check...
        if (this.m_use_api_token == true && (this.m_api_token == null || this.m_api_token.length() == 0)) {
            this.orchestrator().errorLogger().warning("WARNING: API TOKEN AUTH enabled but no token found/acquired... disabling...");
            this.m_use_api_token = false;
        }

        // DEBUG
        if (this.useAPITokenAuth()) {
            this.orchestrator().errorLogger().info("Using API TOKEN Authentication");
        }
        else {
            this.orchestrator().errorLogger().info("Using BASIC Authentication");
        }
    }

    // is our mDS instance actually mDC?
    private boolean bridgingToConnector() {
        return ((this.m_use_api_token == true && this.m_using_callback_webhooks == true && this.m_use_https_dispatch == true)
                || (this.m_use_api_token == true && this.m_mds_enable_long_poll == true && this.m_use_https_dispatch == true));
    }

    // mDS is using Token Auth
    private boolean useAPITokenAuth() {
        return this.m_use_api_token;
    }
    
    // validate the notification
    private Boolean validateNotification(HttpServletRequest request) {
        if (request != null) {
            boolean validated = false;
            if (this.validatableNotifications() == true && request.getHeader("Authentication") != null) {
                String calc_hash = this.orchestrator().createAuthenticationHash();
                String header_hash = request.getHeader("Authentication");
                validated = Utils.validateHash(header_hash, calc_hash);

                // DEBUG
                if (!validated) {
                    this.errorLogger().warning("validateNotification: failed: calc: " + calc_hash + " header: " + header_hash);
                }

                // override
                if (this.m_skip_validation == true) {
                    validated = true;
                }

                // return validation status
                return validated;
            }
            else {
                // using push-url. No authentication possible.
                return true;
            }
        }
        else {
            // no request - so assume we are validated
            return true;
        }
    }

    // create any authentication header JSON that may be necessary
    @SuppressWarnings("empty-statement")
    private String createWebhookHeaderAuthJSON() {
        String hash = this.orchestrator().createAuthenticationHash();

        try {
            Double ver = Double.valueOf(this.m_mds_version);
            if (hash != null && hash.equalsIgnoreCase("none") == true && ver > 3.0 && this.prefBoolValue("mds_use_gw_address") == true) {
                // local mDS does not use thi
                return null;
            }
        }
        catch (NumberFormatException ex) {
            // parsing error of mds_version... just use the default hash (likely "none")
            ;
        }

        // return the authentication header
        return "{\"Authentication\":\"" + hash + "\"}";
    }

    // create our webhook URL that we will get called back on...
    private String createWebhookURL() {
        String url = null;

        String local_ip = Utils.getExternalIPAddress(this.prefBoolValue("mds_use_gw_address"), this.prefValue("mds_gw_address"));
        int local_port = this.prefIntValue("mds_gw_port");
        if (this.m_mds_gw_use_ssl == true) {
            ++local_port;        // SSL will use +1 of this port... ensure firewall configs match!
        }
        String notify_uri = this.prefValue("mds_gw_context_path") + this.prefValue("mds_gw_events_path") + this.getDomain(true);

        // build and return the webhook callback URL
        return this.m_default_gw_uri + local_ip + ":" + local_port + notify_uri;
    }

    // create the dispatch URL for changing the notification webhook URL
    private String createWebhookDispatchURL() {
        return this.createBaseURL() + this.getDomain() + "/notification/" + this.m_mds_gw_callback;
    }

    // get the currently configured callback URL
    public String getWebhook() {
        String url = null;
        String headers = null;

        // create the dispatch URL
        String dispatch_url = this.createWebhookDispatchURL();

        // Issue GET and look at the response
        String json = null;

        // SSL vs. HTTP
        if (this.m_use_https_dispatch == true) {
            // get the callback URL (SSL)
            json = this.httpsGet(dispatch_url);
        }
        else {
            // get the callback URL
            json = this.httpGet(dispatch_url);
        }
        try {
            if (json != null && json.length() > 0) {
                if (this.m_mds_gw_callback.equalsIgnoreCase("callback")) {
                    // JSON parser does not like "headers":{}... so map it out
                    json = json.replace(",\"headers\":{}", "");

                    // Callback API used: parse the JSON
                    Map parsed = (Map) this.parseJson(json.replace(",\"headers\":{}", ""));
                    url = (String) parsed.get("url");

                    // headers are optional...
                    try {
                        headers = (String) parsed.get("headers");
                    }
                    catch (Exception json_ex) {
                        headers = "";
                    }

                    // DEBUG
                    this.orchestrator().errorLogger().info("getNotificationCallbackURL(callback): url: " + url + " headers: " + headers + " dispatch: " + dispatch_url);
                }
                else {
                    // use the Deprecated push-url API... (no JSON)
                    url = json;

                    // DEBUG
                    this.orchestrator().errorLogger().info("getNotificationCallbackURL(push-url): url: " + url + " dispatch: " + dispatch_url);
                }
            }
            else {
                // no response received back from mDS
                this.orchestrator().errorLogger().warning("getNotificationCallbackURL: no response recieved from dispatch: " + dispatch_url);
            }
        }
        catch (Exception ex) {
            this.orchestrator().errorLogger().warning("getNotificationCallbackURL: exception: " + ex.getMessage() + ". json=" + json);
        }

        return url;
    }

    // determine if our callback URL has already been set
    private boolean webhookSet(String target_url) {
        return this.webhookSet(target_url, false);
    }

    // determine if our callback URL has already been set
    private boolean webhookSet(String target_url, boolean skip_check) {
        String current_url = this.getWebhook();
        this.errorLogger().info("webhookSet: current_url: " + current_url + " target_url: " + target_url);
        boolean is_set = (target_url != null && current_url != null && target_url.equalsIgnoreCase(current_url));
        if (is_set == true && this.usingWebhookCallbacks() && skip_check == false) {
            // for Connector, lets ensure that we always have the expected Auth Header setup. So, while the same, lets delete and re-install...
            this.errorLogger().info("webhookSet(callback): deleting existing webhook URL...");
            this.removeWebhook();
            this.errorLogger().info("webhookSet(callback): re-establishing webhook URL...");
            this.setWebhook(target_url, skip_check); // skip_check, go ahead and assume we need to set it...
            this.errorLogger().info("webhookSet(callback): re-checking that webhook URL is properly set...");
            current_url = this.getWebhook();
            is_set = (target_url != null && current_url != null && target_url.equalsIgnoreCase(current_url));
        }
        return is_set;
    }

    // remove the mDS Connector Notification Callback webhook
    private void removeWebhook() {
        // create the dispatch URL
        String dispatch_url = this.createWebhookDispatchURL();

        // SSL vs. HTTP
        if (this.m_use_https_dispatch == true) {
            // delete the callback URL (SSL)
            this.httpsDelete(dispatch_url);
        }
        else {
            // delete the callback URL
            this.httpDelete(dispatch_url);
        }
    }

    // reset the mDS Notification Callback URL
    @Override
    public void resetWebhook() {
        if (this.validatableNotifications() == true) {
            // we simply delete the webhook 
            this.removeWebhook();
        }
        else {
            // we reset to default
            String default_url = this.prefValue("mds_default_notify_url");
            this.errorLogger().info("resetWebhook: resetting notification URL to: " + default_url);
            this.setWebhook(default_url);
        }
    }

    // set our mDS Notification Callback URL
    @Override
    public void setWebhook() {
        if (this.longPollEnabled() == false) {
            String target_url = this.createWebhookURL();
            this.setWebhook(target_url);
            
            // EXPERIMENTAL - test for bulk subscriptions setting
            if (this.m_enable_bulk_subscriptions == true) {
                this.setupBulkSubscriptions();
            }
        }
    }
    
    // EXPERIMENTAL: establish bulk subscription (disabled by default)
    private void setupBulkSubscriptions() {
        // DEBUG
        this.errorLogger().info("setupBulkSubscriptions: setting bulk subscriptions (EXPERIMENTAL)...");
        
        // JSON for the bulk subscription
        String json = "[{\"endpoint-name\":\"*\"}]";
        
        // Create the URI for the bulk subscription PUT
        String url = this.createBaseURL() + "/subscriptions";
        
        // DEBUG
        this.errorLogger().info("setupBulkSubscriptions: URL: " + url + " JSON: " + json);
        
        // send PUT to establish the bulk subscriptions
        String result = this.httpsPut(url, json, "application/json");
        int error_code = this.getLastResponseCode();
        
        // DEBUG
        this.errorLogger().info("setupBulkSubscriptions: Response Code: " + error_code + " RESULT: " + result);
    }

    // set our mDS Notification Callback URL
    private void setWebhook(String target_url) {
        this.setWebhook(target_url, true); // default is to check if the URL is already set... 
    }

    // set our mDS Notification Callback URL
    private void setWebhook(String target_url, boolean check_url_set) {
        boolean callback_url_already_set = false; // assume default is that the URL is NOT set... 

        // we must check to see if we want to check that the URL is already set...
        if (check_url_set == true) {
            // go see if the URL is already set.. 
            callback_url_already_set = this.webhookSet(target_url);
        }

        // proceed to set the URL if its not already set.. 
        if (!callback_url_already_set) {
            String dispatch_url = this.createWebhookDispatchURL();
            String auth_header_json = this.createWebhookHeaderAuthJSON();
            String json = null;

            // build out the callback JSON
            if (this.m_mds_gw_callback.equalsIgnoreCase("callback")) {
                // use the Callback API
                if (auth_header_json == null) {
                    json = "{ \"url\" :\"" + target_url + "\" }";
                }
                else {
                    json = "{ \"url\" :\"" + target_url + "\", \"headers\":" + auth_header_json + "}";
                }

                // DEBUG
                this.errorLogger().info("setWebhook(callback): json: " + json + " dispatch: " + dispatch_url);
            }
            else {
                // use the Deprecated push-url API... (no JSON)
                json = target_url;

                // DEBUG
                this.errorLogger().info("setWebhook(push-url): url: " + json + " dispatch: " + dispatch_url);
            }

            // SSL vs. HTTP
            if (this.m_use_https_dispatch == true) {
                // set the callback URL (SSL)
                this.httpsPut(dispatch_url, json);
            }
            else {
                // set the callback URL
                this.httpPut(dispatch_url, json);
            }

            // check that it succeeded
            if (!this.webhookSet(target_url, !check_url_set)) {
                // DEBUG
                this.errorLogger().warning("setWebhook: ERROR: unable to set callback URL to: " + target_url);

                // reset the webhook - its not set anymore
                if (this.m_webhook_validator != null) {
                    this.m_webhook_validator.resetWebhook();
                }
            }
            else {
                // DEBUG
                this.errorLogger().info("setWebhook: notification URL set to: " + target_url + " (SUCCESS)");

                // record the webhook
                if (this.m_webhook_validator != null) {
                    this.m_webhook_validator.setWebhook(target_url);
                }
            }
        }
        else {
            // DEBUG
            this.errorLogger().info("setWebhook: notification URL already set to: " + target_url + " (OK)");

            // record the webhook
            if (this.m_webhook_validator != null) {
                this.m_webhook_validator.setWebhook(target_url);
            }
        }
    }

    // create the Endpoint Subscription Notification URL
    private String createEndpointResourceSubscriptionURL(String uri, Map options) {
        // build out the URL for mDS Endpoint notification subscriptions...
        String url = this.createBaseURL() + "/" + uri;

        // SYNC Usage
        // add options if present
        if (options != null && this.m_disable_sync == false) {
            // valid options...
            String sync = (String) options.get("sync");

            // construct the query string...
            String qs = "";
            qs = this.buildQueryString(qs, "sync", sync);
            if (qs != null && qs.length() > 0) {
                url = url + "?" + qs;
            }
        }

        // DEBUG
        this.errorLogger().info("createEndpointResourceSubscriptionURL: " + url);

        // return the endpoint notification subscription URL
        return url;
    }

    // create the Endpoint Subscription Notification URL (default options)
    private String createEndpointResourceSubscriptionURL(String endpoint, String uri) {
        HashMap<String, String> options = new HashMap<>();

        // SYNC Usage
        if (this.m_disable_sync == false) {
            options.put("sync", "true");
        }
        return this.createEndpointResourceSubscriptionURL(endpoint, uri, options);
    }

    // create the Endpoint Subscription Notification URL
    private String createEndpointResourceSubscriptionURL(String endpoint, String uri, Map<String, String> options) {
        // build out the URL for mDS Endpoint notification subscriptions...
        // /{domain}/subscriptions/{endpoint-name}/{resource-path}?sync={true&#124;false}
        String url = this.createBaseURL() + this.getDomain() + "/subscriptions/" + endpoint + uri;

        // SYNC Usage 
        // add options if present
        if (options != null && this.m_disable_sync == false) {
            // valid options...
            String sync = (String) options.get("sync");

            // construct the query string...
            String qs = "";
            qs = this.buildQueryString(qs, "sync", sync);
            if (qs != null && qs.length() > 0) {
                url = url + "?" + qs;
            }
        }

        // DEBUG
        this.errorLogger().info("createEndpointResourceSubscriptionURL: " + url);

        // return the endpoint notification subscription URL
        return url;
    }

    // create the Endpoint Resource Request URL 
    private String createEndpointResourceRequestURL(String uri, Map options) {
        // build out the URL for mDS Endpoint Resource requests...
        String url = this.createBaseURL() + uri;

        // add options if present
        if (options != null) {
            // valid options...
            String sync = (String) options.get("sync");
            String cacheOnly = (String) options.get("cacheOnly");
            String noResp = (String) options.get("noResp");
            String pri = (String) options.get("pri");

            // construct the query string...
            String qs = "";

            // SYNC Usage
            if (this.m_disable_sync == false) {
                qs = this.buildQueryString(qs, "sync", sync);
            }

            qs = this.buildQueryString(qs, "cacheOnly", cacheOnly);
            qs = this.buildQueryString(qs, "noResp", noResp);
            qs = this.buildQueryString(qs, "pri", pri);
            if (qs != null && qs.length() > 0) {
                url = url + "?" + qs;
            }
        }

        // DEBUG
        this.errorLogger().info("createEndpointResourceRequestURL: " + url);

        // return the endpoint resource request URL
        return url;
    }

    // create the Endpoint Resource Discovery URL 
    private String createEndpointResourceDiscoveryURL(String uri) {
        // build out the URL for mDS Endpoint Resource discovery...
        String url = this.createBaseURL() + uri;

        // no options available...
        // DEBUG
        //this.errorLogger().info("createEndpointResourceDiscoveryURL: " + url);
        // return the endpoint resource discovery URL
        return url;
    }

    // create the Endpoint Discovery URL 
    private String createEndpointDiscoveryURL(Map options) {
        // build out the URL for mDS Endpoint Discovery...
        String url = this.createBaseURL() + this.getDomain() + "/endpoints";

        // add options if present
        if (options != null) {
            // valid options...
            String type = (String) options.get("type");
            String stale = (String) options.get("stale");

            // construct the query string...
            String qs = "";
            qs = this.buildQueryString(qs, "type", type);
            qs = this.buildQueryString(qs, "stale", stale);
            if (qs != null && qs.length() > 0) {
                url = url + "?" + qs;
            }
        }

        // DEBUG
        //this.errorLogger().info("createEndpointDiscoveryURL: " + url);
        // return the discovery URL
        return url;
    }
    
    // process device-deletions of endpoints (mbed Cloud only)
    @Override
    public void processDeviceDeletions(String[] endpoints) {
        for (int i = 0; i < endpoints.length; ++i) {
            // remove from the validator - bookkeeping
            if (this.m_webhook_validator != null) {
                this.m_webhook_validator.removeSubscriptionsforEndpoint(endpoints[i]);
            }
        }
    }
    
    // process de-registeration of endpoints
    @Override
    public void processDeregistrations(String[] endpoints) {
        for (int i = 0; i < endpoints.length; ++i) {
            // create the endpoint subscription removal URL...
            String url = this.createBaseURL() + this.getDomain() + "/endpoints/" + endpoints[i];
            this.errorLogger().info("processDeregistrations: sending endpoint subscription removal request: " + url);
            this.httpDelete(url);

            // remove from the validator - bookkeeping
            if (this.m_webhook_validator != null) {
                this.m_webhook_validator.removeSubscriptionsforEndpoint(endpoints[i]);
            }
        }
    }
    
    // process registerations-expired of endpoints
    @Override
    public void processRegistrationsExpired(String[] endpoints) {
        // nothing to process for device server
    }
    
    // process the notification
    @Override
    public void processNotificationMessage(HttpServletRequest request, HttpServletResponse response) {
        // build the response
        String response_header = "";
        String json = this.read(request);

        // process and route the mDS message
        this.processDeviceServerMessage(json, request);

        // send the response back as an ACK to mDS
        this.sendResponseToDeviceServer("text/html;charset=UTF-8", request, response, "", "");
    }

    // process and route the mDS message to the appropriate peer method (long poll method)
    public void processDeviceServerMessage(String json) {
        this.processDeviceServerMessage(json, null);
    }

    // process and route the mDS message to the appropriate peer method
    private void processDeviceServerMessage(String json, HttpServletRequest request) {
        // DEBUG
        this.orchestrator().errorLogger().info("processDeviceServerMessage(mDS): Received message from mDS: " + json);

        // tell the orchestrator to call its peer processors with this mDS message
        try {
            if (json != null && json.length() > 0 && json.equalsIgnoreCase("{}") == false) {
                Map parsed = (Map) this.parseJson(json);
                if (parsed != null) {
                    if (parsed.containsKey("notifications")) {
                        if (this.validateNotification(request)) {
                            // DEBUG
                            this.errorLogger().info("processDeviceServerMessage(mDS): notification VALIDATED");

                            // validated notification... process it...
                            this.orchestrator().processNotification(parsed);
                        }
                        else {
                            // validation FAILED. Note but do not process...
                            this.errorLogger().warning("processDeviceServerMessage(mDS): notification validation FAILED. Not processed (OK)");
                        }
                    }

                    // DEBUG
                    this.errorLogger().info("processDeviceServerMessage(mDS) Parsed: " + parsed);
                    
                    // act on the request...
                    if (parsed.containsKey("registrations")) {
                        this.orchestrator().processNewRegistration(parsed);
                    }
                    if (parsed.containsKey("reg-updates")) {
                        this.orchestrator().processReRegistration(parsed);
                    }
                    if (parsed.containsKey("de-registrations")) {
                        this.orchestrator().processDeregistrations(parsed);
                    }
                    if (parsed.containsKey("registrations-expired")) {
                        this.orchestrator().processRegistrationsExpired(parsed);
                    }
                    if (parsed.containsKey("async-responses")) {
                        this.orchestrator().processAsyncResponses(parsed);
                    }
                }
                else {
                    // parseJson() failed...
                    this.errorLogger().warning("processDeviceServerMessage(mDS): unable to parse JSON: " + json);
                }
            }
            else {
                // empty JSON... so not parsed
                this.errorLogger().info("processDeviceServerMessage(mDS): empty JSON not parsed (OK).");
            }
        }
        catch (Exception ex) {
            // exception during JSON parsing
            this.errorLogger().info("processDeviceServerMessage(mDS): Exception during notification body JSON parsing: " + json + "... ignoring.", ex);
        }
    }

    // process an endpoint resource subscription request
    @Override
    public String subscribeToEndpointResource(String uri, Map options, Boolean init_webhook) {
        if (this.m_enable_bulk_subscriptions == false) {
            String url = this.createEndpointResourceSubscriptionURL(uri, options);
            return this.subscribeToEndpointResource(url, init_webhook);
        }
        else {
            this.errorLogger().info("subscribeToEndpointResource: Using bulk subscriptions: URI: " + uri + " Map: " + options + " webhook: " + init_webhook);
            return "";
        }
    }

    // process an endpoint resource subscription request
    @Override
    public String subscribeToEndpointResource(String ep_name, String uri, Boolean init_webhook) {
        if (this.m_enable_bulk_subscriptions == false) {
            String url = this.createEndpointResourceSubscriptionURL(ep_name, uri);
            return this.subscribeToEndpointResource(url, init_webhook);
        }
        else {
            this.errorLogger().info("subscribeToEndpointResource: Using bulk subscriptions: URI: " + uri + " EP: " + ep_name + " webhook: " + init_webhook);
            return "";
        }
    }

    // subscribe to endpoint resources
    public String subscribeToEndpointResource(String url) {
        return this.subscribeToEndpointResource(url, false);
    }

    // create the mDS/mDC URI for subscriptions:  "subscriptions/<endpoint>/<uri>"  
    @Override
    public String createSubscriptionURI(String ep_name, String uri) {
        return "subscriptions" + "/" + ep_name + uri;
    }

    // subscribe to endpoint resources
    private String subscribeToEndpointResource(String url, Boolean init_webhook) {
        if (init_webhook) {
            this.errorLogger().info("subscribeToEndpointResource: (re)setting the event notification URL...");
            this.setWebhook();
        }

        String json = null;
        this.errorLogger().info("subscribeToEndpointResource: (re)establishing subscription request: " + url);
        if (this.requireSSL()) {
            json = this.httpsPut(url);
        }
        else {
            json = this.httpPut(url);
        }

        // save off the subscription
        if (this.m_webhook_validator != null) {
            this.m_webhook_validator.addSubscription(url);
        }

        // return the result
        return json;
    }

    // get to endpoint resource subscription 
    public boolean getEndpointResourceSubscriptionStatus(String url) {
        boolean subscribed = false;
        String json = null;
        this.errorLogger().info("getEndpointResourceSubscriptionStatus: getting subscription status: " + url);
        if (this.requireSSL()) {
            this.httpsGet(url);
        }
        else {
            this.httpGet(url);
        }

        // check the status...
        int status = this.getLastResponseCode();
        status = status - 200;
        if (status >= 0 && status < 100) {
            // 20x response - OK
            subscribed = true;
        }

        // return the result
        return subscribed;
    }

    // process endpoint resource operation request
    @Override
    public String processEndpointResourceOperation(String verb, String ep_name, String uri, String value, String options) {
        String json = null;
        String url = this.createCoAPURL(ep_name, uri);

        // add our options if they are specified
        if (options != null && options.length() > 0 && options.contains("=") == true) {
            // There is no way to validate that these options dont break the request... there may also be security issues here. 
            url += "?" + options;
        }

        if (verb != null && verb.length() > 0) {
            // dispatch the mDS REST based on CoAP verb received
            if (verb.equalsIgnoreCase(("get"))) {
                this.errorLogger().info("processEndpointResourceOperation: Invoking GET: " + url);
                if (this.requireSSL()) {
                    json = this.httpsGet(url);
                }
                else {
                    json = this.httpGet(url);
                }
            }
            if (verb.equalsIgnoreCase(("put"))) {
                this.errorLogger().info("processEndpointResourceOperation: Invoking PUT: " + url + " DATA: " + value);
                if (this.requireSSL()) {
                    json = this.httpsPut(url, value);
                }
                else {
                    json = this.httpPut(url, value);
                }
            }
            if (verb.equalsIgnoreCase(("post"))) {
                this.errorLogger().info("processEndpointResourceOperation: Invoking POST: " + url + " DATA: " + value);
                if (this.requireSSL()) {
                    json = this.httpsPost(url, value, "plain/text");  // nail content_type to "plain/text"
                }
                else {
                    json = this.httpPost(url, value, "plain/text");   // nail content_type to "plain/text"
                }
            }
            if (verb.equalsIgnoreCase(("delete"))) {
                this.errorLogger().info("processEndpointResourceOperation: Invoking DELETE: " + url);
                if (this.requireSSL()) {
                    json = this.httpsDelete(url, "plain/text");      // nail content_type to "plain/text"
                }
                else {
                    json = this.httpDelete(url, "plain/text");       // nail content_type to "plain/text"
                }
            }
            if (verb.equalsIgnoreCase(("del"))) {
                this.errorLogger().info("processEndpointResourceOperation: Invoking DELETE: " + url);
                if (this.requireSSL()) {
                    json = this.httpsDelete(url, "plain/text");      // nail content_type to "plain/text"
                }
                else {
                    json = this.httpDelete(url, "plain/text");       // nail content_type to "plain/text"
                }
            }
        }
        else {
            this.errorLogger().warning("processEndpointResourceOperation: ERROR: CoAP Verb is NULL. Not processing: ep: " + ep_name + " uri: " + uri + " value: " + value);
            json = null;
        }

        return json;
    }
    
    // process an endpoint resource un-subscribe request
    @Override
    public String unsubscribeFromEndpointResource(String uri, Map options) {
        String url = this.createEndpointResourceSubscriptionURL(uri, options);

        // remove the subscription
        String json = this.unsubscribeFromEndpointResource(url);

        // remove subscription
        if (this.m_webhook_validator != null) {
            this.m_webhook_validator.removeSubscription(url);
        }

        // return the JSON result
        return json;
    }

    // remove the mDS Connector Notification Callback
    public String unsubscribeFromEndpointResource(String url) {
        String json = null;

        // DEBUG
        this.errorLogger().info("unsubscribeFromEndpointResource: unsubscribing: " + url);

        // SSL vs. HTTP
        if (this.m_use_https_dispatch == true) {
            // delete the callback URL (SSL)
            json = this.httpsDelete(url);
        }
        else {
            // delete the callback URL
            json = this.httpDelete(url);
        }

        // return any resultant json
        return json;
    }

    // perform device discovery
    @Override
    public String performDeviceDiscovery(Map options) {
        String url = this.createEndpointDiscoveryURL(options);
        String json = null;

        // mDS expects request to come as a http GET
        if (this.requireSSL()) {
            json = this.httpsGet(url);
        }
        else {
            json = this.httpGet(url);
        }
        return json;
    }

    // perform device resource discovery
    @Override
    public String performDeviceResourceDiscovery(String uri) {
        String url = this.createEndpointResourceDiscoveryURL(uri);
        String json = null;
        if (this.requireSSL()) {
            json = this.httpsGet(url);
        }
        else {
            json = this.httpGet(url);
        }
        return json;
    }

    // initialize the endpoint's default attributes 
    private void initDeviceWithDefaultAttributes(Map endpoint) {
        this.pullDeviceManufacturer(endpoint);
        this.pullDeviceSerialNumber(endpoint);
        this.pullDeviceModel(endpoint);
        this.pullDeviceClass(endpoint);
        this.pullDeviceDescription(endpoint);
        this.pullDeviceHardwareInfo(endpoint);
        this.pullDeviceLocationDescriptionInfo(endpoint);
        this.pullDeviceCurrentTimeInfo(endpoint);
        this.pullDeviceTotalMemoryInfo(endpoint);
    }

    // determine if a given endpoint actually has device attributes or not... if not, the defaults will be used
    private boolean hasDeviceAttributes(Map endpoint) {
        boolean has_device_attributes = false;

        try {
            // get the list of resources from the endpoint
            List resources = (List) endpoint.get("resources");

            // look for a special resource - /3/0
            if (resources != null && resources.size() > 0) {
                for (int i = 0; i < resources.size() && !has_device_attributes; ++i) {
                    Map resource = (Map) resources.get(i);
                    if (resource != null) {
                        // get the path value
                        String path = (String) resource.get("path");

                        // look for /3/0
                        if (path != null && path.equalsIgnoreCase(this.m_device_attributes_path) == true) {
                            // we have device attributes in this endpoint... go get 'em. 
                            has_device_attributes = true;
                        }
                    }
                }
            }
        }
        catch (Exception ex) {
            // caught exception
            this.errorLogger().info("hasDeviceAttributes: Exception caught: " + ex.getMessage(), ex);
        }
        
        // DEBUG
        if (has_device_attributes == true) {
            this.errorLogger().info("hasDeviceAttributes: HAS DEVICE ATTRIBUTES: " + endpoint);
        }
        else {
            this.errorLogger().info("hasDeviceAttributes: DOES NOT HAVE DEVICE ATTRIBUTES: " + endpoint);
        }

        // return our status
        return has_device_attributes;
    }

    // dispatch GETs to retrieve the actual device attributes
    private void dispatchDeviceAttributeGETs(Map endpoint, AsyncResponseProcessor processor) {
        // Create the Device Attributes URL
        String url = this.createCoAPURL((String) endpoint.get("ep"), this.m_device_attributes_path);

        // DEBUG
        //this.errorLogger().info("ATTRIBUTES: Calling GET to receive: " + url);
        
        // Dispatch and get the response (an AsyncId)
        String json_response = this.httpsGet(url, this.m_device_attributes_content_type);

        // record the response to get processed later
        if (json_response != null) {
            this.orchestrator().recordAsyncResponse(json_response, url, endpoint, processor);
        }
    }

    // check and dispatch the appropriate GETs to retrieve the actual device attributes
    private void getActualDeviceAttributes(Map endpoint, AsyncResponseProcessor processor) {
        // dispatch GETs to retrieve the attributes from the endpoint... 
        if (this.hasDeviceAttributes(endpoint)) {
            // dispatch GETs to to retrieve and parse those attributes
            this.dispatchDeviceAttributeGETs(endpoint,processor);
        }
        else {
            // device does not have device attributes... so just use the defaults... 
            AsyncResponseProcessor peer_processor = (AsyncResponseProcessor) endpoint.get("peer_processor");
            if (peer_processor != null) {
                // call the AsyncResponseProcessor within the peer...
                peer_processor.processAsyncResponse(endpoint);
            }
            else {
                // error - no peer AsyncResponseProcessor...
                this.errorLogger().warning("getActualDeviceAttributes: no peer AsyncResponse processor. Device may not get addeded within peer.");
            }
        }
    }

    // parse the device attributes
    private Map parseDeviceAttributes(Map response, Map endpoint) {
        LWM2MResource res = null;
        
        try {
            // Convert the TLV to a LWM2M Resource List...
            List<LWM2MResource> list = Utils.tlvDecodeToLWM2MObjectList(this.errorLogger(),(String) response.get("payload"));
            
            // DEBUG
            //for(int i=0;list != null && i<list.size();++i) {
            //    res = list.get(i);
            //    this.errorLogger().info("parseDeviceAttributes: URI: " + 
            //                            this.m_device_attributes_path + "/" + res.getId().intValue() + " Value: " + res.getStringValue() + "]");
            //}
            
            // /3/0/0
            endpoint.put("meta_mfg", Utils.getLWM2MResourceValueByResourceID(this.errorLogger(),list,0)); 
            
            // /3/0/1
            endpoint.put("meta_model", Utils.getLWM2MResourceValueByResourceID(this.errorLogger(),list,1));
            
            // /3/0/2
            endpoint.put("meta_serial", Utils.getLWM2MResourceValueByResourceID(this.errorLogger(),list,2));
            
            // /3/0/13
            endpoint.put("meta_time", Utils.getLWM2MResourceValueByResourceID(this.errorLogger(),list,13)); 
            
            // /3/0/17
            endpoint.put("meta_type", Utils.getLWM2MResourceValueByResourceID(this.errorLogger(),list,17)); 
            
            // /3/0/18
            endpoint.put("meta_hardware", Utils.getLWM2MResourceValueByResourceID(this.errorLogger(),list,18)); 
            
            // /3/0/21
            endpoint.put("meta_total_mem", Utils.getLWM2MResourceValueByResourceID(this.errorLogger(),list,21)); 
        }
        catch (Exception ex) {
            // exception during TLV parse... 
            this.errorLogger().info("parseDeviceAttributes: Error parsing TLV device attributes... using defaults...OK: " + ex.getMessage(),ex);
        }

        // return the updated endpoint
        return endpoint;
    }

    // callback for device attribute processing... 
    @Override
    public boolean processAsyncResponse(Map response) {
        // DEBUG
        //this.errorLogger().info("processAsyncResponse(MDS): RESPONSE: " + response);

        // Get the originating record
        HashMap<String, Object> record = (HashMap<String, Object>) response.get("orig_record");
        if (record != null) {
            Map orig_endpoint = (Map) record.get("orig_endpoint");
            if (orig_endpoint != null) {
                // Get the peer processor
                AsyncResponseProcessor peer_processor = (AsyncResponseProcessor) orig_endpoint.get("peer_processor");
                if (peer_processor != null) {
                    // parse the device attributes
                    this.errorLogger().info("mDS: processAsyncResponse: ORIG endpoint: " + orig_endpoint);
                    this.errorLogger().info("mDS: processAsyncResponse: RESPONSE: " + response);
                    Map endpoint = this.parseDeviceAttributes(response,orig_endpoint);
                    
                    // DEBUG
                    this.errorLogger().info("mDS: processAsyncResponse: endpoint: " + endpoint);

                    // call the AsyncResponseProcessor within the peer to finalize the device
                    peer_processor.processAsyncResponse(endpoint);
                }
                else {
                    // error - no peer AsyncResponseProcessor...
                    this.errorLogger().warning("processAsyncResponse(MDS): no peer AsyncResponse processor. Device may not get addeded within peer: " + record);
                }
            }
            else {
                // error - no peer AsyncResponseProcessor...
                this.errorLogger().warning("processAsyncResponse(MDS): no peer AsyncResponse processor. Device may not get addeded within peer: " + orig_endpoint);
            }

            // return processed status (defaulted)
            return true;
        }

        // return non-processed
        return false;
    }

    // pull the initial device metadata from mDS.. add it to the device endpoint map
    @Override
    public void pullDeviceMetadata(Map endpoint, AsyncResponseProcessor processor) {
        // initialize the endpoint with defaulted device attributes
        this.initDeviceWithDefaultAttributes(endpoint);

        // save off the peer processor for later
        endpoint.put("peer_processor", processor);

        // invoke GETs to retrieve the actual attributes (we are the processor for the callbacks...)
        this.getActualDeviceAttributes(endpoint, this);
    }

    // read the request data
    @SuppressWarnings("empty-statement")
    private String read(HttpServletRequest request) {
        try {
            BufferedReader reader = request.getReader();
            String line = reader.readLine();
            StringBuilder buf = new StringBuilder();
            while (line != null) {
                buf.append(line);
                line = reader.readLine();
            }
            return buf.toString();
        }
        catch (IOException ex) {
            // do nothing
            ;
        }
        return null;
    }

    // send the REST response back to mDS
    private void sendResponseToDeviceServer(String content_type, HttpServletRequest request, HttpServletResponse response, String header, String body) {
        try {
            response.setContentType(content_type);
            response.setHeader("Pragma", "no-cache");
            try (PrintWriter out = response.getWriter()) {
                if (header != null && header.length() > 0) {
                    out.println(header);
                }
                if (body != null && body.length() > 0) {
                    out.println(body);
                }
            }
        }
        catch (Exception ex) {
            this.errorLogger().critical("Unable to send REST response", ex);
        }
    }

    // add REST version information
    private String connectorVersion() {
        if (this.m_use_rest_versions == true) {
            // return the configured version string
            return "/v" + this.m_rest_version;
        }

        // not using rest versioning
        return "";
    }

    // create the base URL for mDS operations
    private String createBaseURL() {
        return this.m_default_mds_uri + this.m_mds_host + ":" + this.m_mds_port + this.connectorVersion();
    }

    // create the CoAP operation URL
    private String createCoAPURL(String ep_name, String uri) {
        String sync_option = "";

        // SYNC Usage
        if (this.m_disable_sync == false) {
            sync_option = "?sync=true";
        }

        String url = this.createBaseURL() + this.getDomain() + "/endpoints/" + ep_name + uri + sync_option;
        return url;
    }

    // build out the query string
    private String buildQueryString(String qs, String key, String value) {
        String updated_qs = qs;

        if (updated_qs != null && key != null && value != null) {
            if (updated_qs.length() == 0) {
                updated_qs = key + "=" + value;
            }
            else if (updated_qs.contains(key) == false) {
                updated_qs = updated_qs + "&" + key + "=" + value;
            }
            else {
                // attempted overwrite of previously set value
                this.errorLogger().warning("attempted overwrite of option: " + key + "=" + value + " in qs: " + updated_qs);
            }
        }

        return updated_qs;
    }

    //
    // The following methods are stubbed out for now - they provide defaulted device metadata info.
    // The actual CoAP Resource URI's are specified in the bridge configuration file and must be the same for all devices.
    // 
    // pull the device manufacturer information
    private void pullDeviceManufacturer(Map endpoint) {
        //this.m_device_manufacturer_res
        endpoint.put("meta_mfg", "ARM");
    }

    // pull the device Serial Number information
    private void pullDeviceSerialNumber(Map endpoint) {
        //this.m_device_serial_number_res
        endpoint.put("meta_serial", "0123456789");
    }

    // pull the device model information
    private void pullDeviceModel(Map endpoint) {
        //this.m_device_model_res
        endpoint.put("meta_model", "mbed");
    }

    // pull the device manufacturer information
    private void pullDeviceClass(Map endpoint) {
        //this.m_device_class_res
        endpoint.put("meta_class", "cortex-m");
    }

    // pull the device manufacturer information
    private void pullDeviceDescription(Map endpoint) {
        //this.m_device_description_res
        endpoint.put("meta_description", "mbed device");
    }

    // pull the device hardware information
    private void pullDeviceHardwareInfo(Map endpoint) {
        //this.m_device_hardware_info_res
        endpoint.put("meta_hardware", "1.0");
    }

    // pull the description location information for the device
    private void pullDeviceLocationDescriptionInfo(Map endpoint) {
        //this.m_device_descriptive_location_res
        endpoint.put("meta_location", "n/a");
    }
    
    // pull the current time from the device
    private void pullDeviceCurrentTimeInfo(Map endpoint) {
        //epoc
        endpoint.put("meta_time",Utils.getUTCTime());  // UTC time
    }
    
    // pull the total device memory information for the device
    private void pullDeviceTotalMemoryInfo(Map endpoint) {
        //this.m_device_descriptive_location_res
        endpoint.put("meta_total_mem", "128K");  // typical min: 128k
    }
    
    // get the last response code
    public int getLastResponseCode() {
        return this.m_http.getLastResponseCode();
    }

    // invoke peristent HTTP Get
    public String persistentHTTPGet(String url) {
        return this.persistentHTTPGet(url, this.m_content_type);
    }

    // invoke peristent HTTPS Get
    private String persistentHTTPGet(String url, String content_type) {
        String response = null;
        if (this.useAPITokenAuth()) {
            response = this.m_http.httpPersistentGetApiTokenAuth(url, this.m_api_token, null, content_type, this.m_mds_domain);
        }
        else {
            response = this.m_http.httpPeristentGet(url, this.m_mds_username, this.m_mds_password, null, content_type, this.m_mds_domain);
        }
        this.errorLogger().info("persistentHTTPGet: response: " + this.m_http.getLastResponseCode());
        return response;
    }

    // invoke peristent HTTPS Get
    public String persistentHTTPSGet(String url) {
        return this.persistentHTTPSGet(url, this.m_content_type);
    }

    // invoke peristent HTTPS Get
    private String persistentHTTPSGet(String url, String content_type) {
        String response = null;
        if (this.useAPITokenAuth()) {
            response = this.m_http.httpsPersistentGetApiTokenAuth(url, this.m_api_token, null, content_type, this.m_mds_domain);
        }
        else {
            response = this.m_http.httpsPeristentGet(url, this.m_mds_username, this.m_mds_password, null, content_type, this.m_mds_domain);
        }
        this.errorLogger().info("persistentHTTPSGet: response: " + this.m_http.getLastResponseCode());
        return response;
    }

    // invoke HTTP GET request (SSL)
    private String httpsGet(String url) {
        return this.httpsGet(url, this.m_content_type);
    }

    // invoke HTTP GET request (SSL)
    private String httpsGet(String url, String content_type) {
        String response = null;
        if (this.useAPITokenAuth()) {
            response = this.m_http.httpsGetApiTokenAuth(url, this.m_api_token, null, content_type, this.m_mds_domain);
        }
        else {
            response = this.m_http.httpsGet(url, this.m_mds_username, this.m_mds_password, null, content_type, this.m_mds_domain);
        }
        this.errorLogger().info("httpsGet: response: " + this.m_http.getLastResponseCode());
        return response;
    }

    // invoke HTTP GET request
    private String httpGet(String url) {
        return this.httpGet(url, this.m_content_type);
    }

    // invoke HTTP GET request
    private String httpGet(String url, String content_type) {
        String response = null;
        if (this.useAPITokenAuth()) {
            response = this.m_http.httpGetApiTokenAuth(url, this.m_api_token, null, content_type, this.m_mds_domain);
        }
        else {
            response = this.m_http.httpGet(url, this.m_mds_username, this.m_mds_password, null, content_type, this.m_mds_domain);
        }
        this.errorLogger().info("httpGet: response: " + this.m_http.getLastResponseCode());
        return response;
    }

    // invoke HTTP PUT request (SSL)
    private String httpsPut(String url) {
        return this.httpsPut(url, null);
    }

    // invoke HTTP PUT request (SSL)
    private String httpsPut(String url, String data) {
        return this.httpsPut(url, data, this.m_content_type);
    }

    // invoke HTTP PUT request (SSL)
    private String httpsPut(String url, String data, String content_type) {
        String response = null;
        if (this.useAPITokenAuth()) {
            response = this.m_http.httpsPutApiTokenAuth(url, this.m_api_token, data, content_type, this.m_mds_domain);
        }
        else {
            response = this.m_http.httpsPut(url, this.m_mds_username, this.m_mds_password, data, content_type, this.m_mds_domain);
        }
        this.errorLogger().info("httpsPut: response: " + this.m_http.getLastResponseCode());
        return response;
    }

    // invoke HTTP PUT request
    private String httpPut(String url) {
        return this.httpPut(url, null);
    }

    // invoke HTTP PUT request
    private String httpPut(String url, String data) {
        return this.httpPut(url, data, this.m_content_type);
    }

    // invoke HTTP PUT request
    private String httpPut(String url, String data, String content_type) {
        String response = null;
        if (this.useAPITokenAuth()) {
            response = this.m_http.httpPutApiTokenAuth(url, this.m_api_token, data, content_type, this.m_mds_domain);
        }
        else {
            response = this.m_http.httpPut(url, this.m_mds_username, this.m_mds_password, data, content_type, this.m_mds_domain);
        }
        this.errorLogger().info("httpPut: response: " + this.m_http.getLastResponseCode());
        return response;
    }

    // invoke HTTP POST request (SSL)
    private String httpsPost(String url, String data, String content_type) {
        String response = null;
        if (this.useAPITokenAuth()) {
            response = this.m_http.httpsPostApiTokenAuth(url, this.m_api_token, data, content_type, this.m_mds_domain);
        }
        else {
            response = this.m_http.httpsPost(url, this.m_mds_username, this.m_mds_password, data, content_type, this.m_mds_domain);
        }
        this.errorLogger().info("httpsPost: response: " + this.m_http.getLastResponseCode());
        return response;
    }

    // invoke HTTP POST request - set the content_type to "plain/text" forcefully...
    private String httpPost(String url, String data, String content_type) {
        String response = null;
        if (this.useAPITokenAuth()) {
            response = this.m_http.httpPostApiTokenAuth(url, this.m_api_token, data, content_type, this.m_mds_domain);
        }
        else {
            response = this.m_http.httpPost(url, this.m_mds_username, this.m_mds_password, data, content_type, this.m_mds_domain);
        }
        this.errorLogger().info("httpPost: response: " + this.m_http.getLastResponseCode());
        return response;
    }

    // invoke HTTP DELETE request
    private String httpsDelete(String url) {
        return this.httpsDelete(url, this.m_content_type);
    }

    // invoke HTTP DELETE request
    private String httpsDelete(String url, String content_type) {
        String response = null;
        if (this.useAPITokenAuth()) {
            response = this.m_http.httpsDeleteApiTokenAuth(url, this.m_api_token, null, content_type, this.m_mds_domain);
        }
        else {
            response = this.m_http.httpsDelete(url, this.m_mds_username, this.m_mds_password, null, content_type, this.m_mds_domain);
        }
        this.errorLogger().info("httpDelete: response: " + this.m_http.getLastResponseCode());
        return response;
    }

    // invoke HTTP DELETE request
    private String httpDelete(String url) {
        return this.httpDelete(url, this.m_content_type);
    }

    // invoke HTTP DELETE request
    private String httpDelete(String url, String content_type) {
        String response = null;
        if (this.useAPITokenAuth()) {
            response = this.m_http.httpDeleteApiTokenAuth(url, this.m_api_token, null, content_type, this.m_mds_domain);
        }
        else {
            response = this.m_http.httpDelete(url, this.m_mds_username, this.m_mds_password, null, content_type, this.m_mds_domain);
        }
        this.errorLogger().info("httpDelete: response: " + this.m_http.getLastResponseCode());
        return response;
    }
}
