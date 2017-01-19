/**
 * @file    Processor.java
 * @brief peer processor base class
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
package com.arm.connector.bridge.coordinator.processors.core;

import com.arm.connector.bridge.coordinator.Orchestrator;
import com.arm.connector.bridge.coordinator.processors.interfaces.AsyncResponseProcessor;
import com.arm.connector.bridge.core.BaseClass;
import com.arm.connector.bridge.core.TypeDecoder;
import com.arm.connector.bridge.core.Utils;
import com.arm.connector.bridge.json.JSONGenerator;
import com.arm.connector.bridge.json.JSONParser;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.commons.codec.binary.Base64;

/**
 * Peer Processor base class
 *
 * @author Doug Anson
 */
public class Processor extends BaseClass {
    public static int NUM_COAP_VERBS = 4;                                   // GET, PUT, POST, DELETE
    private static final String DEFAULT_EMPTY_STRING = " ";
    private Orchestrator m_orchestrator = null;
    private JSONGenerator m_json_generator = null;
    private JSONParser m_json_parser = null;
    protected String m_mds_domain = null;
    private String m_def_domain = null;
    protected String m_suffix = null;
    private String m_empty_string = Processor.DEFAULT_EMPTY_STRING;
    private AsyncResponseManager m_async_response_manager = null;
    private SubscriptionList m_subscriptions = null;
    private String m_mds_topic_root = null;
    private TypeDecoder m_type_decoder = null;
    private boolean m_unified_format_enabled = false;
    protected boolean m_auto_subscribe_to_obs_resources = false;
    private String m_mds_request_tag = null;
    
    // keys used to differentiate between data from CoAP observations and responses from CoAP commands 
    protected String m_observation_key = "observation";             // legacy: "observation", unified: "notify"
    protected String m_cmd_response_key = "cmd-response";           // common for both legacy and unified 

    // default constructor
    public Processor(Orchestrator orchestrator, String suffix) {
        super(orchestrator.errorLogger(), orchestrator.preferences());
        this.m_orchestrator = orchestrator;
        this.m_def_domain = orchestrator.preferences().valueOf("mds_def_domain", suffix);
        this.m_json_parser = orchestrator.getJSONParser();
        this.m_json_generator = orchestrator.getJSONGenerator();
        
        // allocate our AsyncResponse orchestrator
        this.m_async_response_manager = new AsyncResponseManager(orchestrator);
        
        // our suffix
        this.m_suffix = suffix;
        
        // initialize subscriptions
        this.m_subscriptions = new SubscriptionList(orchestrator.errorLogger(), orchestrator.preferences());
        
        // set our domain
        this.m_mds_domain = orchestrator.getDomain();
        
        // initial topic root
        this.m_mds_topic_root = "";
        
        // initialize the auto subscription to OBS resources
        this.initAutoSubscribe(null);
        
        // initialize the mDS request tag
        this.initRequestTag(null);
        
        // allocate our TypeDecoder
        this.m_type_decoder = new TypeDecoder(orchestrator.errorLogger(), orchestrator.preferences());

        // Handle the remapping of empty strings so that our JSON parsers wont complain...
        this.m_empty_string = orchestrator.preferences().valueOf("mds_bridge_empty_string", suffix);
        if (this.m_empty_string == null || this.m_empty_string.length() == 0) {
            this.m_empty_string = Processor.DEFAULT_EMPTY_STRING;
        }
        
        // unified format enabled or disabled
        this.m_unified_format_enabled = orchestrator.preferences().booleanValueOf("unified_format_enabled", this.m_suffix);
        if (this.m_unified_format_enabled == true) {
            this.errorLogger().warning("Unified Bridge Format ENABLED");
            this.m_observation_key = "notify";
        }
        else {
            this.errorLogger().warning("Unified Bridge Format DISABLED");
        }
    }
    
    // initialize the mDS request tag
    protected void initRequestTag(String res_name) {
        this.m_mds_request_tag = "/request";
        
        // mDS Request TAG
        if (res_name != null && res_name.length() > 0) {
            this.m_mds_request_tag = this.orchestrator().preferences().valueOf(res_name, this.m_suffix);
            if (this.m_mds_request_tag != null) {
                this.m_mds_request_tag = "/" + this.m_mds_request_tag;
            }
        }
    }
    
    // get the request tag
    protected String getRequestTag() {
        return this.m_mds_request_tag;
    }
    
    // initialize auto OBS subscriptions
    protected void initAutoSubscribe(String res_name) {
        // default
        this.m_auto_subscribe_to_obs_resources = false;
        
        if (res_name != null && res_name.length() > 0) {
            boolean res_value = this.orchestrator().preferences().booleanValueOf(res_name,this.m_suffix);
            if (res_value != this.m_auto_subscribe_to_obs_resources) {
                this.m_auto_subscribe_to_obs_resources = res_value;
            }
        }
    }
    
    // initialize the topic root...
    protected void initTopicRoot(String pref) {
        String topic_root = this.preferences().valueOf(pref,this.m_suffix);
        if (topic_root != null && topic_root.length() > 0) {
            this.m_mds_topic_root = topic_root;
        }
    }

    // get our topic root
    protected String getTopicRoot() {
        if (this.m_mds_topic_root == null) {
            return "";
        }
        return this.m_mds_topic_root;
    }
    
    // subscriptions list
    protected SubscriptionList subscriptionsList() {
        return this.m_subscriptions;
    }

    // get the AsyncResponseManager
    protected AsyncResponseManager asyncResponseManager() {
        return this.m_async_response_manager;
    }
    
     // get TypeDecoder if needed
    protected TypeDecoder fundamentalTypeDecoder() {
        return this.m_type_decoder;
    }

    // unified format enabled
    protected boolean unifiedFormatEnabled() {
        return this.m_unified_format_enabled;
    }
    
    // get the endpoint type from the endpoint name
    protected String getEndpointTypeFromEndpointName(String ep_name) {
        String ep_type = this.subscriptionsList().endpointTypeFromEndpointName(ep_name);
        if (ep_type != null) {
            return ep_type;
        }
        return null;
    }
    
    // not an observation or a new_registration...
    private boolean isNotObservationOrNewRegistration(String topic) {
        if (topic != null) {
            return (topic.contains(this.m_observation_key) == false && topic.contains("new_registration") == false);
        }
        return false;
    }
    
    // test to check if a topic is requesting endpoint resource discovery
    protected boolean isEndpointResourcesDiscovery(String topic) {
        boolean is_discovery = false;

        String request_topic = this.createEndpointResourceRequest();
        if (topic != null && topic.contains(request_topic) == true) {
            if (this.isNotObservationOrNewRegistration(topic) == true) {
                is_discovery = true;
            }
        }

        // DEBUG
        this.errorLogger().info("isEndpointResourcesDiscovery: topic: " + topic + " is: " + is_discovery);
        return is_discovery;
    }
    
    // test to check if a topic is requesting endpoint resource itself
    protected boolean isEndpointResourceRequest(String topic) {
        boolean is_endpoint_resource_request = false;
        if (this.isEndpointResourcesDiscovery(topic) == true) {
            // get the resource URI
            String resource_uri = this.getResourceURIFromTopic(topic);

            // see what we have
            if (resource_uri != null && resource_uri.length() > 0) {
                if (this.isNotObservationOrNewRegistration(topic) == true) {
                    is_endpoint_resource_request = true;
                }
            }
        }

        // DEBUG
        this.errorLogger().info("isEndpointResourceRequest: topic: " + topic + " is: " + is_endpoint_resource_request);
        return is_endpoint_resource_request;
    }
    
    // test to check if a topic is requesting endpoint discovery
    protected boolean isEndpointDiscovery(String topic) {
        boolean is_discovery = false;

        String request_topic = this.createEndpointDiscoveryRequest();
        if (topic != null && topic.equalsIgnoreCase(request_topic) == true) {
            is_discovery = true;
        }

        // DEBUG
        this.errorLogger().info("isEndpointDiscovery: topic: " + topic + " is: " + is_discovery);
        return is_discovery;
    }

    // test to check if a topic is requesting endpoint resource subscription actions
    protected boolean isEndpointNotificationSubscriptionRequest(String topic) {
        boolean is_endpoint_notification_subscription = false;

        // simply check for "request/subscriptions"
        if (topic.contains("request/subscriptions")) {
            is_endpoint_notification_subscription = true;
        }

        // DEBUG
        this.errorLogger().info("isEndpointNotificationSubscription: topic: " + topic + " is: " + is_endpoint_notification_subscription);
        return is_endpoint_notification_subscription;
    }
    
    // get the resource URI from the topic (request topic sent) 
    // format: <topic_root>/request/endpoints/<ep_type>/<endpoint name>/<URI> POSITION SENSITIVE
    private String getResourceURIFromTopic(String topic, String ep_type, String ep_name) {
        String modified_topic = this.removeRequestTagFromTopic(topic);  // strips <topic_root>/request/endpoints/ POSITION SENSITIVE
        return modified_topic.replace(ep_type + "/" + ep_name, "");      // strips <ep_type>/<endpoint name> POSITION SENSITIVE
    }

    // strip off the request TAG
    // mbed/request/<ep_type>/<endpoint>/<URI> --> <ep_type>/<endpoint>/<URI> POSITION SENSITIVE
    protected String removeRequestTagFromTopic(String topic) {
        if (topic != null) {
            String stripped = topic.replace(this.getTopicRoot() + this.getDomain() + this.getRequestTag() + "/", "");
            this.errorLogger().info("removeRequestTagFromTopic: topic: " + topic + " stripped: " + stripped);
            return stripped;
        }
        return null;
    }

    // response is an AsyncResponse?
    protected boolean isAsyncResponse(String response) {
        return (response.contains("\"async-response-id\":") == true);
    }
    
    // get the endpoint name from the topic (request topic sent) 
    // format: <topic_root>/request/endpoints/<ep_type>/<endpoint name>/<URI> POSITION SENSITIVE
    private String getEndpointNameFromTopic(String topic) {
        String modified_topic = this.removeRequestTagFromTopic(topic); // strips <topic_root>/request/endpoints/ 
        String[] items = modified_topic.split("/");
        if (items.length >= 2 && items[1].trim().length() > 0) { // POSITION SENSITIVE
            return items[1].trim();                              // POSITION SENSITIVE
        }
        return null;
    }
    
    // get the endpoint type from the topic (request topic sent) 
    // format: <topic_root>/request/endpoints/<ep_type>/<endpoint name>/<URI> POSITION SENSITIVE
    private String getEndpointTypeFromTopic(String topic) {
        String modified_topic = this.removeRequestTagFromTopic(topic); // strips <topic_root>/request/endpoints/ 
        String[] items = modified_topic.split("/");
        if (items.length >= 1 && items[0].trim().length() > 0) { // POSITION SENSITIVE
            return items[0].trim();                              // POSITION SENSITIVE
        }
        return null;
    }

    // get the resource URI from the topic (request topic sent) 
    // format: <topic_root>/request/endpoints/<ep_type>/<endpoint name>/<URI>
    protected String getResourceURIFromTopic(String topic) {
        // get the endpoint type 
        String ep_type = this.getEndpointTypeFromTopic(topic);

        // get the endpoint name
        String ep_name = this.getEndpointNameFromTopic(topic);

        // get the URI...
        return this.getResourceURIFromTopic(topic, ep_type, ep_name);
    }
    
    // returns  /mbed/<domain>/<qualifier
    protected String createBaseTopic(String qualifier) {
        return this.getTopicRoot() + this.getDomain() + "/" + qualifier;
    }

    // returns /mbed/<domain>/new_registration/<ep_type>/<endpoint>
    protected String createNewRegistrationTopic(String ep_type, String ep_name) {
        return this.createBaseTopic("new_registration") + "/" + ep_type + "/" + ep_name;
    }

    // returns /mbed/<domain>/discover
    protected String createEndpointDiscoveryRequest() {
        return this.createBaseTopic("discover");
    }

    // returns /mbed/<domain>/request/<ep_type>
    protected String createEndpointResourceRequest() {
        return this.createEndpointResourceRequest(null);
    }

    // returns /mbed/<domain>/request/<ep_type>
    protected String createEndpointResourceRequest(String ep_type) {
        String suffix = "";
        if (ep_type != null && ep_type.length() > 0) {
            suffix = "/" + ep_type;
        }
        return this.createBaseTopic("request") + suffix;
    }
    
    // returns mbed/<domain>/observation/<ep_type>/<endpoint>/<uri>
    protected String createObservationTopic(String ep_type, String ep_name, String uri) {
        return this.createBaseTopic(this.m_observation_key) + "/" + ep_type + "/" + ep_name + uri;
    }

    // returns mbed/<domain>/response/<ep_type>/<endpoint>/<uri>
    protected String createResourceResponseTopic(String ep_type, String ep_name, String uri) {
        return this.createBaseTopic(this.m_cmd_response_key) + "/" + ep_type + "/" + ep_name + uri;
    }
    
    // get the observability of a given resource
    protected boolean isObservableResource(Map resource) {
        String obs_str = (String) resource.get("obs");
        return (obs_str != null && obs_str.equalsIgnoreCase("true"));
    }

    // parse the de-registration body
    protected String[] parseDeRegistrationBody(Map body) {
        List list = (List) body.get("de-registrations");
        if (list != null && list.size() > 0) {
            return list.toString().replace("[", "").replace("]", "").replace(",", " ").split(" ");
        }
        list = (List) body.get("registrations-expired");
        if (list != null && list.size() > 0) {
            return list.toString().replace("[", "").replace("]", "").replace(",", " ").split(" ");
        }
        return new String[0];
    }

    // jsonParser is broken with empty strings... so we have to fill them in with spaces.. 
    private String replaceEmptyStrings(String data) {
        if (data != null) {
            return data.replaceAll("\"\"", "\"" + this.m_empty_string + "\"");
        }
        return data;
    }

    // parse the JSON...
    protected Object parseJson(String json) {
        Object parsed = null;
        String modified_json = "";
        try {
            modified_json = this.replaceEmptyStrings(json);
            if (json != null && json.contains("{") && json.contains("}")) {
                parsed = this.jsonParser().parseJson(modified_json);
            }
        }
        catch (Exception ex) {
            this.orchestrator().errorLogger().critical("JSON parsing exception for: " + modified_json + " message: " + ex.getMessage(), ex);
            parsed = null;
        }
        return parsed;
    }

    // protected getters/setters...
    protected JSONParser jsonParser() {
        return this.m_json_parser;
    }

    // get the JSON generator
    protected JSONGenerator jsonGenerator() {
        return this.m_json_generator;
    }

    // get the orchestrator
    protected Orchestrator orchestrator() {
        return this.m_orchestrator;
    }

    // set our MDS Domain
    protected void setDomain(String mds_domain) {
        this.m_mds_domain = mds_domain;
    }

    // get our MDS Domain
    protected String getDomain() {
        return this.getDomain(false);
    }

    // get our MDS Domain (defaulted)
    protected String getDomain(boolean show_default) {
        if (this.m_mds_domain != null) {
            return "/" + this.m_mds_domain;
        }
        if (show_default == true) {
            return "/" + this.m_def_domain;
        }
        return "";
    }

    // strip array values... not needed
    protected String stripArrayChars(String json) {
        return json.replace("[", "").replace("]", "");
    }
    
    // attempt a json parse... 
    protected Map tryJSONParse(String payload) {
        HashMap<String, Object> result = new HashMap<>();
        try {
            result = (HashMap<String, Object>) this.orchestrator().getJSONParser().parseJson(payload);
            return result;
        }
        catch (Exception ex) {
            // silent
        }
        return result;
    }
    
    // pull the EndpointName from the message
    protected String getCoAPEndpointName(String message) {
        // expected format: { "path":"/303/0/5850", "new_value":"0", "ep":"mbed-eth-observe", "coap_verb": "get" }
        //this.errorLogger().info("getCoAPValue: payload: " + message);
        Map parsed = this.tryJSONParse(message);
        String val = (String) parsed.get("ep");
        if (val == null || val.length() == 0) {
            val = (String) parsed.get("deviceId");
        }
        return val;
    }

    // pull the CoAP verb from the message
    protected String getCoAPVerb(String message) {
        // expected format: { "path":"/303/0/5850", "new_value":"0", "ep":"mbed-eth-observe", "coap_verb": "get" }
        //this.errorLogger().info("getCoAPValue: payload: " + message);
        Map parsed = this.tryJSONParse(message);
        String val = (String) parsed.get("coap_verb");
        if (val == null || val.length() == 0) {
            val = (String) parsed.get("method");
        }

        // map to lower case
        if (val != null) {
            val = val.toLowerCase();
        }
        return val;
    }
    
    // get the resource URI from the message
    protected String getCoAPURI(String message) {
        // expected format: { "path":"/303/0/5850", "new_value":"0", "ep":"mbed-eth-observe", "coap_verb": "get" }
        //this.errorLogger().info("getCoAPURI: payload: " + message);
        Map parsed = this.tryJSONParse(message);
        String val = (String) parsed.get("path");
        if (val == null || val.length() == 0) {
            val = (String) parsed.get("resourceId");
        }

        // adapt for those variants that have path as "311/0/5850" vs. "/311/0/5850"... 
        if (val != null && val.charAt(0) != '/') {
            // prepend a "/"
            val = "/" + val;
        }
        return val;
    }
    
    // get the resource value from the message
    protected String getCoAPValue(String message) {
        // expected format: { "path":"/303/0/5850", "new_value":"0", "ep":"mbed-eth-observe", "coap_verb": "get" }
        //this.errorLogger().info("getCoAPValue: payload: " + message);
        Map parsed = this.tryJSONParse(message);
        String val = (String) parsed.get("new_value");
        if (val == null || val.length() == 0) {
            val = (String) parsed.get("payload");
            if (val != null) {
                // see if the value is Base64 encoded
                String last = val.substring(val.length() - 1);
                if (val.contains("==") || last.contains("=")) {
                    // value appears to be Base64 encoded... so decode... 
                    try {
                        // DEBUG
                        this.errorLogger().info("getCoAPValue: Value: " + val + " flagged as Base64 encoded... decoding...");

                        // Decode
                        val = new String(Base64.decodeBase64(val));

                        // DEBUG
                        this.errorLogger().info("getCoAPValue: Base64 Decoded Value: " + val);
                    }
                    catch (Exception ex) {
                        // just use the value itself...
                        this.errorLogger().info("getCoAPValue: Exception in base64 decode", ex);
                    }
                }
            }
        }
        return val;
    }
    
    // pull any mDC/mDS REST options from the message (optional)
    protected String getRESTOptions(String message) {
        // expected format: { "path":"/303/0/5850", "new_value":"0", "ep":"mbed-eth-observe", "coap_verb": "get", "options":"noResp=true" }
        //this.errorLogger().info("getCoAPValue: payload: " + message);
        Map parsed = this.tryJSONParse(message);
        return (String) parsed.get("options");
    }
    
    // retrieve a specific element from the topic structure
    protected String getTopicElement(String topic, int index) {
        String element = "";
        String[] parsed = topic.split("/");
        if (parsed != null && parsed.length > index) {
            element = parsed[index];
        }
        
        // map to lower case.. 
        if (element != null) {
            element = element.toLowerCase();
        }
        
        return element;
    }
    
     // create the URI path from the topic
    protected String getURIPathFromTopic(String topic, int start_index) {
        try {
            // split by forward slash
            String tmp_slash[] = topic.split("/");

            // we now re-assemble starting from a specific index
            StringBuilder buf = new StringBuilder();
            for (int i = start_index; tmp_slash.length > 5 && i < tmp_slash.length; ++i) {
                buf.append("/");
                buf.append(tmp_slash[i]);
            }

            return buf.toString();
        }
        catch (Exception ex) {
            // Exception during parse
            this.errorLogger().info("WARNING: getURIPathFromTopic: Exception: " + ex.getMessage());
        }
        return null;
    }
    
    // create the observation
    private String createObservation(String verb, String ep_name, String uri, String value) {
        Map notification = new HashMap<>();

        // needs to look like this:  {"path":"/303/0/5700","payload":"MjkuNzU\u003d","max-age":"60","ep":"350e67be-9270-406b-8802-dd5e5f20","value":"29.75"}    
        notification.put("value", this.fundamentalTypeDecoder().getFundamentalValue(value));
        notification.put("path", uri);
        notification.put("ep", ep_name);

        // add a new field to denote its a GET
        notification.put("coap_verb", verb);

        // Unified Format?
        if (this.unifiedFormatEnabled() == true) {
            notification.put("resourceId", uri);
            notification.put("deviceId", ep_name);
            notification.put("method", verb);
        }

        // we will send the raw CoAP JSON... AWSIoT can parse that... 
        String coap_raw_json = this.jsonGenerator().generateJson(notification);

        // strip off []...
        String coap_json_stripped = this.stripArrayChars(coap_raw_json);

        // encapsulate into a coap/device packet...
        String coap_json = coap_json_stripped;

        // DEBUG
        this.errorLogger().info("createObservation: CoAP notification(" + verb + " REPLY): " + coap_json);

        // return the generic MQTT observation JSON...
        return coap_json;
    }
    
    // default formatter for AsyncResponse replies
    public String formatAsyncResponseAsReply(Map async_response, String verb) {
        // DEBUG
        this.errorLogger().info("formatAsyncResponseAsReply(" + verb + ") AsyncResponse: " + async_response);

        // Handle AsyncReplies that are CoAP GETs
        if (verb != null && verb.equalsIgnoreCase("GET") == true) {
            try {
                // check to see if we have a payload or not...
                String payload = (String) async_response.get("payload");
                if (payload != null) {
                    // trim 
                    payload = payload.trim();

                    // parse if present
                    if (payload.length() > 0) {
                        // Base64 decode
                        String value = Utils.decodeCoAPPayload(payload);

                        // build out the response
                        String uri = this.getURIFromAsyncID((String) async_response.get("id"));
                        String ep_name = this.getEndpointNameFromAsyncID((String) async_response.get("id"));

                        // build out the observation
                        String message = this.createObservation(verb, ep_name, uri, value);

                        // DEBUG
                        this.errorLogger().info("formatAsyncResponseAsReply: Created(" + verb + ") GET observation: " + message + " reply topic: " + async_response.get("reply_topic"));

                        // return the message
                        return message;
                    }
                }
                else {
                    // GET should always have a payload
                    this.errorLogger().warning("formatAsyncResponseAsReply (" + verb + "): GET Observation has NULL payload... Ignoring...");
                }
            }
            catch (Exception ex) {
                // Error in creating the observation message from the AsyncResponse GET reply... 
                this.errorLogger().warning("formatAsyncResponseAsReply(GET): Exception in formatAsyncResponseAsReply(): ", ex);
            }
        }

        // Handle AsyncReplies that are CoAP PUTs
        if (verb != null && verb.equalsIgnoreCase("PUT") == true) {
            try {
                // check to see if we have a payload or not... 
                String payload = (String) async_response.get("payload");
                if (payload != null) {
                    // trim 
                    payload = payload.trim();

                    // parse if present
                    if (payload.length() > 0) {
                        // Base64 decode
                        String value = Utils.decodeCoAPPayload(payload);

                        // build out the response
                        String uri = this.getURIFromAsyncID((String) async_response.get("id"));
                        String ep_name = this.getEndpointNameFromAsyncID((String) async_response.get("id"));

                        // build out the observation
                        String message = this.createObservation(verb, ep_name, uri, value);

                        // DEBUG
                        this.errorLogger().info("formatAsyncResponseAsReply: Created(" + verb + ") PUT Observation: " + message);

                        // return the message
                        return message;
                    }
                }
                else {
                    // no payload... so we simply return the async-id
                    String message = (String) async_response.get("async-id");

                    // DEBUG
                    this.errorLogger().info("formatAsyncResponseAsReply: Created(" + verb + ") PUT Observation: " + message);

                    // return message
                    return message;
                }
            }
            catch (Exception ex) {
                // Error in creating the observation message from the AsyncResponse PUT reply... 
                this.errorLogger().warning("formatAsyncResponseAsReply(PUT): Exception in formatAsyncResponseAsReply(): ", ex);
            }
        }

        // return null message
        return null;
    }
    
    // split AsyncID
    private String[] splitAsyncID(String id) {
        String[] parts = null;

        if (id != null && id.length() > 0) {
            // copy the string
            String tmp = id;

            // loop through and remove key delimiters
            tmp = tmp.replace('#', ' ');
            tmp = tmp.replace('@', ' ');
            tmp = tmp.replace('/', ' ');

            // split
            parts = tmp.split(" ");
        }

        // return the parsed parts
        return parts;
    }
    
    // extract the URI from the async-id
    // format: 1408956550#cc69e7c5-c24f-43cf-8365-8d23bb01c707@decd06cc-2a32-4e5e-80d0-7a7c65b90e6e/303/0/5700
    protected String getURIFromAsyncID(String id) {
        String uri = null;

        // split
        String[] parts = this.splitAsyncID(id);

        // re-assemble the URI
        uri = "/";
        for (int i = 3; i < parts.length; ++i) {
            uri += parts[i];
            if (i < (parts.length - 1)) {
                uri += "/";
            }
        }

        // DEBUG
        this.errorLogger().info("getURIFromAsyncID: URI: " + uri);

        // return the URI
        return uri;
    }

    // extract the endpoint name from the async-id
    // format: 1408956550#cc69e7c5-c24f-43cf-8365-8d23bb01c707@decd06cc-2a32-4e5e-80d0-7a7c65b90e6e/303/0/5700
    protected String getEndpointNameFromAsyncID(String id) {
        // split
        String[] parts = this.splitAsyncID(id);

        // DEBUG
        this.errorLogger().info("getEndpointNameFromAsyncID: endpoint: " + parts[1]);

        // return the endpoint name
        return parts[1];
    }
    
    // OVERRIDE: process a received new registration for AWSIoT
    public void processNewRegistration(Map data) {
        this.processRegistration(data,"registrations");
    }
    
    // OVERRIDE: process mds registrations-expired messages 
    public void processRegistrationsExpired(Map parsed) {
        this.processDeregistrations(parsed);
    }
    
    // record an async response to process later (default)
    public void recordAsyncResponse(String response, String uri, Map ep, AsyncResponseProcessor processor) {
        if (this.asyncResponseManager() != null) {
            this.asyncResponseManager().recordAsyncResponse(response, uri, ep, processor);
        }
    }
    
    //
    // These methods are stubbed out by default... but need to be implemented in derived classes.
    // They are the "responders" to mDS events for devices and initialize/start and stop "listeners"
    // that are appropriate for the peer/3rd Party...(for example MQTT...)
    //
    
    // process a received new registration/registration update/deregistration, 
    protected void processRegistration(Map data, String key) {
        // XXX TO DO 
        this.errorLogger().info("processRegistration(BASE): key: " + key + " data: " + data + " : not implemented");
    }
    
    // process a reregistration
    public void processReRegistration(Map message) {
        // XXX to do
        this.errorLogger().info("processReRegistration(BASE): message: " + message  + " : not implemented");
    }

    // process a deregistration
    public String[] processDeregistrations(Map message) {
        // XXX to do
        this.errorLogger().info("processDeregistrations(BASE): message: " + message  + " : not implemented");
        return null;
    }
    
    // process an observation/notification
    public void processNotification(Map data) {
        // XXXX TO DO
        this.errorLogger().info("processNotification(BASE): data: " + data + " : not implemented");
    }
    
    // intialize a listener for the peer
    public void initListener() {
        // XXX to do
        this.errorLogger().info("initListener(BASE): not implemented");
    }

    // stop the listener for a peer
    public void stopListener() {
        // XXX to do
        this.errorLogger().info("stopListener(BASE): not implemented");
    }
    
    // process & route async response messages (defaulted implementation)
    public void processAsyncResponses(Map data) {
        List responses = (List) data.get("async-responses");
        for (int i = 0; responses != null && i < responses.size(); ++i) {
            this.asyncResponseManager().processAsyncResponse((Map) responses.get(i));
        }
    }
}
