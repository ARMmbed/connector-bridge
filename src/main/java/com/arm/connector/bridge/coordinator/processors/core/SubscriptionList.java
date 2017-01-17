/**
 * @file    SubscriptionList.java
 * @brief mDS subscription list manager
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

import com.arm.connector.bridge.core.BaseClass;
import com.arm.connector.bridge.core.ErrorLogger;
import com.arm.connector.bridge.preferences.PreferenceManager;
import java.util.ArrayList;
import java.util.HashMap;

/**
 * mDS subscription list manager
 *
 * @author Doug Anson
 */
public class SubscriptionList extends BaseClass {

    private ArrayList<HashMap<String, String>> m_subscriptions = null;
    private String m_non_domain = null;

    // constructor
    public SubscriptionList(ErrorLogger error_logger, PreferenceManager preference_manager) {
        super(error_logger, preference_manager);
        this.m_subscriptions = new ArrayList<>();
        this.m_non_domain = this.preferences().valueOf("mds_def_domain");
    }

    // add subscription
    public void addSubscription(String domain, String endpoint, String ep_type, String uri) {
        domain = this.checkAndDefaultDomain(domain);
        if (!this.containsSubscription(domain, endpoint, ep_type, uri)) {
            this.errorLogger().info("Adding Subscription: " + domain + ":" + endpoint + ":" + ep_type + ":" + uri);
            this.m_subscriptions.add(this.makeSubscription(domain, endpoint, ep_type, uri));
        }
    }

    // contains a given subscription?
    public boolean containsSubscription(String domain, String endpoint, String ep_type, String uri) {
        boolean has_subscription = false;
        domain = this.checkAndDefaultDomain(domain);
        HashMap<String, String> subscription = this.makeSubscription(domain, endpoint, ep_type, uri);
        if (this.containsSubscription(subscription) >= 0) {
            has_subscription = true;
        }

        return has_subscription;
    }

    // remove a subscription
    public void removeSubscription(String domain, String endpoint, String ep_type, String uri) {
        domain = this.checkAndDefaultDomain(domain);
        HashMap<String, String> subscription = this.makeSubscription(domain, endpoint, ep_type, uri);
        int index = this.containsSubscription(subscription);
        if (index >= 0) {
            this.errorLogger().info("Removing Subscription: " + domain + ":" + endpoint + ":" + uri);
            this.m_subscriptions.remove(index);
        }
    }

    // contains a given subscription?
    private int containsSubscription(HashMap<String, String> subscription) {
        int index = -1;

        for (int i = 0; i < this.m_subscriptions.size() && index < 0; ++i) {
            if (this.sameSubscription(subscription, this.m_subscriptions.get(i))) {
                index = i;
            }
        }

        return index;
    }

    // compare subscriptions
    private boolean sameSubscription(HashMap<String, String> s1, HashMap<String, String> s2) {
        boolean same_subscription = false;

        // compare contents...
        if (s1.get("domain") != null && s2.get("domain") != null && s1.get("domain").equalsIgnoreCase(s2.get("domain"))) {
            if (s1.get("endpoint") != null && s2.get("endpoint") != null && s1.get("endpoint").equalsIgnoreCase(s2.get("endpoint"))) {
                if (s1.get("ep_type") != null && s2.get("ep_type") != null && s1.get("ep_type").equalsIgnoreCase(s2.get("ep_type"))) {
                    if (s1.get("uri") != null && s2.get("uri") != null && s1.get("uri").equalsIgnoreCase(s2.get("uri"))) {
                        // they are the same
                        same_subscription = true;
                    }
                }
            }
        }

        return same_subscription;
    }

    // make subscription entry 
    private HashMap<String, String> makeSubscription(String domain, String endpoint, String ep_type, String uri) {
        domain = this.checkAndDefaultDomain(domain);
        HashMap<String, String> subscription = new HashMap<>();
        subscription.put("domain", domain);
        subscription.put("endpoint", endpoint);
        subscription.put("ep_type", ep_type);
        subscription.put("uri", uri);
        return subscription;
    }

    // default domain
    private String checkAndDefaultDomain(String domain) {
        if (domain == null || domain.length() <= 0) {
            return this.m_non_domain;
        }
        return domain;
    }

    // get the endpoint type for a given endpoint
    public String endpointTypeFromEndpointName(String endpoint) {
        String ep_type = null;

        for (int i = 0; i < this.m_subscriptions.size() && ep_type == null; ++i) {
            HashMap<String, String> subscription = this.m_subscriptions.get(i);
            if (endpoint != null && endpoint.equalsIgnoreCase(subscription.get("endpoint")) == true) {
                ep_type = subscription.get("ep_type");
            }
        }

        // DEBUG
        this.errorLogger().info("endpointTypeFromEndpointName: endpoint: " + endpoint + " type: " + ep_type);

        // return the endpoint type
        return ep_type;
    }
}
