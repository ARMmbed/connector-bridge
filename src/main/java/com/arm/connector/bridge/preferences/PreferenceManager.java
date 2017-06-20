/**
 * @file    PreferenceManager.java
 * @brief preferences manager
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
package com.arm.connector.bridge.preferences;

import com.arm.connector.bridge.core.BaseClass;
import com.arm.connector.bridge.core.ErrorLogger;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.Properties;

/**
 * preferences manager
 *
 * @author Doug Anson
 */
public class PreferenceManager extends BaseClass {

    private static final int DEFAULT_INT_VALUE = -1;
    private static final float DEFAULT_FLOAT_VALUE = (float) -1.0;
    private static final String PROPERTY_DEFINE = "config_file";      // passed as -Dconfig_file="../conf/service.properties"
    private static final String DEFAULT_PROPERTIES_FILE = "WEB-INF/classes/service.properties";
    private String m_properties_file = null;

    private Properties m_config_properties = null;        // DB config properties

    public PreferenceManager(ErrorLogger error_logger) {
        super(error_logger, null);
        this.m_properties_file = DEFAULT_PROPERTIES_FILE;
        this.readPreferencesFile();
    }

    public boolean booleanValueOf(String key) {
        return this.booleanValueOf(key, null);
    }

    public boolean booleanValueOf(String key, String suffix) {
        boolean result = false;
        String value = this.valueOf(key, suffix);
        if (value != null && value.length() > 0 && value.equalsIgnoreCase("true")) {
            result = true;
        }
        return result;
    }

    public int intValueOf(String key) {
        return this.intValueOf(key, null);
    }

    public int intValueOf(String key, String suffix) {
        int result = DEFAULT_INT_VALUE;
        String value = this.valueOf(key, suffix);
        try {
            if (value != null && value.length() > 0) {
                result = Integer.parseInt(value);
            }
        }
        catch (NumberFormatException ex) {
            result = DEFAULT_INT_VALUE;
        }
        return result;
    }

    public float floatValueOf(String key) {
        return this.floatValueOf(key, null);
    }

    public float floatValueOf(String key, String suffix) {
        float result = DEFAULT_FLOAT_VALUE;
        String value = this.valueOf(key, suffix);
        try {
            if (value != null && value.length() > 0) {
                result = Float.parseFloat(value);
            }
        }
        catch (NumberFormatException ex) {
            result = DEFAULT_FLOAT_VALUE;
        }
        return result;
    }

    public String valueOf(String key) {
        return this.valueOf(key, null);
    }

    public String valueOf(String key, String suffix) {
        String value = this.m_config_properties.getProperty(this.createKey(key, suffix));

        // DEBUG
        //this.errorLogger().info("Preference: [" + this.createKey(key,suffix) + "] = [" + value + "]");
        // return the value
        return value;
    }

    private String createKey(String key, String suffix) {
        // default
        String full_key = key;

        // look for the suffix... if its there, use it.
        if (suffix != null && suffix.length() > 0) {
            full_key = key + "_" + suffix;
        }

        // special case handling: if "_0", then treat as ""
        if (suffix != null && suffix.length() > 0 && suffix.equalsIgnoreCase("0")) {
            // reset to the key itself only... 
            full_key = key;
        }

        // return the full key
        return full_key;
    }

    private String getAbsolutePath(String file) {
        String fq_file = file;

        try {
            String dir = System.getProperty("user.dir");
            String separator = System.getProperty("file.separator");
            fq_file = dir + separator + file;
            //this.errorLogger().info("getAbsolutePath: dir: " + dir + " file: " + file + " FQ file: " + fq_file);
        }
        catch (Exception ex) {
            this.errorLogger().warning("getAbsolutePath: unable to calculate absolute path for: " + file);
            fq_file = file;
        }
        return fq_file;
    }

    // read the preferences file
    private boolean readPreferencesFile() {
        boolean success = false;

        try {
            this.m_properties_file = System.getProperty(PreferenceManager.PROPERTY_DEFINE, PreferenceManager.DEFAULT_PROPERTIES_FILE);
            success = this.readPreferencesFile(this.getAbsolutePath(this.m_properties_file), false);
            if (!success) {
                this.errorLogger().warning("Unable to read config file: " + this.getAbsolutePath(this.m_properties_file) + " trying: " + PreferenceManager.DEFAULT_PROPERTIES_FILE);
                return this.readPreferencesFile(PreferenceManager.DEFAULT_PROPERTIES_FILE);
            }
        }
        catch (Exception ex) {
            success = this.readPreferencesFile(PreferenceManager.DEFAULT_PROPERTIES_FILE);
        }
        return success;
    }

    // read the DB configuration properties file
    private boolean readPreferencesFile(String file) {
        return this.readPreferencesFile(file, true);
    }

    // read the DB configuration properties file
    private boolean readPreferencesFile(String file, boolean war_internal) {
        boolean success = false;
        FileInputStream fis = null;
        if (this.m_config_properties == null) {
            try {
                this.m_config_properties = new Properties();
                if (war_internal) {
                    this.m_config_properties.load(Thread.currentThread().getContextClassLoader().getResourceAsStream(file));
                }
                else {
                    fis = new FileInputStream(file);
                    this.m_config_properties.load(fis);
                    if (fis != null) {
                        fis.close();
                    }
                }
                this.errorLogger().info("Read configuration file: " + file + " successfully");
                success = true;
            }
            catch (IOException ex) {
                this.errorLogger().critical("Unable to read properties file: " + file);
                this.m_config_properties = null;
            }
        }
        return success;
    }

    public void reload() {
        this.m_config_properties = null;
        this.readPreferencesFile();
    }
}
