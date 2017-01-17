
/**
 * @file    Main.java
 * @brief main entry for the connector bridge
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
import com.arm.connector.bridge.servlet.Manager;
import com.arm.connector.bridge.core.ErrorLogger;
import com.arm.connector.bridge.preferences.PreferenceManager;
import com.arm.connector.bridge.servlet.Console;
import com.arm.connector.bridge.servlet.EventsProcessor;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.ssl.SslSocketConnector;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.servlet.ServletHolder;

/**
 * Primary entry point for the connector-bridge Jetty application
 *
 * @author Doug Anson
 */
public class Main {

    public static void main(String[] args) throws Exception {
        ErrorLogger logger = new ErrorLogger();
        PreferenceManager preferences = new PreferenceManager(logger);

        // configure the error logger logging level
        logger.configureLoggingLevel(preferences);

        // initialize the server,,,
        Server server = new Server(preferences.intValueOf("mds_gw_port"));

        ServletContextHandler context = new ServletContextHandler(ServletContextHandler.SESSIONS);
        context.setContextPath(preferences.valueOf("mds_gw_context_path"));
        server.setHandler(context);

        // check for and add SSL support if configured...
        if (preferences.booleanValueOf("mds_gw_use_ssl") == true) {
            // Enable SSL Support
            SslSocketConnector sslConnector = new SslSocketConnector();
            sslConnector.setPort(preferences.intValueOf("mds_gw_port") + 1);
            sslConnector.setHost("0.0.0.0");
            sslConnector.setKeystore("keystore.jks");
            sslConnector.setPassword(preferences.valueOf("mds_gw_keystore_password"));
            server.addConnector(sslConnector);
        }

        Console console = new Console();
        EventsProcessor eventsProcessor = new EventsProcessor();
        final Manager manager = eventsProcessor.manager();
        manager.initListeners();

        // add a shutdown hook
        Runtime.getRuntime().addShutdownHook(
                new Thread() {
            @Override
            public void run() {
                System.out.println("Resetting notification handlers...");
                manager.resetNotifications();

                System.out.println("Stopping Listeners...");
                manager.stopListeners();
            }
        });

        // console 
        context.addServlet(new ServletHolder(console), preferences.valueOf("mds_gw_console_path"));

        // notification events: wildcard for domain inclusion
        context.addServlet(new ServletHolder(eventsProcessor), preferences.valueOf("mds_gw_events_path") + "/*");

        // start
        server.start();

        // set the webhooks for mDS
        manager.initWebhooks();

        // JOIN
        server.join();
    }
}
