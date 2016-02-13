package org.apache.camel.component.mongotc.test;

import java.net.UnknownHostException;

import org.apache.camel.builder.RouteBuilder;
import org.apache.camel.component.mongotc.MongoTCPersistentTrackingConfiguration;
import org.apache.camel.spring.javaconfig.SingleRouteCamelConfiguration;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import com.mongodb.MongoClient;

@Configuration
public class BenchmarkConfiguration extends SingleRouteCamelConfiguration {

    public static final String DB_PERS_DISABLED = "mongodb-PersistenTracking-Disabled";
    public static final String DB_PERS_ENABLED = "mongodb-PersistenTracking-Enabled";
    public static final String MB_PERS_DISABLED = "mongotc-PersistenTracking-Disabled";
    public static final String MB_PERS_ENABLED = "mongotc-PersistenTracking-Enabled";
    public static final String DB_NAME = "eventsms-tests";
    public static final String EVENTS_COLLECTION_NAME = "events";
    public static final String TRACKER_COLLECTION_NAME = "tracker";
    public static final String CONSUMER_ID = "backend-ui";
    public static final long CURSOR_REGENERATION_DELAY = 2000;
    public static final int DOCUMENTS_PER_PRODUCER = 40000;
    public static final int PRODUCERS = 10;

    public static String buildMongoDBTrackingDisabledUri() {
        return new StringBuffer(
                String.format("mongodb:mongoClient?database=%s&collection=%s",
                        DB_NAME, EVENTS_COLLECTION_NAME))
                                .append("&tailTrackIncreasingField=_id")
                                .toString();
    }

    public static String buildMongoDBTrackingBEnabledUri() {
        return new StringBuffer(
                String.format("mongodb:mongoClient?database=%s&collection=%s",
                        DB_NAME, EVENTS_COLLECTION_NAME))
                                .append("&persistentTailTracking=").append(true)
                                .append("&tailTrackIncreasingField=_id")
                                .append("&persistentId=").append(CONSUMER_ID)
                                .append("&tailTrackCollection=")
                                .append(MongoTCPersistentTrackingConfiguration.TRACKER_COLLECTION_NAME)
                                .append("&tailTrackField=")
                                .append(MongoTCPersistentTrackingConfiguration.LAST_TRACK_ID_FIELD)
                                .append("&cursorRegenerationDelay=")
                                .append(CURSOR_REGENERATION_DELAY).toString();
    }

    public static String buildMongoTCTrackingDisabledUri() {
        return new StringBuffer(
                String.format("mongotc:mongoClient?database=%s&collection=%s",
                        DB_NAME, EVENTS_COLLECTION_NAME)).toString();
    }

    public static String buildMongoTCTrackingEnabledUri() {
        return new StringBuffer(
                String.format("mongotc:mongoClient?database=%s&collection=%s",
                        DB_NAME, EVENTS_COLLECTION_NAME))
                                .append("&persistent.consumerId=")
                                .append(CONSUMER_ID)
                                .append("&persistent.cursorRegenerationDelay=")
                                .append(CURSOR_REGENERATION_DELAY).toString();
    }

    @Qualifier("mongoClient")
    @Bean
    public MongoClient mongoClient() throws UnknownHostException {
        return new MongoClient();
    }

    @Override
    public RouteBuilder route() {
        return new RouteBuilder() {

            @Override
            public void configure() throws Exception {
                // Ruta con PERSISTENT Tail Tracking desactivado

                from(buildMongoTCTrackingDisabledUri())
                        .routeId(MB_PERS_DISABLED).autoStartup(false)
                        .to("mock:test");

                from(buildMongoTCTrackingEnabledUri()).routeId(MB_PERS_ENABLED)
                        .autoStartup(false).to("mock:test");

                from(buildMongoDBTrackingDisabledUri()).autoStartup(false)
                        .to("mock:test").routeId(DB_PERS_DISABLED);

                from(buildMongoDBTrackingBEnabledUri())
                        // .id("consumer4")
                        .autoStartup(false).to("mock:test")
                        .routeId(DB_PERS_ENABLED);

            }
        };
    }
}