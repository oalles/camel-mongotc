package es.neivi.camel.mongo.benchmark;

import java.net.UnknownHostException;

import org.apache.camel.builder.RouteBuilder;
import org.apache.camel.component.mongomb.MongoMBPersistentTrackingConfiguration;
import org.apache.camel.spring.javaconfig.SingleRouteCamelConfiguration;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import com.mongodb.MongoClient;

@Configuration
// @ComponentScan("es.neivi.camel.mongo.benchmark.routebuilders")
public class BenchmarkConfiguration extends SingleRouteCamelConfiguration {

	public static final String DB_PERS_DISABLED = "MongoDB-PersistenTracking-Disabled";
	public static final String DB_PERS_ENABLED = "MongoDB-PersistenTracking-Enabled";
	public static final String ESB_PERS_DISABLED = "MongoMB-PersistenTracking-Disabled";
	public static final String ESB_PERS_ENABLED = "MongoMB-PersistenTracking-Enabled";
	public static final String DB_NAME = "eventsms-tests";
	public static final String EVENTS_COLLECTION_NAME = "events";
	public static final String TRACKER_COLLECTION_NAME = "tracker";
	public static final String CONSUMER_ID = "backend-ui";
	public static final long CURSOR_REGENERATION_DELAY = 2000;
	public static final int DOCUMENTS_PER_PRODUCER = 40000;
	public static final int PRODUCERS = 10;

	public static String buildDBDisabledUri() {
		return new StringBuffer(String.format(
				"mongodb:mongoClient?database=%s&collection=%s", DB_NAME,
				EVENTS_COLLECTION_NAME)).append("&tailTrackIncreasingField=_id")
				.toString();
	}

	public static String buildDBEnabledUri() {
		return new StringBuffer(String.format(
				"mongodb:mongoClient?database=%s&collection=%s", DB_NAME,
				EVENTS_COLLECTION_NAME))
				.append("&persistentTailTracking=")
				.append(true)
				.append("&tailTrackIncreasingField=_id")
				.append("&persistentId=")
				.append(CONSUMER_ID)
				.append("&tailTrackCollection=")
				.append(MongoMBPersistentTrackingConfiguration.TRACKER_COLLECTION_NAME)
				.append("&tailTrackField=")
				.append(MongoMBPersistentTrackingConfiguration.LAST_TRACK_ID_FIELD)
				.append("&cursorRegenerationDelay=")
				.append(CURSOR_REGENERATION_DELAY).toString();
	}

	public static String buildESBdisabledUri() {
		return new StringBuffer(String.format(
				"mongomb:mongoClient?database=%s&collection=%s", DB_NAME,
				EVENTS_COLLECTION_NAME)).toString();
	}

	public static String buildESBEnabledUri() {
		return new StringBuffer(String.format(
				"mongomb:mongoClient?database=%s&collection=%s", DB_NAME,
				EVENTS_COLLECTION_NAME)).append("&persistent.consumerId=")
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

				from(buildESBdisabledUri()).routeId(ESB_PERS_DISABLED)
						.autoStartup(false).to("mock:test");

				from(buildESBEnabledUri()).routeId(ESB_PERS_ENABLED)
						.autoStartup(false).to("mock:test");

				from(buildDBDisabledUri()).autoStartup(false).to("mock:test")
						.routeId(DB_PERS_DISABLED);

				from(buildDBEnabledUri())
						// .id("consumer4")
						.autoStartup(false).to("mock:test")
						.routeId(DB_PERS_ENABLED);

			}
		};
	}
}