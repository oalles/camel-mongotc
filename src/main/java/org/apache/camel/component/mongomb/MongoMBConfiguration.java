package org.apache.camel.component.mongomb;

import java.util.concurrent.ExecutorService;

import org.apache.camel.component.mongomb.exceptions.CamelMongoMBException;
import org.apache.camel.spi.UriParam;
import org.apache.camel.spi.UriParams;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.mongodb.MongoClient;
import com.mongodb.client.MongoDatabase;

/**
 * Represents a set of configuration values for a MongoESBEndpoint. Contains all
 * the data needed for exchange interaction.
 */
@UriParams
public class MongoMBConfiguration {

	private static final Logger LOG = LoggerFactory
			.getLogger(MongoMBConfiguration.class);

	/**
	 * If the name of the database is not provided a database named
	 * <code>eventsystemdb</code> will be created
	 */
	public static final String DEFAULT_EVENT_SYSTEM_DB_NAME = "eventsystemdb";

	/**
	 * Default name for the collection that stores all the events published in
	 * the system whether one is not provided
	 */
	public static final String DEFAULT_EVENTS_COLLECTION_NAME = "events";

	/**
	 * Holds the name provided for the database that this component is bound to.
	 * This database stores the data needed to provided a working Mongo ESB.
	 * 
	 * At least there will be one collection in order to store all the events
	 * being published.
	 * 
	 * Eventually, there will be a collection for persistent tracking purposes.
	 * Allowing the consumer tasks to fetch the last event they processed.
	 */
	@UriParam(defaultValue = DEFAULT_EVENT_SYSTEM_DB_NAME)
	private String database;

	/**
	 * Name provided for the collection that store all the events being
	 * published by the systemm. This collections is a capped collection. s
	 */
	@UriParam(defaultValue = DEFAULT_EVENTS_COLLECTION_NAME)
	private String collection;

	/**
	 * Eventually, contains the data needed in order to implement a persistent
	 * tracking system. If this value is null, there is no tracking information
	 * provided, meaning that persistent tracking is going to be disable.
	 */
	private MongoMBPersistentTrackingConfiguration persistentTrackingConfiguration;

	private MongoClient mongoClient;
	private MongoDatabase mongoDatabase;

	public MongoClient getMongoClient() {
		return mongoClient;
	}

	public void setMongoClient(MongoClient mongoClient) {
		this.mongoClient = mongoClient;
	}

	public String getDatabase() {
		if (database == null)
			return DEFAULT_EVENT_SYSTEM_DB_NAME;
		return database;
	}

	public void setDatabase(String database) {
		this.database = database;
	}

	public String getCollection() {
		if (collection == null)
			return DEFAULT_EVENTS_COLLECTION_NAME;
		return collection;
	}

	public void setCollection(String collection) {
		this.collection = collection;
	}

	public MongoDatabase getMongoDatabase() {
		if (mongoDatabase == null) {
			mongoDatabase = mongoClient.getDatabase(database);
		}
		return mongoDatabase;
	}

	public void setMongoDatabase(MongoDatabase mongoDatabase) {
		this.mongoDatabase = mongoDatabase;
	}

	public MongoMBPersistentTrackingConfiguration getPersistentTrackingConfiguration() {
		return persistentTrackingConfiguration;
	}

	public void setPersistentTrackingConfiguration(
			MongoMBPersistentTrackingConfiguration persistentTrackingConfiguration) {
		this.persistentTrackingConfiguration = persistentTrackingConfiguration;
	}

	public static String getDefaultEventSystemDbName() {
		return DEFAULT_EVENT_SYSTEM_DB_NAME;
	}

	public static String getDefaultEventsCollectionName() {
		return DEFAULT_EVENTS_COLLECTION_NAME;
	}

	public boolean isPersistentTrackingEnable() {
		return (persistentTrackingConfiguration != null);
	}

	public void isValid() {
		if (mongoClient == null
				|| (persistentTrackingConfiguration != null && (persistentTrackingConfiguration
						.getConsumerId() == null || persistentTrackingConfiguration
						.getConsumerId().isEmpty()))) {
			String m = "Invalid MongoESBConfiguration. Please check your URI. Remember you need MongoClient instance in Camel Registry, and if persistent tracking configuration enable you a nonempty consumer task id";
			LOG.error(m);
			throw new CamelMongoMBException(m);
		}
	}
}
