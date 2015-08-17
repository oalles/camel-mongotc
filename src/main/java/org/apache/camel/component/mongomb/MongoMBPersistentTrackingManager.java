/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.camel.component.mongomb;

import org.bson.Document;
import org.bson.types.ObjectId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.model.IndexOptions;
import com.mongodb.client.model.UpdateOptions;

/**
 * Responsible of persistent the id of the last tracked event and fetching the
 * last tracked event id from the db.
 *
 */
public class MongoMBPersistentTrackingManager {

	private static final Logger LOG = LoggerFactory
			.getLogger(MongoMBPersistentTrackingManager.class);

	private final MongoMBConfiguration configuration;
	private MongoCollection<Document> trackerCollection;

	public MongoMBPersistentTrackingManager(MongoMBConfiguration configuration) {

		if (!configuration.isPersistentTrackingEnable())
			throw new IllegalArgumentException(
					"Inconsistence: We expected a MongoESBConfiguration instance, with persistent configuration enabled");

		this.configuration = configuration;
		this.trackerCollection = configuration
				.getMongoDatabase()
				.getCollection(
						MongoMBPersistentTrackingConfiguration.TRACKER_COLLECTION_NAME);

		// Check if it has an INDEX on the field
		// MongoESBPersistentTrackingConfiguration.CONSUMER_ID_FIELD
		MongoCursor<Document> indexesCursor = this.trackerCollection
				.listIndexes().iterator();
		boolean indexExist = false;
		while (indexesCursor.hasNext()) {
			Document index = indexesCursor.next();
			Document key = (Document) index.get("key");
			if (key.containsKey(MongoMBPersistentTrackingConfiguration.CONSUMER_ID_FIELD)) {
				indexExist = true;
				break;
			}
			// LOG.debug("Index: {}", index);
		}
		if (!indexExist) {

			// Option 1: BUILD INDEX
			IndexOptions indexOptions = new IndexOptions().unique(true);
			Document index = new Document(
					MongoMBPersistentTrackingConfiguration.CONSUMER_ID_FIELD,
					1);
			this.trackerCollection.createIndex(index, indexOptions);
			LOG.info("+ MONGOESB - Index built: {}",
					MongoMBPersistentTrackingConfiguration.CONSUMER_ID_FIELD);

			// Option 2. STOP and NOTIFY
			// String m = String
			// .format("Collection: %s should have an index on %s field",
			// MongoESBPersistentTrackingConfiguration.TAILTRACKING_COLLECTION_NAME,
			// MongoESBPersistentTrackingConfiguration.CONSUMER_ID_FIELD);
			// LOG.error(m);
			// throw new CamelMongoESBException(m);
		}

	}

	/**
	 * Insert the _id for a event processed by a consumerId in the persistent
	 * tracking collection
	 * 
	 * @throws com.mongodb.MongoWriteException
	 *             if the write failed due some other failure specific to the
	 *             update command
	 * @throws com.mongodb.MongoWriteConcernException
	 *             if the write failed due being unable to fulfil the write
	 *             concern
	 * @throws com.mongodb.MongoException
	 *             if the write failed due some other failure
	 * @throws java.lang.IllegalArgumentException
	 *             if a null argument was passed to the method
	 */
	public void persistLastTrackedEventId(final ObjectId processedEventId) {

		if (processedEventId == null) {
			String m = "A not null eventId was expected. This show some type of inconsistence in the application?";
			LOG.error(m);
			throw new IllegalArgumentException(m);
		}

		// san index on (CONSUMER-ID)is needed
		Document filter = new Document(
				MongoMBPersistentTrackingConfiguration.CONSUMER_ID_FIELD,
				configuration.getPersistentTrackingConfiguration()
						.getConsumerId());

		Document update = new Document("$set", new Document(
				MongoMBPersistentTrackingConfiguration.LAST_TRACK_ID_FIELD,
				processedEventId));

		// Throws RTE: MongoException, MongoWriteException,
		// MongoWriteConcernException
		
		trackerCollection.updateOne(filter, update,
				new UpdateOptions().upsert(true));
		
		LOG.debug(
				"\n+ MongoESB - Last Event ID persisted: {}.\n",
				processedEventId);
	}

	/**
	 * get the last processed event id associated with the bound consumer task
	 * id.
	 * 
	 * @return null - no tracking information available for current consumer id.
	 * @return last processed event id for the bound consumer id.
	 * 
	 */
	public synchronized ObjectId fetchLastTrackedEventId() {

		// Needs an index on (CONSUMER-ID)
		// Filter by consumer-id
		Document filter = new Document(
				MongoMBPersistentTrackingConfiguration.CONSUMER_ID_FIELD,
				configuration.getPersistentTrackingConfiguration()
						.getConsumerId());

		// We get the last track record for this given consumer
		Document lastRecordByConsumer = trackerCollection.find(filter).first();

		if (lastRecordByConsumer != null) {

			return lastRecordByConsumer
					.getObjectId(MongoMBPersistentTrackingConfiguration.LAST_TRACK_ID_FIELD);
		}

		// none event id persisted for the cosumer id bound to this instance

		return null;
	}

}
