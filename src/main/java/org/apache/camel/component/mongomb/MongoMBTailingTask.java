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

import java.util.concurrent.TimeUnit;

import org.apache.camel.Exchange;
import org.apache.camel.component.mongomb.exceptions.CamelMongoMBException;
import org.apache.camel.component.mongomb.exceptions.CappedCollectionRequiredException;
import org.apache.camel.component.mongomb.exceptions.ConsumerChangedItsStateToNotStartedException;
import org.bson.Document;
import org.bson.types.ObjectId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.mongodb.CursorType;
import com.mongodb.MongoException;
import com.mongodb.MongoQueryException;
import com.mongodb.MongoSocketException;
import com.mongodb.ReadPreference;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Filters;

/**
 * Task responsible for fetching the events from the events (capped) collection.
 */
public class MongoMBTailingTask implements Runnable {

	private static final Logger LOG = LoggerFactory
			.getLogger(MongoMBTailingTask.class);

	private final MongoMBConsumer consumer;

	/**
	 * Collection storing events being published by systems interacting. Should
	 * be capped, have a size, max.
	 */
	// TODO: WiredTIGER StorageEngine(Concurrent Writes-READS). No
	// compression(capped, size is fixed)
	private MongoCollection<Document> eventsCollection;

	/*
	 * PERSISTENT TRACKER.
	 * 
	 * If proper persistent tracking configuration is set, tracker != null &&
	 * cursorRegenerationDelay != 0
	 */
	
	private MongoMBPersistentTrackingManager tracker;
	private long cursorRegenerationDelay;
	private ObjectId lastTrackedId = null;

	public MongoMBTailingTask(MongoMBConsumer consumer) {
		
		this.consumer = consumer;
		
		MongoMBConfiguration configuration = getConfiguration();
		MongoDatabase mongoDatabase = configuration.getMongoDatabase();
		String collectionName = configuration.getCollection();
		eventsCollection = mongoDatabase.getCollection(collectionName);
		
		// Check eventsCollection is a capped collection...
		final Document collStatsCommand = new Document("collStats",
				collectionName);
		Boolean isCapped = mongoDatabase.runCommand(collStatsCommand,
				ReadPreference.primary()).getBoolean("capped");
		if (!isCapped) {
			throw new CappedCollectionRequiredException(
					"Tailable cursors are only compatible with capped collections, and collection "
							+ collectionName + " is not capped.");
		}

		// Persistent TRACKING ENABLED? If enabled tracker != null &&
		// cursorRegenerationDelay != 0
		if (configuration.isPersistentTrackingEnable()) {
			tracker = new MongoMBPersistentTrackingManager(configuration);
			cursorRegenerationDelay = getConfiguration()
					.getPersistentTrackingConfiguration()
					.getCursorRegenerationDelay();
			if (cursorRegenerationDelay == 0) {
				cursorRegenerationDelay = MongoMBPersistentTrackingConfiguration.DEFAULT_CURSOR_REGENERATION_DELAY;
			}
		}
	}

	/**
	 * Fetch last tracked id from db.
	 */
	public void init() {
		MongoMBConfiguration configuration = getConfiguration();
		if (configuration.isPersistentTrackingEnable()) {
			lastTrackedId = tracker.fetchLastTrackedEventId();
		}
	}

	/**
	 * Builds a tailable & awaitdata cursor to fetch events from the events
	 * collection.
	 */
	public MongoCursor<Document> buildCursor() {

		if (lastTrackedId == null) {
			return eventsCollection.find().sort(new Document("$natural", 1))
					.cursorType(CursorType.TailableAwait).iterator();
		} else {

			// we know we processed the event with "_id": lastTrackedId
			// We are interested in the first event with id greater than
			// lastTrackedId
			return eventsCollection

			.find(Filters.gt("_id", lastTrackedId))

			.sort(new Document("$natural", 1))
					.cursorType(CursorType.TailableAwait).iterator();
		}
	}

	/**
	 * TAILING TASK:
	 * 
	 */
	@Override
	public void run() {

		try {

			while (true) {

				// Work with cursor until lost or
				// consumer changes its state to not started.

				// hasNext throws IllegalStateException when cursor is closed
				// (not by consumer)
				MongoCursor<Document> cursor = buildCursor();
				if (cursor.hasNext()) {

					// throws ConsumerChangedStateToNotStarted
					iterateCursor(cursor);

					// Cursor was LOST

					// a delay to generate a cursor can be provided by
					// configuration
					// wait to regenerate another cursor
					applyDelayToGenerateCursor();
				} else {

					// hasNext returned with no data
					LOG.debug("Cursor returned no data");
					if (cursor != null)
						cursor.close();
				}

			} // while(keepRunning) block
		} catch (IllegalStateException e) {

			// Cursor was closed not by consumer
			// Consumer changed its state
			LOG.info("+ MONGOESB: Cursor was closed");

		} catch (ConsumerChangedItsStateToNotStartedException e) {
			// Consumer changed its state
			LOG.info("+ MONGOESB: Consumer changed its state");
		} catch (MongoException e) {
			LOG.info("+ MONGOESB: MongoException - STOP EXECUTION - {}",
					e.toString());
			throw new CamelMongoMBException(e);
		} catch (RuntimeException e) {
			throw new CamelMongoMBException(e);
		} finally {
			LOG.info("+ MONGOESB - STOP TAILING TASK");
		}

	} // run

	/**
	 * Cursor LOGIC. A cursor is built and can be iterated until lost or until
	 * consumer changes its state to a not started state. throws
	 * CamelMongoMBConsumerNotStarted to sign a change in consumer states.
	 * 
	 * @throws ConsumerChangedItsStateToNotStartedException
	 *             to signal consumer state changed from started
	 */
	private void iterateCursor(final MongoCursor<Document> cursor) {

		// stores the id of the last event fetched by this cursor...
		ObjectId lastProcessedId = null;

		try {

			while (true) {

				// Is there a new document to be processed?
				Document next = cursor.tryNext();

				if (next == null) {

					// It is likely we come from a burst of processing ...

					// This is a chance to persist last processed
					// id...go for it

					if (tracker != null && lastProcessedId != null) {

						tracker.persistLastTrackedEventId(lastProcessedId);
						lastTrackedId = lastProcessedId;
					}

					// Wait for a new event to be processed
					if (!cursor.hasNext()) {
						LOG.debug("INNER has NEXT returned no data");
						cursor.close();
					}

				} else {

					// There is an event to be processed

					Exchange exchange = getEndpoint().createExchange();
					exchange.getIn().setBody(next);

					try {
						// LOG.trace("Sending exchange: {}, ObjectId: {}",
						// exchange, next.get("_id"));
						consumer.getProcessor().process(exchange);
						lastProcessedId = next.getObjectId("_id");
					} catch (Exception e) {
						// exceptions in processor chain
						exchange.setException(e);
					}
				}

				// Check whether to keep execution
				if (!consumerIsStarted())
					throw new ConsumerChangedItsStateToNotStartedException(
							"Cursor Changed its state to not started");
			} // while

		} catch (MongoSocketException e) {
			// The cursor was closed
			LOG.error("\n\nMONGOESB - NETWORK problems: Server Address: {}", e
					.getServerAddress().toString());

			// Not recoverable. Do not regenerate the cursor
			throw new CamelMongoMBException(String.format(
					"Network Problemns detected. Server address: %s", e
							.getServerAddress().toString()));
		} catch (MongoQueryException e) {
			// MongoCursorNotFoundException
			// The cursor was closed
			// Recoverable: Do regenerate the cursor
			LOG.info("Cursor {} has been closed.");
		} catch (IllegalStateException e) {
			// .hasNext(): Cursor was closed by other THREAD (consumer
			// cleaningup)?)
			// Recoverable. Do regenerate the cursor.
			LOG.info("Cursor being iterated was closed\n{}", e.toString());
		} catch (ConsumerChangedItsStateToNotStartedException e) {
			// Not recoverable: Do not regenerate the cursor.
			throw e;
		} finally {

			// persist tracking state
			if (tracker != null && lastProcessedId != null) {
				tracker.persistLastTrackedEventId(lastProcessedId);
				lastTrackedId = lastProcessedId;
			}

			// Cleanup resources.
			if (cursor != null)
				cursor.close();
		}
	}

	/**
	 * Tailing task should run only when consumer state is STARTED. Otherwise,
	 * should be close.
	 * 
	 * @return true if tailing task should be running.
	 */
	public boolean consumerIsStarted() {
		return getConsumer().isStarted();
	}

	private void applyDelayToGenerateCursor() {

		if (cursorRegenerationDelay != 0) {
			try {
				TimeUnit.MILLISECONDS.sleep(cursorRegenerationDelay);
			} catch (InterruptedException e) {
				LOG.error("Thread was interrupted", e);
				Thread.currentThread().interrupt();
			}
		}
	}

	public MongoMBConsumer getConsumer() {
		return consumer;
	}

	public MongoMBEndpoint getEndpoint() {
		return getConsumer().getEndpoint();
	}

	public MongoMBConfiguration getConfiguration() {
		return getEndpoint().getConfiguration();
	}
}
