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
package org.apache.camel.component.mongotc;

import java.util.concurrent.Executor;

import org.apache.camel.Exchange;
import org.apache.camel.Processor;
import org.apache.camel.impl.DefaultConsumer;
import org.bson.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import es.omarall.configuration.MTCConfiguration;
import es.omarall.configuration.MTCPersistentTrackingConfiguration;
import es.omarall.mtc.DocumentHandler;
import es.omarall.mtc.TailingTask;

/**
 * The MongoESB consumer consumes messages from a capped collection with a
 * tailabable consumer.
 */
// It is an STATEFUL Service
public class MongoTCConsumer extends DefaultConsumer implements DocumentHandler {

	private static final Logger LOG = LoggerFactory
			.getLogger(MongoTCConsumer.class);

	private TailingTask tailingTask;
	private Executor executor;

	public MongoTCConsumer(MongoTCEndpoint endpoint, Processor processor) {

		super(endpoint, processor);

		MTCConfiguration mtcContiguration = buildMTCConfigurationFromMongoTCConfiguration(getConfiguration());
		tailingTask = new TailingTask(mtcContiguration);
		tailingTask.setDocumentHandler(this);

	}

	@Override
	protected void doStart() throws Exception {

		// Pre START logic:

		// starting...
		super.doStart();

		// here this.isStarted()==false

		// fetch lastTrackedId if needed
		tailingTask.start();

		// Start consuming from the cursor.
		getExecutor().execute(tailingTask);

	}

	@Override
	protected void doStop() throws Exception {

		super.doStop();

		tailingTask.stop();

		// if (executor != null)
		// executor.shutdown();
	}

	@Override
	public MongoTCEndpoint getEndpoint() {
		return (MongoTCEndpoint) super.getEndpoint();
	}

	public MongoTCConfiguration getConfiguration() {
		return getEndpoint().getConfiguration();
	}

	@Override
	public void handleDocument(Document doc) {

		Exchange exchange = getEndpoint().createExchange();
		exchange.getIn().setBody(doc);
		try {
			this.getProcessor().process(exchange);
		} catch (Exception e) {
			// exceptions in processor chain
			exchange.setException(e);
		}
	}

	public static MTCConfiguration buildMTCConfigurationFromMongoTCConfiguration(
			MongoTCConfiguration mbConfiguration) {

		if (mbConfiguration == null)
			throw new IllegalArgumentException(
					"Not null MongoTCConfiguration expected");

		MTCConfiguration mtcContiguration = new MTCConfiguration();
		mtcContiguration.setCollection(mbConfiguration.getCollection());
		mtcContiguration.setDatabase(mbConfiguration.getDatabase());
		mtcContiguration.setMongoClient(mbConfiguration.getMongoClient());

		MongoTCPersistentTrackingConfiguration pConf = mbConfiguration
				.getPersistentTrackingConfiguration();
		if (pConf != null) {
			MTCPersistentTrackingConfiguration pMtcConfiguration = new MTCPersistentTrackingConfiguration();
			pMtcConfiguration.setConsumerId(pConf.getConsumerId());
			pMtcConfiguration.setCursorRegenerationDelay(pConf
					.getCursorRegenerationDelay());
			mtcContiguration
					.setPersistentTrackingConfiguration(pMtcConfiguration);
		}

		return mtcContiguration;

	}

	public Executor getExecutor() {
		// Obtain a reference to a task executor to run the tailing task
		if (executor == null)
			executor = getEndpoint()
					.getCamelContext()
					.getExecutorServiceManager()
					.newFixedThreadPool(this, getEndpoint().getEndpointUri(), 1);
		return executor;
	}
}
