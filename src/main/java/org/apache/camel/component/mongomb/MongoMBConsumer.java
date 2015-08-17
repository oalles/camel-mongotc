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

import java.util.concurrent.ExecutorService;

import org.apache.camel.Processor;
import org.apache.camel.impl.DefaultConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The MongoESB consumer consumes messages from a capped collection with a
 * tailabable consumer.
 */
// It is an STATEFUL Service
public class MongoMBConsumer extends DefaultConsumer {

	private static final Logger LOG = LoggerFactory
			.getLogger(MongoMBConsumer.class);

	private MongoMBTailingTask tailingTask;
	private ExecutorService executor;

	public MongoMBConsumer(MongoMBEndpoint endpoint, Processor processor) {
		super(endpoint, processor);
		tailingTask = new MongoMBTailingTask(this);

		// Obtain a reference to a task executor to run the tailing task
		if (executor == null)
			executor = getEndpoint()
					.getCamelContext()
					.getExecutorServiceManager()
					.newFixedThreadPool(this, getEndpoint().getEndpointUri(), 1);
	}

	@Override
	protected void doStart() throws Exception {

		// Pre START logic:

		// starting...
		super.doStart();

		// here this.isStarted()==false

		// fetch lastTrackedId if needed
		tailingTask.init();

		// Start consuming from the cursor.
		executor.execute(tailingTask);

	}

	@Override
	protected void doStop() throws Exception {

		super.doStop();

		if (executor != null)
			executor.shutdown();
	}

	@Override
	protected void doResume() throws Exception {

		// Pre START logic:

		// starting...
		super.doResume();

		// fetch lastTrackedId if needed
		tailingTask.init();

		// Iterate cursor
		executor.execute(tailingTask);
	}

	@Override
	public MongoMBEndpoint getEndpoint() {
		return (MongoMBEndpoint) super.getEndpoint();
	}

	public MongoMBConfiguration getConfiguration() {
		return getEndpoint().getConfiguration();
	}
}
