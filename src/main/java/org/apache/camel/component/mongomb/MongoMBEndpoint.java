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

import org.apache.camel.Consumer;
import org.apache.camel.Exchange;
import org.apache.camel.Processor;
import org.apache.camel.Producer;
import org.apache.camel.RuntimeCamelException;
import org.apache.camel.impl.DefaultEndpoint;
import org.apache.camel.spi.Metadata;
import org.apache.camel.spi.UriEndpoint;
import org.apache.camel.spi.UriPath;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@UriEndpoint(scheme = "mongomb", title = "MongoDB Based EventSystem", syntax = "mongomb:connectionBean", consumerClass = MongoMBConsumer.class, label = "nosql, event system")
public class MongoMBEndpoint extends DefaultEndpoint {

	private static final Logger LOG = LoggerFactory
			.getLogger(MongoMBEndpoint.class);

	/**
	 * data needed for exchange interaction
	 */
	private MongoMBConfiguration configuration;

	/**
	 * MongoClient identifier in CAMEL REGISTRY
	 */
	@UriPath
	@Metadata(required = "true")
	private String connectionBean;

	// We are just going to allow fully initialized endpoint instances
	/**
	 * Constructs a partially-initialized MongoMBEndpoint instance. Useful when
	 * creating endpoints manually (e.g., as beans in Spring).
	 */
	// public MongoMBEndpoint() {
	// }

	/**
	 * Constructs a fully-initialized MongoMBEndpoint instance. This is the
	 * preferred method of constructing an object from Java code (as opposed to
	 * Spring beans, etc.).
	 * 
	 * @param endpointUri
	 *            the full URI used to create this endpoint
	 * @param component
	 *            the component that created this endpoint
	 */
	public MongoMBEndpoint(String uri, MongoMBComponent component) {
		super(uri, component);
		LOG.info("+ MongoMB - Endpoint created.");
	}

	/**
	 * A producer not needed
	 */
	@Override
	public Producer createProducer() throws Exception {
		throw new RuntimeCamelException(
				"Cannot produce to a MongoDbConsumerEndpoint: "
						+ getEndpointUri());
	}

	@Override
	public Consumer createConsumer(Processor processor) throws Exception {

		Consumer consumer = new MongoMBConsumer(this, processor);
		// configureConsumer(consumer);
		LOG.debug("\n+ MongoMB - Consumer created.\n");
		return consumer;
	}

	public MongoMBConfiguration getConfiguration() {
		return configuration;
	}

	public void setConfiguration(MongoMBConfiguration configuration) {
		this.configuration = configuration;
	}

	@Override
	public boolean isSingleton() {
		return true;
	}

	@Override
	public Exchange createExchange() {
		return super.createExchange();
	}

	public String getConnectionBean() {
		return connectionBean;
	}

	public void setConnectionBean(String connectionBean) {
		this.connectionBean = connectionBean;
	}
}
