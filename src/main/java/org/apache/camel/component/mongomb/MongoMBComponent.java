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

import java.util.Map;

import org.apache.camel.CamelContext;
import org.apache.camel.Endpoint;
import org.apache.camel.impl.UriEndpointComponent;
import org.apache.camel.util.CamelContextHelper;
import org.apache.camel.util.IntrospectionSupport;
import org.apache.camel.util.URISupport;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.mongodb.MongoClient;

/**
 * Represents the component that manages {@link MongoMBEndpoint}s.
 */
public class MongoMBComponent extends UriEndpointComponent {

	private static final Logger LOG = LoggerFactory
			.getLogger(MongoMBComponent.class);

	public MongoMBComponent() {
		super(MongoMBEndpoint.class);
	}

	public MongoMBComponent(CamelContext context) {
		super(context, MongoMBEndpoint.class);
	}

	/**
	 * Endpoints factory
	 */
	@Override
	protected Endpoint createEndpoint(String uri, String remaining,
			Map<String, Object> parameters) throws Exception {

		LOG.trace(
				"Creating endpoint uri=[{}], path=[{}], parameters=[{}]",
				new Object[] { URISupport.sanitizeUri(uri),
						URISupport.sanitizePath(remaining), parameters });

		// Set configuration based on uri parameters
		MongoMBConfiguration config = new MongoMBConfiguration();
		setProperties(config, parameters);

		// Set persistent configuration based on persistent parameters
		Map<String, Object> tailTrackingProperties = IntrospectionSupport
				.extractProperties(parameters, "persistent.");
		if (tailTrackingProperties != null && !tailTrackingProperties.isEmpty()) {

			MongoMBPersistentTrackingConfiguration ttConfig = new MongoMBPersistentTrackingConfiguration();
			setProperties(ttConfig, tailTrackingProperties);
			config.setPersistentTrackingConfiguration(ttConfig);
		}

		LOG.debug("Looking up in CAMEL REGISTRY for bean referenced by: {}",
				URISupport.sanitizePath(remaining));
		// Throws NoSuchBeanException -> Let it go
		MongoClient mongoClient = CamelContextHelper.mandatoryLookup(
				getCamelContext(), remaining, MongoClient.class);
		config.setMongoClient(mongoClient);

		// Before the endpoint is built check configuration is valid
		config.isValid();
		
		// Notify persistence is ENABLED or NOT. 
		StringBuffer m = new StringBuffer(
				"\n+ MongoMB - Valid Configuration\n+ MongoMB - Persistent tracking:");
		if (config.isPersistentTrackingEnable())
			m.append(" ENABLED.\n");
		else
			m.append(" DISABLED.\n");
		LOG.info(m.toString());

		// Component is an Endpoint factory. So far, just one Endpoint type.
		// Endpoint construction and configuration.
		MongoMBEndpoint endpoint = new MongoMBEndpoint(uri, this);
		endpoint.setConfiguration(config);
		endpoint.setConnectionBean(remaining);

		return endpoint;
	}
}
