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

import org.apache.camel.spi.UriParam;
import org.apache.camel.spi.UriParams;

/**
 * Contains all the information related to enable a working persistent tracking
 * system, being able to allow a consmer task to remember the last event it
 * processed.
 */
@UriParams
public class MongoMBPersistentTrackingConfiguration {

	public static final String TRACKER_COLLECTION_NAME = "tracker";
	public static final String LAST_TRACK_ID_FIELD = "last-tracked-id";
	public static final String CONSUMER_ID_FIELD = "consumer-task-id";
	public static final long DEFAULT_CURSOR_REGENERATION_DELAY = 1000;

	/**
	 * Consumer task identifier. It is the only required parameter in order to
	 * enable persistent tracking system.
	 */
	@UriParam
	private String consumerId;

	@UriParam(defaultValue = "1000")
	private long cursorRegenerationDelay = 1000L;

	public String getConsumerId() {
		return consumerId;
	}

	public void setConsumerId(String consumerId) {
		this.consumerId = consumerId;
	}

	public long getCursorRegenerationDelay() {
		return cursorRegenerationDelay;
	}

	public void setCursorRegenerationDelay(long cursorRegenerationDelay) {
		this.cursorRegenerationDelay = cursorRegenerationDelay;
	}
}