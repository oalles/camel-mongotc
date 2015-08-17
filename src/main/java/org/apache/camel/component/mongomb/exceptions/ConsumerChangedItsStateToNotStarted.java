package org.apache.camel.component.mongomb.exceptions;

/**
 * Signs the MongoESBConsumer changed its state to not started, so it is not
 * intented to consume documents from events collection.
 */
public class ConsumerChangedItsStateToNotStarted extends CamelMongoMBException {

	public ConsumerChangedItsStateToNotStarted() {
		// TODO Auto-generated constructor stub
	}

	public ConsumerChangedItsStateToNotStarted(String message, Throwable cause,
			boolean enableSuppression, boolean writableStackTrace) {
		super(message, cause, enableSuppression, writableStackTrace);
		// TODO Auto-generated constructor stub
	}

	public ConsumerChangedItsStateToNotStarted(String message, Throwable cause) {
		super(message, cause);
		// TODO Auto-generated constructor stub
	}

	public ConsumerChangedItsStateToNotStarted(String message) {
		super(message);
		// TODO Auto-generated constructor stub
	}

	public ConsumerChangedItsStateToNotStarted(Throwable cause) {
		super(cause);
		// TODO Auto-generated constructor stub
	}

}
