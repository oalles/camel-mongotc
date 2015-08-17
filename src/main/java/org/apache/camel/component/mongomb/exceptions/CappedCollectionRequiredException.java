package org.apache.camel.component.mongomb.exceptions;

/**
 * Signal that a capped collection is requiered to be bound to this endpoint.
 */
public class CappedCollectionRequiredException extends CamelMongoMBException {
	
	public CappedCollectionRequiredException() {
		// TODO Auto-generated constructor stub
	}

	public CappedCollectionRequiredException(String message, Throwable cause,
			boolean enableSuppression, boolean writableStackTrace) {
		super(message, cause, enableSuppression, writableStackTrace);
		// TODO Auto-generated constructor stub
	}

	public CappedCollectionRequiredException(String message, Throwable cause) {
		super(message, cause);
		// TODO Auto-generated constructor stub
	}

	public CappedCollectionRequiredException(String message) {
		super(message);
		// TODO Auto-generated constructor stub
	}

	public CappedCollectionRequiredException(Throwable cause) {
		super(cause);
		// TODO Auto-generated constructor stub
	}

}
