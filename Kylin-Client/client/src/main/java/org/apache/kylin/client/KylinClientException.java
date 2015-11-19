package org.apache.kylin.client;

public class KylinClientException extends Exception {

	private static final long serialVersionUID = 1L;
	
	public KylinClientException(String message) {
		super(message);
	}
	
	public KylinClientException(String message, Throwable t) {
		super(message, t);
	}
}
