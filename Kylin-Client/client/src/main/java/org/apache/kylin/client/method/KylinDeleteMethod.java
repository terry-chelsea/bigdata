package org.apache.kylin.client.method;

import org.apache.commons.httpclient.HttpClient;
import org.apache.log4j.Logger;

public class KylinDeleteMethod extends KylinMethod {
	private static Logger logger = Logger.getLogger(KylinDeleteMethod.class);
	private HttpClient httpClient = null;
	
	public KylinDeleteMethod(HttpClient httpClient, String hostname, int port, String username, String password) {
		super(hostname, port, username, password);
		this.httpClient = httpClient;
	}
}
