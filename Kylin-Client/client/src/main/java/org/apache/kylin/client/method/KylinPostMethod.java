package org.apache.kylin.client.method;

import java.io.IOException;

import org.apache.commons.httpclient.HttpClient;
import org.apache.commons.httpclient.methods.PostMethod;
import org.apache.commons.httpclient.methods.StringRequestEntity;
import org.apache.log4j.Logger;

public class KylinPostMethod extends KylinMethod {
	private Logger logger = Logger.getLogger(KylinPostMethod.class);
	private HttpClient httpClient = null;
	
	public KylinPostMethod(HttpClient httpClient, String hostname, int port, String username, String password) {
		super(hostname, port, username, password);
		this.httpClient = httpClient;
	}
	
    public boolean auth() throws IOException {
        PostMethod post = new PostMethod(toBaseUrl() + "/kylin/api/user/authentication");
        addHttpHeaders(post);
        StringRequestEntity requestEntity = new StringRequestEntity("{}", "application/json", "UTF-8");
        post.setRequestEntity(requestEntity);

        httpClient.executeMethod(post);

        if (post.getStatusCode() != 200 && post.getStatusCode() != 201) {
            logger.error("Authentication to kylin server response code " + post.getStatusCode() + 
            		", response string : " + getMethodResponseString(post));
            return false;
        }
        return true;
    }
}
