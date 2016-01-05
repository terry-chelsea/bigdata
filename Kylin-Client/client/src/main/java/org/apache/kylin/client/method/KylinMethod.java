package org.apache.kylin.client.method;

import java.io.IOException;
import java.io.InputStream;

import javax.xml.bind.DatatypeConverter;

import org.apache.commons.httpclient.HttpMethod;
import org.apache.commons.httpclient.HttpMethodBase;
import org.apache.kylin.client.KylinClientException;
import org.apache.kylin.jdbc.Driver;
import org.apache.log4j.Logger;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;

public abstract class KylinMethod {
	private Logger logger = Logger.getLogger(KylinMethod.class);
	
	protected String hostname;
	protected int port;
	protected String username;
	protected String password;
	protected ObjectMapper jsonMapper = new ObjectMapper();

	
	protected KylinMethod(String hostname, int port, String username, String password) {
		this.hostname = hostname;
		this.port = port;
		this.username = username;
		this.password = password;
		jsonMapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
	}
	
	protected String toBaseUrl() {
		return String.format("http://%s:%d/", this.hostname, this.port);
	}
	
	protected String toJdbcUrl(String projectName) {
		return String.format(Driver.CONNECT_STRING_PREFIX + "//" + "%s:%d/%s", this.hostname, this.port, projectName);
	}
	
	protected void addHttpHeaders(HttpMethodBase method) {
        method.addRequestHeader("Accept", "application/json, text/plain, */*");
        method.addRequestHeader("Content-Type", "application/json");

        String basicAuth = DatatypeConverter.printBase64Binary((this.username + ":" + this.password).getBytes());
        method.addRequestHeader("Authorization", "Basic " + basicAuth);
    }
	
    protected String getMethodResponseString(HttpMethod method) {
    	String response = null;
    	try {
			response = method.getResponseBodyAsString();
		} catch (IOException e) {
			logger.warn("Get method response string from method " + method + " error!", e);
		}
    	return response;
    }
   
    protected KylinClientException createInputStreamError(String url, Exception e) {
		String errorMsg = "Fetch " + getMethodName() + " Method response to " + url + " error";
		logger.error(errorMsg, e);
		return new KylinClientException(errorMsg, e);
	}
	
	protected KylinClientException createJsonError(String url, String data, Throwable t) {
		String errorMsg = "Deserialize " + getMethodName() + " Method response to " + url + " error";
		if(data != null) {
			errorMsg += ", Input Data : " + data;
		}
		logger.error(errorMsg, t);
		return new KylinClientException(errorMsg, t);
	}
	
	protected KylinClientException cannotFindError(String type, String name) {
		String errorMsg = "Can not find " + type + " named " + name;
		logger.error(errorMsg);
		return new KylinClientException(errorMsg);
	}
	
	protected KylinClientException errorCodeError(String url, int code) {
		String errorMsg = "Error code From " + getMethodName() + " Method to " +
				url + ", Code " + code;
		logger.error(errorMsg);

		return new KylinClientException(errorMsg);
	}
	
	protected KylinClientException executeMethodError(String url, Throwable t) {
		String errorMsg = "Exception happen while executing " + getMethodName() + 
				" Method to " + url;
		logger.error(errorMsg, t);

		return new KylinClientException(errorMsg, t);
	}
	
	protected abstract String getMethodName();
}
