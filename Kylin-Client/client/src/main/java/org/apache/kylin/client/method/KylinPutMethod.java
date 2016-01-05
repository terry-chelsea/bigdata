package org.apache.kylin.client.method;

import java.io.IOException;
import java.io.UnsupportedEncodingException;

import org.apache.commons.httpclient.HttpClient;
import org.apache.commons.httpclient.methods.PutMethod;
import org.apache.commons.httpclient.methods.StringRequestEntity;
import org.apache.kylin.client.KylinClientException;
import org.apache.kylin.client.request.JobBuildRequest;
import org.apache.kylin.job.JobInstance;
import org.apache.log4j.Logger;

import com.fasterxml.jackson.core.JsonProcessingException;

public class KylinPutMethod extends KylinMethod {
	private static Logger logger = Logger.getLogger(KylinPutMethod.class);
	private HttpClient httpClient = null;
	
	public KylinPutMethod(HttpClient client, String hostname, int port, String username,
			String password) {
		super(hostname, port, username, password);
		this.httpClient = client;
	}
	
	public String buildNewSegment(String cubeName, long startTime, long endTime) throws KylinClientException {
		JobBuildRequest request = new JobBuildRequest();
		request.setBuildType("BUILD");
		request.setStartTime(startTime);
		request.setEndTime(endTime);
		
		String url = String.format("%s/kylin/api/cubes/%s/rebuild", toBaseUrl(), cubeName);
		String response = null;
    	try {
        	response = putRequest(url, request);
			JobInstance cubingJob = jsonMapper.readValue(response, JobInstance.class);
			logger.debug("Build cube " + cubingJob + " success...");
			return cubingJob.getId();
		} catch (IOException e) {
			throw createJsonError(url, response, e);
		} 
	}

	protected String putRequest(String url, Object obj) 
    		throws KylinClientException {
    	if(this.httpClient == null)
    		return null;
    	
    	PutMethod put = new PutMethod(url);
    	addHttpHeaders(put);
        if(obj != null) {
	        try {
	        	String param = jsonMapper.writeValueAsString(obj);
	        	logger.debug("Post parameter : " + param);
				put.setRequestEntity(new StringRequestEntity(param, null, "UTF-8"));
			} catch (JsonProcessingException | UnsupportedEncodingException e1) {
				throw createJsonError(url, obj.toString(), e1);
			}
        }

        logger.debug("PUT URL " + url);

        synchronized(httpClient) {
	        try {
	        	httpClient.executeMethod(put);
			} catch (IOException e) {
				throw executeMethodError(url, e);
			}
	
	        if (put.getStatusCode() != 200) {
	        	throw errorCodeError(url, put.getStatusCode());
	        }
	
	        String response = null;
	        try {
	        	response = put.getResponseBodyAsString();
			} catch (IOException e) {
				throw createInputStreamError(url, e);
			}
	        return response;
        }
    }

	@Override
	protected String getMethodName() {
		return "PUT";
	}
}
