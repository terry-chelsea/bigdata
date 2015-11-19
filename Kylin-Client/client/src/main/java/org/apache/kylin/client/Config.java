package org.apache.kylin.client;

import java.util.Properties;

public class Config {
	private String hostname;
	private int port;
	private String database;
	private String executeSql;
	private String executeFile;
	private boolean silent;
	private boolean isVerbose;
	private Properties properties;
	private String username;
	private String password;
	private boolean isSSL;
	
	public String getHostname() {
		return hostname;
	}
	public void setHostname(String hostname) {
		this.hostname = hostname;
	}
	public int getPort() {
		return port;
	}
	public void setPort(int port) {
		this.port = port;
	}
	public String getDatabase() {
		return database;
	}
	public void setDatabase(String database) {
		this.database = database;
	}
	public String getExecuteSql() {
		return executeSql;
	}
	public void setExecuteSql(String executeSql) {
		this.executeSql = executeSql;
	}
	public String getExecuteFile() {
		return executeFile;
	}
	public void setExecuteFile(String executeFile) {
		this.executeFile = executeFile;
	}
	public boolean isSilent() {
		return silent;
	}
	public void setSilent(boolean silent) {
		this.silent = silent;
	}
	
	public Properties getProperties() {
		return properties;
	}
	public void setProperties(Properties properties) {
		this.properties = properties;
	}
	public boolean isVerbose() {
		return isVerbose;
	}
	public void setVerbose(boolean isVerbose) {
		this.isVerbose = isVerbose;
	}
	public String getUsername() {
		return username;
	}
	public void setUsername(String username) {
		this.username = username;
	}
	public String getPassword() {
		return password;
	}
	public void setPassword(String password) {
		this.password = password;
	}
	public boolean isSSL() {
		return isSSL;
	}
	public void setSSL(boolean isSSL) {
		this.isSSL = isSSL;
	}
	
}
