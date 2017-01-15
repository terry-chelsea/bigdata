package com.netease.impala.client;

import java.lang.reflect.Method;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;

import javax.security.sasl.Sasl;

import org.apache.hive.jdbc.Utils;
import org.apache.hive.jdbc.ZooKeeperHiveClientException;
import org.apache.hive.jdbc.Utils.JdbcConnectionParams;
import org.apache.hive.service.auth.HiveAuthFactory;
import org.apache.hive.service.auth.KerberosSaslHelper;
import org.apache.hive.service.auth.PlainSaslHelper;
import org.apache.hive.service.auth.SaslQOP;
import org.apache.hive.service.cli.thrift.TCLIService;
import org.apache.hive.service.cli.thrift.TCloseSessionReq;
import org.apache.hive.service.cli.thrift.TOpenSessionReq;
import org.apache.hive.service.cli.thrift.TOpenSessionResp;
import org.apache.hive.service.cli.thrift.TProtocolVersion;
import org.apache.hive.service.cli.thrift.TSessionHandle;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ImpalaThriftClient {
	private static final Logger logger = LoggerFactory.getLogger(ThriftClientExample.class);
	private static final String AUTH_QOP = "saslQop";
	private static final String AUTH_PRINCIPAL = "principal";
	private static final String AUTH_TYPE = "auth";
	private static final String AUTH_SIMPLE = "noSasl";
	private static final String SERVICE_DISCOVERY_MODE = "serviceDiscoveryMode";
	private static final String SERVICE_DISCOVERY_MODE_ZOOKEEPER = "zooKeeper";
	private static final String IMPALA_PROXY_CONFIG_NAME = "impala.doas.user";
    private static final int loginTimeout = 30000;
    
	private String url = null;
	private JdbcConnectionParams connParams;
	private Map<String, String> sessConfMap;
	private TSessionHandle sessHandle = null;
	private TProtocolVersion protocol = null;
	private boolean isClosed = true;
	private TTransport transport = null;
	private TCLIService.Iface client = null;
	
	public ImpalaThriftClient(String url) {
		this.url = url;
	}
	
	public TCLIService.Iface getHS2Client() {
		if(this.isClosed == true) {
			throw new IllegalArgumentException("Hive server2 client do not initialized.");
		}
		return this.client;
	}
	
	public void init() throws Exception {
		connParams = Utils.parseURL(url);
		sessConfMap = connParams.getSessionVars();

		transport = openTransport();
		client = new TCLIService.Client(new TBinaryProtocol(transport));
		
	    TOpenSessionReq openReq = new TOpenSessionReq();
	    Map<String, String> sessVars = connParams.getSessionVars();
	    Map<String, String> openConf = new HashMap<String, String>();
	    // switch the database
	    openConf.put("use:database", connParams.getDbName());
	    if (sessVars.containsKey(HiveAuthFactory.HS2_PROXY_USER)) {
	    	openConf.put(IMPALA_PROXY_CONFIG_NAME, sessVars.get(HiveAuthFactory.HS2_PROXY_USER));
	    }
	    openReq.setConfiguration(openConf);
	    if (AUTH_SIMPLE.equals(sessVars.get(AUTH_TYPE))) {
	        openReq.setUsername(sessVars.get("user"));
	        openReq.setPassword(sessVars.get("password"));
	    }
	    
	    TOpenSessionResp openResp = client.OpenSession(openReq);
	    Utils.verifySuccess(openResp.getStatus());
	    sessHandle = openResp.getSessionHandle();
	    protocol = openResp.getServerProtocolVersion();
	    isClosed = false;
	}
	
	public void close() throws SQLException {
		if (!isClosed) {
			TCloseSessionReq closeReq = new TCloseSessionReq(sessHandle);
			try {
				client.CloseSession(closeReq);
			} catch (TException e) {
				throw new SQLException("Error while cleaning up the server resources", e);
			} finally {
				isClosed = true;
				if (transport != null) {
					transport.close();
				}
			}
		}
	}
	
	public TSessionHandle getSessionHandler() {
		return sessHandle;
	}
	
	public TProtocolVersion getProtocolVersion() {
		return protocol;
	}

	private TTransport openTransport() throws Exception {
		String jdbcUriString = connParams.getJdbcUriString();
		Map<String, String> sessConfMap = connParams.getSessionVars();
		TTransport transport = null;
		while (true) {
			try {
				transport = createBinaryTransport(connParams);
				if (!transport.isOpen()) {
					logger.info("Will try to open client transport with JDBC Uri: " + jdbcUriString);
					transport.open();
				}
				break;
			} catch (TTransportException e) {
				logger.info("Could not open client transport with JDBC Uri: " + jdbcUriString);
		        // We'll retry till we exhaust all HiveServer2 uris from ZooKeeper
		        if ((sessConfMap.get(SERVICE_DISCOVERY_MODE) != null)
		        		&& (SERVICE_DISCOVERY_MODE_ZOOKEEPER.equalsIgnoreCase(sessConfMap
		        				.get(SERVICE_DISCOVERY_MODE)))) {
		        	try {
		        		// Update jdbcUriString, host & port variables in connParams
		        		// Throw an exception if all HiveServer2 uris have been exhausted,
		        		// or if we're unable to connect to ZooKeeper.
		        		callMethod(Utils.class, "updateConnParamsFromZooKeeper", connParams);
		        	} catch (ZooKeeperHiveClientException ze) {
		        		throw new SQLException(
		        				"Could not open client transport for any of the Server URI's in ZooKeeper: "
		        						+ ze.getMessage(), " 08S01", ze);
		        	}
		        	// Update with new values
		        	jdbcUriString = connParams.getJdbcUriString();
		        	logger.info("Will retry opening client transport");
		        } else {
		        	throw new SQLException("Could not open client transport with JDBC Uri: " + jdbcUriString
		        			+ ": " + e.getMessage(), " 08S01", e);
		        }
			}
		}
		
		return transport;
	}
	
	private void callMethod(Class<?> clazz, String methodName, Object obj) throws Exception {
		Method method = clazz.getDeclaredMethod(methodName, obj.getClass());
		method.setAccessible(true);
		method.invoke(null, obj);
	}
	
	private TTransport createBinaryTransport(JdbcConnectionParams connParams) 
			throws Exception {
		SaslQOP saslQOP = SaslQOP.AUTH;
		String host = connParams.getHost();
		int port = connParams.getPort();
		// for kerberos
		Map<String, String> saslProps = new HashMap<String, String>();
		TTransport transport = null;
	    if (sessConfMap.containsKey(AUTH_QOP)) {
	    	try {
	    		saslQOP = SaslQOP.fromString(sessConfMap.get(AUTH_QOP));
	    	} catch (IllegalArgumentException e) {
	    		throw new SQLException("Invalid " + AUTH_QOP +
	                " parameter. " + e.getMessage(), "42000", e);
	    	}
	    	saslProps.put(Sasl.QOP, saslQOP.toString());
	    } else {
	    	// If the client did not specify qop then just negotiate the one supported by server
	    	saslProps.put(Sasl.QOP, "auth-conf,auth-int,auth");
	    }
	    saslProps.put(Sasl.SERVER_AUTH, "true");
	    if (sessConfMap.containsKey(AUTH_PRINCIPAL)) {
	          transport = KerberosSaslHelper.getKerberosTransport(
	              sessConfMap.get(AUTH_PRINCIPAL), host,
	              HiveAuthFactory.getSocketTransport(host, port, loginTimeout), saslProps,
	              false);
	    } else {
	    	String tokenStr = null;
	    	if("delegationToken".equalsIgnoreCase(tokenStr)) {
	    		tokenStr = org.apache.hadoop.hive.shims.Utils.getTokenStrForm(HiveAuthFactory.HS2_CLIENT_TOKEN);
	    	}
	    	if(tokenStr != null) {
	    		transport = KerberosSaslHelper.getTokenTransport(tokenStr,
	    				host, HiveAuthFactory.getSocketTransport(host, port, loginTimeout), saslProps);
	    	} else {
	    		String username = sessConfMap.containsKey("user") ? sessConfMap.get("user") :"anonymous";
	    		String passwd = sessConfMap.containsKey("password") ? sessConfMap.get("password") :"anonymous";
	    		transport = HiveAuthFactory.getSocketTransport(host, port, loginTimeout);
	    		transport = PlainSaslHelper.getPlainTransport(username, passwd, transport);
	    	}
	    }
	    
	    if (!transport.isOpen()) {
	    	logger.info("Will try to open client transport with JDBC Uri: " + connParams.getJdbcUriString());
	    	transport.open();
	    }
	    return transport;
	}
}
