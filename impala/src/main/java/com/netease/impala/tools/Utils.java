package com.netease.impala.tools;

import java.sql.Connection;
import java.sql.DriverManager;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.security.UserGroupInformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Utils {
	private static final Logger LOGGER = LoggerFactory.getLogger(Utils.class);
	private static final String HIVE_JDBC_DRIVER_NAME = "org.apache.hive.jdbc.HiveDriver";
	private static final String[] dateFormats = new String[]{null, null, null, null, "yyyy", null, "yyyyMM", "yyyy-MM", 
			"yyyyMMdd", null, "yyyy-MM-dd"};

	public static boolean initKerberos(String keytab, String principal) {
		Configuration conf = new Configuration();
        conf.setBoolean("hadoop.security.authorization", true);
        conf.set("hadoop.security.authentication", "kerberos");
		UserGroupInformation.setConfiguration(conf);
		
		try {
			UserGroupInformation.loginUserFromKeytab(principal, keytab);
		} catch(Exception e) {
			LOGGER.error("login with " + keytab + " principal " + principal + " failed !", e);
			return false;
		}
		return true;
	}
	
	public static Map<String, String> agrsToMap(String[] args, int expected, String errorMsg) {
        LOGGER.info("===Args=== \n{}", Arrays.toString(args));
		if(args.length < expected) {
			LOGGER.error(errorMsg);
			return null;
		}
		Map<String, String> parsedArgs = new HashMap<String, String>();
		for(String arg : args){
			String realArgs = arg.trim();
			int index = realArgs.indexOf("=");
			if(index < 0)
				continue;
			String key = realArgs.substring(0, index);
			String value = realArgs.substring(index + 1, realArgs.length());
			if(parsedArgs.containsKey(key)) {
				parsedArgs.put(key, parsedArgs.get(key) + " " + value);
			} else {
				parsedArgs.put(key, value);
			}
		}
        LOGGER.info("Input Args parsed: {}", parsedArgs);
		return parsedArgs;
	}
	
	public static Date formatDate(String dt, String format) {
		SimpleDateFormat dateFmt = null;
		if(format == null) {
			int len = dt.length();
			format = len >= dateFormats.length ? null : dateFormats[len];
		} 
		if(format == null) {
			LOGGER.warn("Can not find data format for partition {} ." + dt);
			return null;
		}
		dateFmt = new SimpleDateFormat(format);
		
		try {
			Date date = dateFmt.parse(dt);
			return date;
		} catch (ParseException e) {
			LOGGER.warn("Parse date {} with format {} failed.", dt, format);
			return null;
		}
	}
	
	public static HiveMetaStoreClient createMetaStoreClient() throws MetaException {
		HiveConf c = new HiveConf();
	    final HiveMetaStoreClient client = new HiveMetaStoreClient(c);
	    return client;
	}
	
	public static Connection createRemoteClient(String url) throws Exception {
		Class.forName(HIVE_JDBC_DRIVER_NAME);
		Connection conn = DriverManager.getConnection(url);
		return conn;
	}
	
	public static String tempJsonTableName(String table) {
		return String.format("__tmp_%s_json__", table);
	}
	
	public static List<String> splitTableNames(String tableName) {
		List<String> tables = new LinkedList<String>();
		String[] tableNames = tableName.split(",");
		for(String tb : tableNames) {
			tables.add(tb.trim());
		}
		
		return tables;
	}
}
