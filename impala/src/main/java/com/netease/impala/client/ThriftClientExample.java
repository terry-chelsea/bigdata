package com.netease.impala.client;

import java.io.File;
import java.io.IOException;
import java.sql.SQLException;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

import jline.console.ConsoleReader;
import jline.console.history.FileHistory;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.cli.PosixParser;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hive.jdbc.Utils;
import org.apache.hive.service.cli.RowSet;
import org.apache.hive.service.cli.RowSetFactory;
import org.apache.hive.service.cli.thrift.TCLIService;
import org.apache.hive.service.cli.thrift.TCancelOperationReq;
import org.apache.hive.service.cli.thrift.TCancelOperationResp;
import org.apache.hive.service.cli.thrift.TColumnDesc;
import org.apache.hive.service.cli.thrift.TExecuteStatementReq;
import org.apache.hive.service.cli.thrift.TExecuteStatementResp;
import org.apache.hive.service.cli.thrift.TFetchOrientation;
import org.apache.hive.service.cli.thrift.TFetchResultsReq;
import org.apache.hive.service.cli.thrift.TFetchResultsResp;
import org.apache.hive.service.cli.thrift.TGetColumnsReq;
import org.apache.hive.service.cli.thrift.TGetColumnsResp;
import org.apache.hive.service.cli.thrift.TGetOperationStatusReq;
import org.apache.hive.service.cli.thrift.TGetOperationStatusResp;
import org.apache.hive.service.cli.thrift.TGetResultSetMetadataReq;
import org.apache.hive.service.cli.thrift.TGetResultSetMetadataResp;
import org.apache.hive.service.cli.thrift.TGetSchemasReq;
import org.apache.hive.service.cli.thrift.TGetSchemasResp;
import org.apache.hive.service.cli.thrift.TGetTablesReq;
import org.apache.hive.service.cli.thrift.TGetTablesResp;
import org.apache.hive.service.cli.thrift.TOperationHandle;
import org.apache.hive.service.cli.thrift.TOperationState;
import org.apache.hive.service.cli.thrift.TProtocolVersion;
import org.apache.hive.service.cli.thrift.TRowSet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 使用proxy的方式连接impala服务器，如果不使用代理的方式，可以直接使用hive jdbc连接。
 * 而对于代理的方式因为hive使用的代理参数为hive.server2.proxy.user=xxx，但是impala使用的参数为impala.doas.user
 * 但是不能够直接通过在url后面加上参数来指定，因为hive-jdbc在openSession的时候会对所有的参数加上
 * set:hiveconf:和set:hivevar:的前缀，导致impala不能识别，因此在使用proxy的时候不能直接使用hive-jdbc。
 * 
 * Input: hive jdbc url,兼容hive url的格式,需要使用一个可代理用户执行
 * Output : TCLIService.Iface对象，已经执行openSession。
 * 
 */
public class ThriftClientExample {
	private static int ONE_FETCH_COUNT = 10;
	private static final Options options = new Options();
	private static final Logger LOG = LoggerFactory.getLogger(ThriftClientExample.class);
	private static String currentDatabase = "default";
	private static boolean isVerbose = false;
	
	static {
		options.addOption("u", "url", true, "impala url");
		options.addOption("k", "keytab", true, "keytab location");
		options.addOption("p", "principal", true, "principal name");
		options.addOption("v", "verbose", false, "Verbose mode");
		options.addOption("h", "help", false, "help message");
	}
	
	private static void currentKerberosUser() throws IOException {
		Configuration conf = new Configuration();
		conf.set("hadoop.security.authentication", "kerberos");
		UserGroupInformation.setConfiguration(conf);
		System.out.println(UserGroupInformation.getCurrentUser());
	}
	
	public static void help() {
		new HelpFormatter().printHelp("impala-client-example", options);
	}
	
	private static String init(String[] args) throws ParseException {
		PosixParser parser = new PosixParser(); 
		CommandLine cmds = parser.parse(options, args);
		 
		if(cmds.hasOption("help")) {
		    return null;
		}
		
		String url = cmds.getOptionValue("u", null);
		if(url == null) {
			LOG.warn("Must specify url parameter.");
			return null;
		}
		
		String keytab = cmds.getOptionValue("k", null);
		String principal = cmds.getOptionValue("p", null);
		
		if(keytab != null && principal != null) {
			com.netease.impala.tools.Utils.initKerberos(keytab, principal);
		}
		isVerbose = cmds.hasOption("v");
		return url;
	}
	
	private static String spacesForString(String s) {
	    if (s == null || s.length() == 0) {
	      return "";
	    }
	    return String.format("%1$-" + s.length() +"s", "");
	}
	
	public static void main(String[] args) throws Exception {
		String url = init(args);
		currentKerberosUser();

		if(url == null) {
			help();
			return ;
		}

		ImpalaThriftClient client = new ImpalaThriftClient(url);
		client.init();
		
		System.out.println("Create connection to " + url + " successfully.");
		System.out.println("Current database is " + currentDatabase);
		
		OutputHandler handler = new SystemOutHandler();
		int ret = -1;
		try {
			String line = null;
			String prefix = "";
			ConsoleReader reader = getReader();
			String prompt = "IMPALA";
			String dbSpaces = spacesForString(prompt);
			
			while ((line = reader.readLine(prompt + "> ")) != null) { 
				if (!prefix.equals("")) {
					prefix += ' ';
				}
				if (line.trim().endsWith(";") && !line.trim().endsWith("\\;")) {
					line = prefix + line;
					line = line.trim();
					int lastIndex = line.length();
					for( ; lastIndex > 0 && line.charAt(lastIndex - 1) == ';' ; lastIndex --);
					line = line.substring(0, lastIndex);
					
					ret = processCmd(client, line, handler);
					prefix = "";
					dbSpaces = dbSpaces.length() == prompt.length() ? dbSpaces : spacesForString(prompt);
				} else {
					prefix = prefix + line;
					prompt = dbSpaces;
					continue;
				}
			}
		} catch (IOException e) {
			LOG.error("Fetch input from console reader error" , e);
			ret = -1;
		} finally {
			client.close();
		}
	}
	
	
	private static int processCmd(ImpalaThriftClient client, String cmd, OutputHandler handler) {
		Result result = null;
		if(isVerbose) {
			handler.handleOut("Sql : " + cmd);
		}
		try {
/**			
			if(select.matcher(cmd).matches()) {
			} else if(showDatabases.matcher(cmd).matches()) {
				result = getDatabases(client);
			} else if(showTables.matcher(cmd).matches()) {
				result = getTables(client, currentDatabase);
			} else if(descTables.matcher(cmd).matches()) {
				
				result = getColumns(client, currentDatabase, );
			} else {
				LOG.info("Can not match input sql : " + cmd);
				System.out.println("Not Supportted SQL : " + cmd);
			} 
			**/
			result = queryStatement(client, cmd);
			if(result != null) {
				handler.handleResult(result.getHeader(), result.getData());
			}
		} catch (Exception e) {
			LOG.warn("Execute sql " + cmd + " failed.", e);
			handler.handleErr("Error : " + e.getMessage());
		}
		return 0;
	}
	
	private static ConsoleReader getReader() {
	    try {
	    	ConsoleReader reader = new ConsoleReader(System.in, System.out);
			reader.setBellEnabled(false);
		    final String HISTORYFILE = ".impala_history";
		    String historyDirectory = System.getProperty("user.home");
		    if ((new File(historyDirectory)).exists()) {
		    	String historyFile = historyDirectory + File.separator + HISTORYFILE;
		    	reader.setHistory(new FileHistory(new File(historyFile)));
		    } else {
		    	LOG.warn("WARNING: Directory for Hive history file: " + historyDirectory +
                           " does not exist.   History will not be available during this session.");
		    }
		    return reader;
	    } catch (Exception e) {
	    	LOG.warn("WARNING: Encountered an error while trying to initialize Hive's " +
	    			"history file.  History will not be available during this session.", e);
	    }
	    
	    return null;
	}
	
	private static Result getDatabases(ImpalaThriftClient cli) throws Exception {
		TCLIService.Iface client = cli.getHS2Client();
		TGetSchemasReq req = new TGetSchemasReq(cli.getSessionHandler());
		TGetSchemasResp resp = client.GetSchemas(req);
		Utils.verifySuccessWithInfo(resp.getStatus());
		return fetchResultSetAndMeta(client, resp.getOperationHandle(), cli.getProtocolVersion());
	}
	
	private static Result getTables(ImpalaThriftClient cli, String database) throws Exception {
		TCLIService.Iface client = cli.getHS2Client();
		TGetTablesReq req = new TGetTablesReq(cli.getSessionHandler());
		req.setSchemaName(database);
		
		TGetTablesResp resp = client.GetTables(req);
		Utils.verifySuccessWithInfo(resp.getStatus());
		
		return fetchResultSetAndMeta(client, resp.getOperationHandle(), cli.getProtocolVersion());
	}
	
	private static Result getCloumns(ImpalaThriftClient cli, String database, String table) throws Exception {
		TCLIService.Iface client = cli.getHS2Client();
		TGetColumnsReq req = new TGetColumnsReq(cli.getSessionHandler());
		req.setSchemaName(database);
		req.setTableName(table);
		
		TGetColumnsResp resp = client.GetColumns(req);
		Utils.verifySuccessWithInfo(resp.getStatus());
		
		return fetchResultSetAndMeta(client, resp.getOperationHandle(), cli.getProtocolVersion());
	}
	
	private static void queryWithCancel(ImpalaThriftClient cli, String sql) throws Exception {
		queryStatementInside(cli, sql, true, false);
	}
	
	private static Result queryStatement(ImpalaThriftClient cli, String sql) throws Exception {
		return queryStatementInside(cli, sql, false, true);
	}
	
	private static Result queryStatementSync(ImpalaThriftClient cli, String sql) throws Exception {
		return queryStatementInside(cli, sql, false, true);
	}
	
	private static Result queryStatementInside(ImpalaThriftClient cli, String sql, boolean cancel, boolean sync) throws Exception {
		TCLIService.Iface client = cli.getHS2Client();
		TExecuteStatementReq req = new TExecuteStatementReq();
		req.setRunAsync(sync);
		req.setSessionHandle(cli.getSessionHandler());
		req.setStatement(sql);
		TExecuteStatementResp resp = client.ExecuteStatement(req);
		Utils.verifySuccessWithInfo(resp.getStatus());
		TOperationHandle stmtHandle = resp.getOperationHandle();
		TProtocolVersion version = cli.getProtocolVersion();
		
		if(cancel) {
			TCancelOperationReq cancelReq = new TCancelOperationReq(stmtHandle);
			TCancelOperationResp cancelResp = client.CancelOperation(cancelReq);
			Utils.verifySuccessWithInfo(cancelResp.getStatus());
			System.out.println("Cancel query finish !");
			
			TGetOperationStatusResp status = client.GetOperationStatus(new TGetOperationStatusReq(stmtHandle));
	        Utils.verifySuccessWithInfo(status.getStatus());

			if(status.getOperationState() == TOperationState.CANCELED_STATE) {
				System.out.println("Canceled statement !");
			}
		} else {
			if(sync) {
				return fetchResultSetAndMeta(client, stmtHandle, version);
			} else {
				fetchResult(client, stmtHandle, version, sql);
			}
		}
		return null;
	}
	
	private static void fetchResult(TCLIService.Iface client, TOperationHandle opHandler, TProtocolVersion version, String sql) throws Exception {
		while(true) {
			TGetOperationStatusResp status = client.GetOperationStatus(new TGetOperationStatusReq(opHandler));
			Utils.verifySuccessWithInfo(status.getStatus());
			if(status.isSetOperationState() && status.getOperationState() == TOperationState.FINISHED_STATE) {
				System.out.println("Query finish !");
				break;
			}
			
			System.out.println("get query state : " + status.getOperationState());
		}
		
		if (!opHandler.isHasResultSet()) {
			System.out.println("Statement " + sql + " do not has resultset");
			return ;
		}
		
		fetchResultSetAndMeta(client, opHandler, version);
	}
	
	private static Result fetchResultSetAndMeta(TCLIService.Iface client, TOperationHandle opHandler, TProtocolVersion version) 
			throws Exception {
		Result result = new Result();
		List<String> header = new LinkedList<String>();
		boolean fetchMeta = false;
		RowSet fetchedRows = null;
		Iterator<Object[]> fetchedRowsItr = null;
		while(true) {
			TFetchResultsResp fetchResp = null;
			try {
				TFetchOrientation orientation = TFetchOrientation.FETCH_NEXT;
				if (fetchedRows == null || !fetchedRowsItr.hasNext()) {
					TFetchResultsReq fetchReq = new TFetchResultsReq(opHandler, orientation, ONE_FETCH_COUNT);
					fetchResp = client.FetchResults(fetchReq);
		        }
		        Utils.verifySuccessWithInfo(fetchResp.getStatus());
		        
		        if(fetchMeta == false) {
		        	TGetResultSetMetadataReq metaReq = new TGetResultSetMetadataReq(opHandler);
		    		TGetResultSetMetadataResp metaResp = client.GetResultSetMetadata(metaReq);
		            Utils.verifySuccessWithInfo(metaResp.getStatus());
		            if(metaResp.isSetSchema() == false ||  metaResp.getSchema() == null)
		            	return null;
		            StringBuffer meta = new StringBuffer();
		            for(TColumnDesc col : metaResp.getSchema().getColumns()) {
//		            	meta.append(String.format("%s(%s)", col.getColumnName(), col.getTypeDesc())).append("\t");
		            	header.add(col.getColumnName());
		            }
		            fetchMeta = true;
		        }
		        result.setHeader(header);

		        TRowSet results = fetchResp.getResults();
		        fetchedRows = RowSetFactory.create(results, version);
		        fetchedRowsItr = fetchedRows.iterator();

		        while (fetchedRowsItr.hasNext()) {
		        	List<String> lst = new LinkedList<String>();
//		        	StringBuffer line = new StringBuffer();
		        	Object[] row = fetchedRowsItr.next();
		        	for(Object obj : row) {
//		        		line.append(obj).append("\t");
		        		lst.add(obj.toString());
		        	}
		        	result.addData(lst);
//		        	System.out.println(line.toString());
		        } 
		        
		        if(!fetchResp.isHasMoreRows()) {
//		        	System.out.println("Finish fetch rows...");
		        	break;
		        }
			} catch (SQLException eS) {
				throw eS;
		    } catch (Exception ex) {
		    	throw new SQLException("Error retrieving next row", ex);
		    }
		}
		
		return result;
	}
}