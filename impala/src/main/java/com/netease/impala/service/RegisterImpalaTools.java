package com.netease.impala.service;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Properties;

import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hive.service.cli.CLIService;
import org.apache.hive.service.cli.thrift.ThriftCLIService;
import org.apache.hive.service.cli.thrift.ThriftHttpCLIService;
import org.apache.hive.service.server.HiveServer2;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RegisterImpalaTools {
	private static final Logger logger = LoggerFactory.getLogger(RegisterImpalaTools.class);
	private static String ImpalaComamndName = "impalad";
	private static int MAX_ERROR_COUNT = 3;
	
	/**
	 * @param args
	 * @throws IllegalAccessException
	 * @throws IllegalArgumentException
	 * @throws InvocationTargetException
	 */
	public static void main(String[] args) throws Exception {
		Options options = initOptions();
		org.apache.commons.cli.CommandLine commandLine = new GnuParser().parse(options, args);
		Properties confProps = commandLine.getOptionProperties("hiveconf");
		
		HiveConf conf = new HiveConf();
        for (String propKey : confProps.stringPropertyNames()) {
        	System.setProperty(propKey, confProps.getProperty(propKey));
        	conf.set(propKey, confProps.getProperty(propKey));
        }
		
		if(commandLine.hasOption("pid")) {
			String pid = commandLine.getOptionValue("pid");
			int ret = register(conf);
			if(ret != 0) {
				logger.error("Register current impalad node to zookeeper failed.");
				System.exit(-1);
			}
			monitorImpaladProcess(pid);
			logger.error("Impalad process has been down, pid is {}", pid);
		} else if (commandLine.hasOption("deregister")) {
	          deregister(conf, commandLine.getOptionValue("deregister"));
	          return ;
		} else {
			new HelpFormatter().printHelp("impala-register-zookeeper-tools", options);
			return;
		}
	}
	
	private static Options initOptions() {
		Options options = new Options();
		options.addOption(OptionBuilder
		      .withValueSeparator()
		      .hasArgs(2)
		      .withArgName("property=value")
		      .withLongOpt("hiveconf")
		      .withDescription("Use value for given property")
		      .create());
		options.addOption(OptionBuilder.withLongOpt("pid").hasArg().withArgName("impalad process id")
		      .withDescription("register and monitor for the impalad process.")
		      .create());
		options.addOption(OptionBuilder
		          .hasArgs(1)
		          .withArgName("versionNumber")
		          .withLongOpt("deregister")
		          .withDescription("Deregister all instances of given version from dynamic service discovery")
		          .create());
		options.addOption(new Option("h", "help", false, "Print help information"));
		
		return options;
	}
	
	private static void monitorImpaladProcess(String pid) {
		int errorCount = 0;
		while(true) {
			boolean ret = checkImpaladProcess(pid);
			if(ret == false) {
				logger.warn("Impalad process check return false, pid is {}", pid);
				errorCount ++;
				if(errorCount == MAX_ERROR_COUNT) {
					return;
				}
			} else {
				logger.warn("Impalad process check return true, pid is {}", pid);
				errorCount = 0;
			}
			try {
				Thread.sleep(5000);
			} catch (InterruptedException e) {
				logger.warn("Thread interrupted while sleep.", e);
			}
		}
	}
	
	private static boolean checkImpaladProcess(String pid) {
		BufferedReader reader = null;
		try {
			String path = "/proc/" + pid;
			String commandFile = "comm";
			File fp = new File(path);
			
			if(fp.exists() && fp.isDirectory()) {
				File cmd = new File(fp, commandFile);
				if(!cmd.exists() || !cmd.isFile()) {
					logger.info("File {} is not exist or not a normal file !", cmd.getAbsoluteFile());
					return false;
				}
				reader = new BufferedReader(new FileReader(cmd));
				String line = reader.readLine();
				if(line.trim().equals(ImpalaComamndName)) {
					return true;
				}
			}
		} catch(Exception e) {
			logger.error("Error happened in read command file.", e);
		} finally {
			if(reader != null) {
				try {
					reader.close();
				} catch (IOException e) {
				}
			}
		}
		return false;
	}
	
	private static int register(HiveConf conf) throws Exception {
		HiveServer2 server = new HiveServer2();
		CLIService cliService = new CLIService(server);
		ThriftCLIService thriftCLIService = new ThriftHttpCLIService(cliService);
		thriftCLIService.init(conf);
		Field cliServiceField = server.getClass().getDeclaredField("thriftCLIService");
		if(cliServiceField == null) {
			logger.warn("Can not find thriftCLIService field in " + server.getClass());
			return -1;
		}
		cliServiceField.setAccessible(true);
		cliServiceField.set(server, thriftCLIService);
		
		Method addMethod = server.getClass().getDeclaredMethod("addServerInstanceToZooKeeper", 
				HiveConf.class);
		if(addMethod == null) {
			logger.warn("Can not find addServerInstanceToZooKeeper method in " + server.getClass());
			return -1;
		}
		
		addMethod.setAccessible(true);
		addMethod.invoke(server, conf);
		if(!server.isRegisteredWithZooKeeper()) {
			logger.error("Can not register to zookeeper, check config in hive-site.xml !");
			return -1;
		}
		
		return 0;
	}
	
	private static void deregister(HiveConf conf, String version) throws Exception {
 		HiveServer2 server = new HiveServer2();
	
 		Method deleteMethod = server.getClass().getDeclaredMethod("deleteServerInstancesFromZooKeeper", String.class);
 		deleteMethod.setAccessible(true);
 		deleteMethod.invoke(server, version);
 	}
}
