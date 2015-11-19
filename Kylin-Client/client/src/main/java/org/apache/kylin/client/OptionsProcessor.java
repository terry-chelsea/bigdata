package org.apache.kylin.client;

import java.util.Arrays;
import java.util.Properties;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.log4j.Logger;

public class OptionsProcessor {
    private static Logger logger = Logger.getLogger(OptionsProcessor.class);
    private final Options options = new Options();
    private CommandLine commandLine;
    
    @SuppressWarnings("static-access")
    public OptionsProcessor() {
    	// -d database
    	options.addOption(OptionBuilder
    			.hasArg(true)
    			.withArgName("databasename")
    			.withDescription("Specify the database to use")
    			.create('d'));

    	// -e 'quoted-query-string'
    	options.addOption(OptionBuilder
    			.hasArg(true)
    			.withArgName("quoted-query-string")
    			.withDescription("SQL from command line")
    			.create('e'));

    	// -f <query-file>
    	options.addOption(OptionBuilder
    			.hasArg(true)
    			.withArgName("filename")
    			.withDescription("SQL from files")
    			.create('f'));

    	// -conf x=y
    	options.addOption(OptionBuilder
    			.withValueSeparator()
    			.hasArgs(2)
    			.withArgName("property=value")
    			.withLongOpt("conf")
    			.withDescription("Use value for given property")
    			.create());

    	// -h hostname/ippaddress
    	options.addOption(OptionBuilder
    			.hasArg(true).isRequired()
    			.withArgName("hostname")
    			.withDescription("connecting to Kylin Server on remote host")
    			.create('h'));

    	// -P port
    	options.addOption(OptionBuilder
    			.hasArg(true)
    			.withArgName("port")
    			.withDescription("connecting to Kylin Server on port number")
    			.create('P'));
    	
    	// -u uasename
    	options.addOption(OptionBuilder
    			.hasArg(true)
    			.withArgName("username")
    			.withDescription("username that connecting to Kylin Server")
    			.create('u'));
    	
    	// -p password
    	options.addOption(OptionBuilder
    			.hasArg(true)
    			.withArgName("password")
    			.withDescription("password of connecting to Kylin Server")
    			.create('p'));

    	// [-S|--silent]
//    	options.addOption(new Option("S", "silent", false, "Silent mode in interactive shell"));

    	// [-v|--verbose]
    	options.addOption(new Option("v", "verbose", false, "Verbose mode (echo executed SQL to the console)"));

    	// [-H|--help]
    	options.addOption(new Option("H", "help", false, "Print help information"));

    }
    
    public Config precessArgs(String[] args) {
    	Config conf = new Config();
    	try {
			commandLine = new GnuParser().parse(options, args);
		} catch (ParseException e) {
			printUsage();
			return null;
		}
    	
        if (commandLine.hasOption('H')) {
        	printUsage();
        	return null;
        }

//        conf.setSilent(commandLine.hasOption('S'));

        conf.setDatabase(commandLine.getOptionValue("d"));

        conf.setExecuteSql(commandLine.getOptionValue('e'));

        conf.setExecuteFile(commandLine.getOptionValue('f'));

        conf.setVerbose(commandLine.hasOption('v'));

        conf.setHostname((String) commandLine.getOptionValue('h'));

        conf.setPort(Integer.parseInt((String) commandLine.getOptionValue('P', "7070")));
        
        conf.setUsername(commandLine.getOptionValue('u', "ADMIN"));
        
        conf.setPassword(commandLine.getOptionValue('p', "KYLIN"));

        if (conf.getExecuteFile() != null && conf.getExecuteSql() != null) {
        	System.err.println("The '-e' and '-f' options cannot be specified simultaneously");
        	printUsage();
        	return null;
        }

        if (commandLine.hasOption("conf")) {
        	Properties confProps = commandLine.getOptionProperties("conf");
        	conf.setProperties(confProps);
        }

        return conf;
    }
    
    private void printUsage() {
    	new HelpFormatter().printHelp("kylin", options);
    }
}
