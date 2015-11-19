package org.apache.kylin.client;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintStream;
import java.io.PrintWriter;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import jline.ConsoleReader;
import jline.History;

import org.apache.kylin.client.meta.ColumnMeta;
import org.apache.kylin.client.meta.CubeDescMeta;
import org.apache.kylin.client.meta.CubeDimensionMeta;
import org.apache.kylin.client.meta.CubeMeasureMeta;
import org.apache.kylin.client.meta.CubeMeta;
import org.apache.kylin.client.meta.CubeModelMeta;
import org.apache.kylin.client.meta.LookupTableMeta;
import org.apache.kylin.client.meta.ProjectMeta;
import org.apache.kylin.client.meta.TableMeta;
import org.apache.kylin.client.method.Utils;
import org.apache.log4j.Logger;

public class ClientDriver {
	private static String SHOW_PROJECTS = "SHOW PROJECTS";
	private static String SHOW_PROJECTS_DESC = "Show all projects in kylin";
	private static String USE_PROJECT = "USE ${PROJECT}";
	private static String USE_PROJECT_DESC = "Switch to named project";
	private static String SHOW_CUBES = "SHOW CUBES";
	private static String SHOW_CUBES_DESC = "Show all cubes in current project";
	private static String DESCRIBE_CUBE = "DESCRIBE ${CUBE}";
	private static String DESCRIBE_CUBE_DESC = "Describe named cube metadata, including model, dimensions and measures";
	private static String SELECT = "SELECT xxx";
	private static String SELECT_DESC = "Execute query sql with dimensions and measures";

/*	
	private static String SELECT = "select [[distinct](dimension)], [aggerate(measure)] from FactTableName [inner/left/right join] LookupTablebName where "
		    + "... group by dimension [having aggerate(measure)] [order by aggerate(measure)] [asc|desc] [limit n] [offset m]";
*/
	private static Pattern SHOW_DATABASES_PATTERN = Pattern.compile("(?i)\\s*SHOW\\s+PROJECTS");
	private static Pattern USE_PROJECT_PATTERN = Pattern.compile("(?i)\\s*USE\\s+(.*)");
	private static Pattern SHOW_CUBES_PATTERN = Pattern.compile("(?i)\\s*SHOW\\s+CUBES");
	private static Pattern DESCRIBE_CUBE_PATTERN = Pattern.compile("(?i)\\s*DESCRIBE\\s+(.*)");
	private static Pattern SELECT_PATTERN = Pattern.compile("(?i)" + "\\s*SELECT\\s+.*");
	
	private static String PROMPT_DEFAULT = "kylin";
	private Logger logger = Logger.getLogger(ClientDriver.class);
	
	private Config config;
	private Connection connection = null;
	private ProjectMeta currentProject = null;
	private static PrintStream ERROR_OUT = System.err;
	
	private Kylin kylin = null;
	
	public ClientDriver(Config conf) {
		this.config = conf;
		kylin = new Kylin(config.getHostname(), config.getPort(), config.getUsername(), config.getPassword());
	}
	
	private String getPrompt() {
		String prompt = PROMPT_DEFAULT;
		if(currentProject != null) {
			prompt = PROMPT_DEFAULT + "(" + currentProject.getProjectName() + ")";
		}
		return prompt;
	}
	
	private static String spacesForString(String s) {
	    if (s == null || s.length() == 0) {
	      return "";
	    }
	    return String.format("%1$-" + s.length() +"s", "");
	}
	
	private ConsoleReader getReader() {
	    try {
	    	ConsoleReader reader = new ConsoleReader(System.in, new PrintWriter(ERROR_OUT));
			reader.setBellEnabled(false);
		    final String HISTORYFILE = ".hivehistory";
		    String historyDirectory = System.getProperty("user.home");
		    if ((new File(historyDirectory)).exists()) {
		    	String historyFile = historyDirectory + File.separator + HISTORYFILE;
		    	reader.setHistory(new History(new File(historyFile)));
		    } else {
		    	logger.warn("WARNING: Directory for Hive history file: " + historyDirectory +
                           " does not exist.   History will not be available during this session.");
		    }
		    return reader;
	    } catch (Exception e) {
	    	logger.warn("WARNING: Encountered an error while trying to initialize Hive's " +
	    			"history file.  History will not be available during this session.", e);
	    }
	    
	    return null;
	}

	
	public int run() {
		String initProject = this.config.getDatabase();
		if(initProject != null) {
			boolean succ = createProjectConnection(initProject);
			if(!succ) {
				logger.warn("Create jdbc connection to initialize project " + initProject);
				ERROR_OUT.println("Create Connection to project " + initProject + " error !");
			}
		}
		if(connection == null && currentProject != null) {
			String projectName = currentProject.getProjectName();
			try {
				connection = kylin.getJdbcConnection(projectName);
			} catch (KylinClientException e) {
				logger.warn("Create init connection for project " + projectName);
			}
		}
		
		if(config.getExecuteSql() != null) {
			int cmdStatus = this.processLine(config.getExecuteSql());
			return cmdStatus;
		}
		
		if(config.getExecuteFile() != null) {
			int fileStatus = this.processFile(config.getExecuteFile());
			return fileStatus;
		}
		
		int ret = -1;
		try {
			String line = null;
			String prefix = "";
			ConsoleReader reader = getReader();
			String prompt = getPrompt();
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
					
					ret = this.processCmd(line);
					prefix = "";
					prompt = getPrompt();
					dbSpaces = dbSpaces.length() == prompt.length() ? dbSpaces : spacesForString(prompt);
				} else {
					prefix = prefix + line;
					prompt = dbSpaces;
					continue;
				}
			}
		} catch (IOException e) {
			logger.error("Fetch input from console reader error" , e);
			return -1;
		}
		return ret;
	}
	
	public int processLine(String line) {
		return processCmd(line);
	}
	
	public int processFile(String fileName) {
		File file = new File(fileName);
		StringBuffer sb = new StringBuffer();
		String line = null;
		BufferedReader reader = null;
		try {
			reader = new BufferedReader(new InputStreamReader(new FileInputStream(file)));
			while((line = reader.readLine()) != null) {
				if(!line.startsWith("--")) {
					sb.append(line + " ");
				}
			}
		} catch (Exception e) {
			logger.error("Read sql from file " + fileName + " error", e);
			return -1;
		} finally {
			if(reader != null)
				try {
					reader.close();
				} catch (IOException e) {
					logger.error("Close file " + fileName + " error", e);
					return -1;
				}
		}
		int ret = -1;
		for(String cmd : sb.toString().split(";")) {
			if(cmd.trim().isEmpty())
				continue;
			ret = processCmd(cmd);
		}
		return ret;
	}
	
	public int processCmd(String cmd) {
		long start = System.currentTimeMillis();
		cmd = cmd.trim();
		if(cmd.isEmpty())
			return 0;
		
		logger.info("Process sql : " + cmd);
		if(cmd.equalsIgnoreCase("quit") || cmd.equalsIgnoreCase("exit")) {
			System.exit(0);
		}
		if(config.isVerbose())
			System.out.println("SQL : " + cmd);
		
		int ret = -1;
		boolean executed = false;
		if(SHOW_DATABASES_PATTERN.matcher(cmd).matches()) {
			ret = processShowDatabasesCmd(cmd);
			executed = true;
		} else if (USE_PROJECT_PATTERN.matcher(cmd).matches()) {
			ret = processUseProjectCmd(cmd);
			executed = true;
		}
		
		if(!executed) {
			if(currentProject == null) {
				logger.warn("Execute sql " + cmd + " while no project selected");
				ERROR_OUT.println("No project selected!");
				return -1;
			}
			if(SELECT_PATTERN.matcher(cmd).matches()) {
				ret = processSqlCmd(cmd);
			} else if(DESCRIBE_CUBE_PATTERN.matcher(cmd).matches()) {
				ret = processDescribeCmd(cmd);
			} else if(SHOW_CUBES_PATTERN.matcher(cmd).matches()) {
				ret = processShowCubesCmd(cmd);
			} else {
				logger.warn("Not Support SQL : " + cmd);
				ERROR_OUT.println("Not Support SQL : " + cmd);
				printHelpMessage();
				ret = -1;
			}
		}
		if(ret >= 0) {
			long end = System.currentTimeMillis();
			ERROR_OUT.println("OK");
			ERROR_OUT.println(String.format("Time taken: %.3f seconds, Fetched: %d row(s)", 
					(end - start) / (double)1000, ret));
			return 0;
		}
		return -1;
	}
	
	private void printHelpMessage() {
		List<String> headers = Arrays.asList("COMMAND", "INSTRUCTION");
		List<List<String>> datas = Arrays.asList(
				Arrays.asList(SHOW_PROJECTS, SHOW_PROJECTS_DESC), 
				Arrays.asList(USE_PROJECT, USE_PROJECT_DESC),
				Arrays.asList(SHOW_CUBES, SHOW_CUBES_DESC),
				Arrays.asList(DESCRIBE_CUBE, DESCRIBE_CUBE_DESC),
				Arrays.asList(SELECT, SELECT_DESC));
		
		Utils.printResultWithTable(headers, datas, 2);
	}
	
	public List<String> supportSql() {
		return Arrays.asList(SHOW_PROJECTS, USE_PROJECT, SHOW_CUBES, DESCRIBE_CUBE, SELECT);
	}
	
	private boolean createProjectConnection(String projectName) {
		ProjectMeta formerProject = currentProject;
		//切换到相同的project
		if(currentProject == null || !currentProject.getProjectName().equalsIgnoreCase(projectName)) {
			List<ProjectMeta> projects = null;
			try {
				projects = kylin.getAllProjects();
			} catch (KylinClientException e) {
				logger.error("Fetch all project error", e);
				return false;
			}
			boolean found = false;
			for(ProjectMeta project : projects) {
				if(project.getProjectName().equalsIgnoreCase(projectName)) {
					currentProject = project;
					found = true;
					break;
				}
			}
			if(!found) {
				logger.error("Can not find project " + projectName);
				return false;
			}
		}
		boolean ret = createConnection(true) != null;
		if(!ret) {
			currentProject = formerProject;
		}
		return ret;
	}
	
	private Connection createConnection(boolean force) {
		if(!force && connection != null) {
			return connection;
		}
		if(currentProject == null) {
			logger.warn("No project selected while create new jdbc connection");
			return null;
		}
		String currentProjectName = currentProject.getProjectName();
		Connection conn;
		try {
			conn = kylin.getJdbcConnection(currentProjectName);
		} catch (KylinClientException e) {
			logger.error("Create jdbc connection to project " + currentProjectName);
			return null;
		}
		this.connection = conn;
		return this.connection;
	}
	
	private int processSqlCmd(String cmd) {
		Connection conn = createConnection(false);
		if(conn == null) {
			return -1;
		}
		
		Statement state = null;
		ResultSet result = null;
		try {
			state = conn.createStatement();
			result = state.executeQuery(cmd);
			ResultSetMetaData meta = result.getMetaData();
            List<String> headers = Utils.extractCloumns(cmd);
            int columnSize = meta.getColumnCount();
            if(headers == null || headers.size() != columnSize) {
            	headers = new ArrayList<String>(columnSize);
            	for(int i = 0 ; i < columnSize ; ++ i) {
            		headers.add(meta.getColumnName(i + 1));
            	}
            }
            List<List<String>> datas = new LinkedList<List<String>>();

            while(result.next()) {
                List<String> data = new ArrayList<String>(columnSize);
                for(int i = 0 ; i < columnSize ; ++ i) {
                    data.add(result.getString(i + 1));
                }
                datas.add(data);
            }
            return Utils.printResultWithTable(headers, datas, 0);
		} catch (SQLException e) {
			logger.error("Execute SELECT SQL " + cmd + " error", e);
			//output error message to user
			ERROR_OUT.println(e.getMessage());
			return -1;
		} finally {
			Utils.close(result, state, null);
		}
	}
	
	private int processDescribeCmd(String cmd) {
		Matcher matcher = DESCRIBE_CUBE_PATTERN.matcher(cmd);
		if(!matcher.matches())
			return -1;
		String cubeName = matcher.group(matcher.groupCount());
		
		CubeMeta cubeMeta;
		try {
			cubeMeta = kylin.getCubeByName(currentProject, cubeName);
		} catch (KylinClientException e) {
			logger.error("Can not find cube " + cubeName + " in project " + 
					currentProject.getProjectName());
			return -1;
		}
		CubeModelMeta cubeModel;
		CubeDescMeta cubeDesc;
		try {
			cubeModel = kylin.getCubeModel(cubeMeta);
			cubeDesc = kylin.getCubeDescription(cubeMeta);
		} catch (KylinClientException e) {
			logger.error("Fetch cube model and desc of cube " + cubeMeta.getCubeName() + 
					"error", e);
			return -1;
		}
		
		int count = 0;
		count += printCubeModel(cubeModel);
		count += printCubeDesc(cubeDesc);
		return count;
	}
	
	private int printCubeModel(CubeModelMeta cubeModel) {
		ERROR_OUT.println("Cube Model : ");
		List<String> headers = Arrays.asList("TABLE", "TYPE", "JOIN", "PK", "FK");
		List<List<String>> datas = new LinkedList<List<String>>();
		if(cubeModel == null) {
			return Utils.printResultWithTable(headers, datas, 0);
		}
		TableMeta factTable = cubeModel.getFactTable();
		
		datas.add(Arrays.asList(factTable.getFullTableName(), "FACT", "NULL", "NULL", "NULL"));
		for(LookupTableMeta lookup : cubeModel.getLookupTables()) {
			datas.add(Arrays.asList(lookup.getTable().getFullTableName(), "DIMENSION", lookup.getType().toUpperCase(),
					getColumnsString(lookup.getPrimaryKeys()).toUpperCase(), getColumnsString(lookup.getForeignKeys()).toUpperCase()));
		}
		
		return Utils.printResultWithTable(headers, datas, 0);
	}
	
	private String getColumnsString(List<ColumnMeta> columns) {
		StringBuffer sb = new StringBuffer();
		int i = 0;
		for(ColumnMeta column : columns) {
			if(i ++ > 0) {
				sb.append(",");
			}
			sb.append(column.getName());
		}
		
		return sb.toString();
	}
	
	private int printCubeDesc(CubeDescMeta cubeDesc) {
		if(cubeDesc == null)
			return 0;
		
		ERROR_OUT.println("\nCube Dimensions : ");
		List<String> headers = Arrays.asList("NAME", "TABLE", "COLUMN", "TYPE");
		List<List<String>> datas = new LinkedList<List<String>>();
		for(CubeDimensionMeta dim : cubeDesc.getDimensions()) {
			datas.add(Arrays.asList(dim.getName(), dim.getColumn().getTable().getFullTableName(), dim.getColumn().getName(), 
					dim.getColumn().getType()));
		}
		int count = 0;
		count += Utils.printResultWithTable(headers, datas, 0);
		ERROR_OUT.println("\nCube Measures : ");
		headers = Arrays.asList("NAME", "EXPRESSION", "PARAMETER", "TYPE");
		datas = new LinkedList<List<String>>();
		
		for(CubeMeasureMeta measure : cubeDesc.getMeasures()) {
			datas.add(Arrays.asList(measure.getName(), measure.getExpression(), measure.getValue(), measure.getReturnType()));
		}
		count += Utils.printResultWithTable(headers, datas, 0);
		return count;
	}
	
	private int processShowCubesCmd(String cmd) {
		List<CubeMeta> cubes;
		try {
			cubes = kylin.getProjectCubes(currentProject);
		} catch (KylinClientException e) {
			logger.error("Fetch cubes of project " + currentProject.getProjectName() + 
					" error", e);
			return -1;
		}
		List<String> headers = Arrays.asList("NAME", "PROJECT", "ENABLE", "SIZE", "START_TIME", "END_TIME");
		List<List<String>> datas = new LinkedList<List<String>>();
		for(CubeMeta cube : cubes) {
			datas.add(Arrays.asList(cube.getCubeName(), currentProject.getProjectName(), String.valueOf(cube.isEnable()).toUpperCase(), 
					cube.getCubeSizeKb() + "KB", String.valueOf(cube.getRangeStart()), String.valueOf(cube.getRangeEnd())));
		}
		
		return Utils.printResultWithTable(headers, datas, 0);
	}
	
	private int processShowDatabasesCmd(String cmd) {
		List<ProjectMeta> projects;
		try {
			projects = kylin.getAllProjects();
		} catch (KylinClientException e) {
			logger.error("Fetch all projects error", e);
			return -1;
		}
		List<String> headers = Arrays.asList("PROJECT", "HIVE", "DESCRIPTION");
		List<List<String>> datas = new ArrayList<List<String>>(projects.size());
		
		for(ProjectMeta project : projects) {
			datas.add(Arrays.asList(project.getProjectName(), project.getHiveName(), project.getDescription()));
		}
		
		return Utils.printResultWithTable(headers, datas, 0);
	}
	
	private int processUseProjectCmd(String cmd) {
		Matcher matcher = USE_PROJECT_PATTERN.matcher(cmd);
		if(!matcher.matches())
			return -1;
		String projectName = matcher.group(matcher.groupCount());
		String from = currentProject == null ? "null" : currentProject.getProjectName();
		if(createProjectConnection(projectName)) {
			ERROR_OUT.println("Change Current Project from " + from + " to " + projectName);
			return 0;
		} else {
			return -1;
		}
	}
	
    public static void main(String[] args) {
    	String str = "use HELLO";
    	Matcher matcher = USE_PROJECT_PATTERN.matcher(str);
    	System.out.println(matcher.groupCount());
    	System.out.println(matcher.matches());
    	System.out.println(matcher.group(1));
    	
    	List<String> headers = Arrays.asList("HELLO", "WORLD", "全部都是中文");
    	List<List<String>> datas = Arrays.asList(
    			Arrays.asList("123", "345", "456"), 
    			Arrays.asList("llllllllll","yyyyyyyyyyyy", "TTTTTTTTTTTT"));
    	
    	int ret = Utils.printResultWithTable(headers, datas, 4);
    	System.out.println("lines " + ret);
    	
    	String test = "你好";
    	String realStr = String.format("%6s", test);
    	for(int i = 0 ; i < realStr.length() ; ++ i) {
    		if(realStr.charAt(i) == ' ') {
    			System.out.println("Space !");
    		}
    	}
    }
}
