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

import org.apache.kylin.client.ColorBuffer.ColorAttr;
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

import com.fasterxml.jackson.databind.ObjectMapper;


public class ClientDriver {
	private Config config;
	private Connection connection = null;
	private ProjectMeta currentProject = null;
	private static PrintStream resultOut = System.out;
	private static PrintStream otherOut = System.err;
	
	private Kylin kylin = null;
	
	private static String SHOW_DATABASES = "SHOW PROJECTS";
	private static String USE_PROJECT = "USE ${PROJECT}";
	private static String SHOW_CUBES = "SHOW CUBES";
	private static String DESCRIBE_CUBE = "DESCRIBE ${CUBE}";
	private static String SELECT = "select [[distinct](dimension)], [aggerate(measure)] from FactTableName [inner/left/right join] LookupTablebName where "
		                + "... group by dimension [having aggerate(measure)] [order by aggerate(measure)] [asc|desc] [limit n] [offset m]";
	
	private static Pattern SHOW_DATABASES_PATTERN = Pattern.compile("(?i)SHOW\\s+PROJECTS");
	private static Pattern USE_PROJECT_PATTERN = Pattern.compile("(?i)USE\\s+(.*)");
	private static Pattern SHOW_CUBES_PATTERN = Pattern.compile("(?i)SHOW\\s+CUBES");
	private static Pattern DESCRIBE_CUBE_PATTERN = Pattern.compile("(?i)DESCRIBE\\s+(.*)");
	private static Pattern SELECT_PATTERN = Pattern.compile("(?i)" + "SELECT\\s+.*");
	
	private static String PROMPT_DEFAULT = "kylin";
	
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
	    	ConsoleReader reader = new ConsoleReader(System.in, new PrintWriter(System.out));
			reader.setBellEnabled(false);
		    final String HISTORYFILE = ".hivehistory";
		    String historyDirectory = System.getProperty("user.home");
		    if ((new File(historyDirectory)).exists()) {
		    	String historyFile = historyDirectory + File.separator + HISTORYFILE;
		    	reader.setHistory(new History(new File(historyFile)));
		    } else {
		    	System.err.println("WARNING: Directory for Hive history file: " + historyDirectory +
                           " does not exist.   History will not be available during this session.");
		    }
		    return reader;
	    } catch (Exception e) {
	    	System.err.println("WARNING: Encountered an error while trying to initialize Hive's " +
	    			"history file.  History will not be available during this session.");
	    	System.err.println(e.getMessage());
	    }
	    
	    return null;
	}

	
	public int run() {
		String initProject = this.config.getDatabase();
		if(initProject != null) {
			boolean succ = createProjectConnection(initProject);
			if(! succ) {
				otherOut.println("Create Connection to project " + initProject + " error !");
			}
		}
		if(connection == null && currentProject != null) {
			connection = kylin.getJdbcConnection(currentProject.getProjectName());
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
			e.printStackTrace(otherOut);
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
			e.printStackTrace();
			return -1;
		} finally {
			if(reader != null)
				try {
					reader.close();
				} catch (IOException e) {
					e.printStackTrace();
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
				System.err.println("No project selected!");
				return -1;
			}
			if(SELECT_PATTERN.matcher(cmd).matches()) {
				ret = processSqlCmd(cmd);
			} else if(DESCRIBE_CUBE_PATTERN.matcher(cmd).matches()) {
				ret = processDescribeCmd(cmd);
			} else if(SHOW_CUBES_PATTERN.matcher(cmd).matches()) {
				ret = processShowCubesCmd(cmd);
			} else {
				System.err.println("Not Support SQL : " + cmd);
				printErrorMessage();
				ret = -1;
			}
		}
		if(ret >= 0) {
			long end = System.currentTimeMillis();
			System.out.println("OK");
			System.out.println(String.format("Time taken: %.3f seconds, Fetched: %d row(s)", 
					(end - start) / (double)1000, ret));
			return 0;
		}
		return -1;
	}
	
	private void printErrorMessage() {
		
	}
	
	public List<String> supportSql() {
		return Arrays.asList(SHOW_DATABASES, USE_PROJECT, SHOW_CUBES, DESCRIBE_CUBE, SELECT);
	}
	
	private boolean createProjectConnection(String projectName) {
		ProjectMeta formerProject = currentProject;
		//切换到相同的project
		if(currentProject == null || !currentProject.getProjectName().equalsIgnoreCase(projectName)) {
			List<ProjectMeta> projects = kylin.getAllProject();
			if(projects == null) {
				otherOut.println("Get Projects error !");
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
				otherOut.println("Can not find project " + projectName);
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
			System.err.println("No project selected!");
			return null;
		}
		String currentProjectName = currentProject.getProjectName();
		Connection conn = kylin.getJdbcConnection(currentProjectName);
		if(conn == null) {
			System.err.println("Create connection to " + currentProjectName + " failed !");
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
            List<String> headers = extractCloumns(cmd);
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
            return printResultWithTable(headers, datas);
		} catch (SQLException e) {
			otherOut.println(e.getMessage());
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
		
		CubeMeta cubeMeta = kylin.getCubeByName(currentProject, cubeName);
		if(cubeMeta == null) {
			otherOut.println("Can not find cube " + cubeName + " in project " + currentProject.getProjectName());
			return -1;
		}
		CubeModelMeta cubeModel = kylin.getCubeModel(cubeMeta);
		CubeDescMeta cubeDesc = kylin.getCubeDescription(cubeMeta);
		
		int count = 0;
		count += printCubeModel(cubeModel);
		count += printCubeDesc(cubeDesc);
		return count;
	}
	
	private int printCubeModel(CubeModelMeta cubeModel) {
		System.out.println("Cube Model : ");
		List<String> headers = Arrays.asList("TABLE", "TYPE", "JOIN", "PK", "FK");
		List<List<String>> data = new LinkedList<List<String>>();
		if(cubeModel == null) {
			return printResult(headers, data);
		}
		TableMeta factTable = cubeModel.getFactTable();
		
		data.add(Arrays.asList(factTable.getFullTableName(), "FACT", "NULL", "NULL", "NULL"));
		for(LookupTableMeta lookup : cubeModel.getLookupTables()) {
			data.add(Arrays.asList(lookup.getTable().getFullTableName(), "DIMENSION", lookup.getType().toUpperCase(),
					getColumnsString(lookup.getPrimaryKeys()).toUpperCase(), getColumnsString(lookup.getForeignKeys()).toUpperCase()));
		}
		
		return printResult(headers, data);
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
		
		System.out.println("\nCube Dimensions : ");
		List<String> headers = Arrays.asList("NAME", "TABLE", "COLUMN", "TYPE");
		List<List<String>> data = new LinkedList<List<String>>();
		for(CubeDimensionMeta dim : cubeDesc.getDimensions()) {
			data.add(Arrays.asList(dim.getName(), dim.getColumn().getTable().getFullTableName(), dim.getColumn().getName(), 
					dim.getColumn().getType()));
		}
		int count = 0;
		count += printResult(headers, data);
		System.out.println("\nCube Measures : ");
		headers = Arrays.asList("NAME", "EXPRESSION", "PARAMETER", "TYPE");
		data = new LinkedList<List<String>>();
		
		for(CubeMeasureMeta measure : cubeDesc.getMeasures()) {
			data.add(Arrays.asList(measure.getName(), measure.getExpression(), measure.getValue(), measure.getReturnType()));
		}
		count += printResult(headers, data);
		return count;
	}
	
	private int processShowCubesCmd(String cmd) {
		List<CubeMeta> cubes = kylin.getProjectCubes(currentProject);
		if(cubes == null) {
			otherOut.println("Get Cubes in project " + currentProject.getProjectName() + " error!");
			return -1;
		}
		List<String> headers = Arrays.asList("NAME", "PROJECT", "ENABLE", "SIZE", "START_TIME", "END_TIME");
		List<List<String>> data = new LinkedList<List<String>>();
		for(CubeMeta cube : cubes) {
			data.add(Arrays.asList(cube.getCubeName(), currentProject.getProjectName(), String.valueOf(cube.isEnable()).toUpperCase(), 
					cube.getCubeSizeKb() + "KB", String.valueOf(cube.getRangeStart()), String.valueOf(cube.getRangeEnd())));
		}
		
		return printResult(headers, data);
	}
	
	private int processShowDatabasesCmd(String cmd) {
		List<ProjectMeta> projects = kylin.getAllProject();
		if(projects == null) {
			otherOut.println("Get Projects error !");
			return -1;
		}
		List<String> headers = Arrays.asList("PROJECT", "HIVE", "DESCRIPTION");
		List<List<String>> data = new ArrayList<List<String>>(projects.size());
		
		for(ProjectMeta project : projects) {
			data.add(Arrays.asList(project.getProjectName(), project.getHiveName(), project.getDescription()));
		}
		
		return printResult(headers, data);
	}
	
	private int processUseProjectCmd(String cmd) {
		Matcher matcher = USE_PROJECT_PATTERN.matcher(cmd);
		if(!matcher.matches())
			return -1;
		String projectName = matcher.group(matcher.groupCount());
		String from = currentProject == null ? "null" : currentProject.getProjectName();
		if(createProjectConnection(projectName)) {
			System.out.println("Change Current Project from " + from + " to " + projectName);
			return 0;
		} else {
			return -1;
		}
	}
	
	private static int printResultWithTable(List<String> headers, List<List<String>> datas) {
		int SPACE_BEFORE_AND_AFTER = 1;
        List<Integer> delimIndex = new ArrayList<Integer>(headers.size() + 1);
        List<Integer> columnMaxLength = new ArrayList<Integer>(headers.size());
        delimIndex.add(0);
        int index = 0;
        for(int i = 0 ; i < headers.size() ; ++ i) {
        	if(headers.get(i) == null)
        		headers.set(i, "NULL");
        	String header = headers.get(i);

            index += (SPACE_BEFORE_AND_AFTER * 2 + length(header) + 1);
        	delimIndex.add(index);
        	columnMaxLength.add(length(header));
        }
        
        delimIndex.set(0, 0);
        for(List<String> data : datas) {
        	index = 0;
        	for(int i = 0 ; i < delimIndex.size() - 1 ; ++ i) {
        		if(data.get(i) == null)
        			data.set(i, "NULL");
        		
        		int dataLen = length(data.get(i));
        		if(dataLen > columnMaxLength.get(i)) {
        			columnMaxLength.set(i, dataLen);
        		}
        		index = delimIndex.get(i) + SPACE_BEFORE_AND_AFTER * 2 + columnMaxLength.get(i) + 1;
        		if(index > delimIndex.get(i + 1)) {
        			delimIndex.set(i + 1, index);
        		}
        	}
        }
        StringBuffer spaces = new StringBuffer();
        for(int i = 0 ; i < delimIndex.get(delimIndex.size() - 1) ; ++ i) {
            spaces.append(" ");
        }
        String spaceStr = spaces.toString();
        
        String headerLine = getHeaderLine(delimIndex);
        resultOut.println(headerLine);
        String headerDataLine = getDataLine(headers, delimIndex, spaceStr);
        resultOut.println(headerDataLine);        
        resultOut.println(headerLine);
        for(List<String> data : datas) {
        	String dataLine = getDataLine(data, delimIndex, spaceStr);
        	resultOut.println(dataLine);
        }
        resultOut.println(headerLine);
        
        return datas.size() - 1;
	}
	
    public static List<String> extractCloumns(String sql) {
    	String sample = sql.toUpperCase();
    	sample = sample.trim();
    	if(!sample.startsWith("SELECT")) {
    		return null;
    	}
    	int firstFromIndex = sample.indexOf("FROM");
    	if(firstFromIndex < 0) {
    		return null;
    	}
      
    	String arrays[]= sample.substring("SELECT".length(), firstFromIndex).split(",");

    	//需要考虑带不带AS等情况
        List<String> columns=new ArrayList<String>();
        for(String d:arrays){
            String name[]=  d.split("\\s"); //按空白分隔符划分
            //防止只有一个空格的情况
            if(name.length < 1) 
            	continue;
            columns.add(name[name.length-1]);
        }
        //不能不指定一列
        if(columns.isEmpty()) {
        	return null;
        }
        return columns;
    }
	
	public static String getDataLine(List<String> data, List<Integer> indes, String spaces) {
		StringBuffer sb = new StringBuffer();
		for(int i = 0 ; i < indes.size() - 1; ++ i) {
			sb.append("|");
			int gap = indes.get(i + 1) - indes.get(i) - 1 - length(data.get(i));
			int headSpace = gap / 2;
			int tailSpace = gap - headSpace;
			sb.append(spaces.substring(0, headSpace)).append(data.get(i)).append(spaces.substring(0, tailSpace));
		}
		sb.append("|");
		return sb.toString();
	}
	
	public static String getHeaderLine(List<Integer> indes) {
		StringBuffer sb = new StringBuffer();
		for(int i = 0 ; i < indes.size() - 1; ++ i) {
			sb.append("+");
			for(int j = indes.get(i) + 1 ; j < indes.get(i + 1) ; ++ j) {
				sb.append("-");
			}
		}
		sb.append("+");
		return sb.toString();
	}
	
    //打印结果，每一行的每一列之间空四个格
    private int printResult(List<String> headers, List<List<String>> datas) {
        int SPACE_GAP_COUNT = 4;
        List<Integer> columnLength = new ArrayList<Integer>(headers.size());
        int index = 0;
        for(String header : headers) {
            columnLength.add(index);
            index += (SPACE_GAP_COUNT + length(header));
        }
        
        for(List<String> data : datas) {
            index = 0;
            for(int i = 0 ; i < columnLength.size() ; ++ i) {
                if(i > 0)
                    index = columnLength.get(i - 1) + SPACE_GAP_COUNT + length(data.get(i - 1));
                    
                if(index > columnLength.get(i)) {
                    columnLength.set(i, index);
                }
                if(data.get(i) == null) {
                    data.set(i, "NULL");
                }
            }
        }
        StringBuffer spaces = new StringBuffer();
        for(int i = 0 ; i < columnLength.get(columnLength.size() - 1) ; ++ i) {
            spaces.append(" ");
        }
        
        StringBuffer line = new StringBuffer();
        for(int i = 0 ; i < columnLength.size() ; ++ i) {
            int gap = 0;
            if(i > 0) {
                gap = columnLength.get(i) - (columnLength.get(i - 1) + length(headers.get(i - 1)));
            }
            if(gap > 0) {
                line.append(spaces.subSequence(0, gap));
            }
            line.append(headers.get(i));
        }
        resultOut.println(line);
        
        for(List<String> data : datas) {
            line = new StringBuffer();
            for(int i = 0 ; i < columnLength.size() ; ++ i) {
                int gap = 0;
                if(i > 0) {
                    gap = columnLength.get(i) - (columnLength.get(i - 1) + length(data.get(i - 1)));
                }
                if(gap > 0) {
                    line.append(spaces.subSequence(0, gap));
                }
                line.append(data.get(i));
            }
            resultOut.println(line);
        }
        return datas.size();
    }
    
    private static int length(String str) {
        int len = str.length();
        int sumLen = 0;
        for(int i = 0 ; i < len ; ++ i) {
        	char ch = str.charAt(i);
        	if((int)ch > 0 && (int)ch < 128) {
        		sumLen ++;
        	} else {
        		sumLen += 2;
        	}
        }
        return sumLen;
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
    	
    	int ret = printResultWithTable(headers, datas);
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
