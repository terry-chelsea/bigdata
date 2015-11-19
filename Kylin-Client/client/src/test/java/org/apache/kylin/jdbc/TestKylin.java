package org.apache.kylin.jdbc;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;

import org.apache.kylin.client.Kylin;
import org.apache.kylin.client.meta.CubeDescMeta;
import org.apache.kylin.client.meta.CubeDimensionMeta;
import org.apache.kylin.client.meta.CubeMeasureMeta;
import org.apache.kylin.client.meta.CubeMeta;
import org.apache.kylin.client.meta.CubeModelMeta;
import org.apache.kylin.client.meta.LookupTableMeta;
import org.apache.kylin.client.meta.ProjectMeta;
import org.apache.kylin.client.meta.TableMeta;
import org.apache.kylin.client.method.Utils;

public class TestKylin {
	public static Random rand = new Random();
	private static int DIM_NUMBER = 2;

	public static void main(String[] args) {
		if(args.length != 2) {
			System.out.println("./command projectName cubeName");
			return ;
		}
		String projectName = args[0];
		String cubeName = args[1];
		
    	Kylin kylin = new Kylin("10.164.96.37", 7070);
    	List<ProjectMeta> projects = kylin.getAllProject();
    	
    	ProjectMeta curProject = null;
    	for(ProjectMeta project : projects) {
    		if(project.getProjectName().equalsIgnoreCase(projectName)) {
    			curProject = project;
    			break;
    		}
    	}
    	
    	if(curProject == null) {
    		System.out.println("Not found project " + projectName);
    		return ;
    	}
    	
    	CubeMeta cube = kylin.getCubeByName(curProject, cubeName);
    	if(cube == null) {
    		System.out.println("Can not found cube " + cubeName);
    	}
    	CubeDescMeta cubeDesc = kylin.getCubeDescription(cube);
    	System.out.println("Cube desc : " + cubeDesc);
    	
    	CubeModelMeta cubeModel = kylin.getCubeModel(cube);
    	
    	List<CubeMeasureMeta> measures = cubeDesc.getMeasures();
		StringBuffer measureString = new StringBuffer();
		for(int j = 0 ; j < measures.size() ; ++ j) {
			if(j > 0) {
				measureString.append(",");
			}
			measureString.append(measures.get(j).toAggerateExpression());
		}
		
    	for(int i = 0 ; i < 20 ; ++ i) {
    		String sqlString = getSqlString(cubeDesc, cubeModel, measureString.toString());
    		System.out.println(sqlString);
    		
    		executeSql(curProject.getProjectName(), kylin, sqlString);
    	}
	}
	
	private static List<String> getDimensionValues(CubeDimensionMeta dim) {
		String columnName = dim.getColumn().getName();
		String tableName = dim.getColumn().getTable().getName();
		
		String sqlFormat = "SELECT %s FROM %s GROUP BY %s";
		String sql = String.format(sqlFormat, columnName, tableName, columnName);
		return null;
	}
	
	private static String getSqlString(CubeDescMeta cubeDesc, CubeModelMeta cubeModel, String measureString) {
    	List<CubeMeasureMeta> measures = cubeDesc.getMeasures();
		List<CubeDimensionMeta> dimensions = cubeDesc.getDimensions();
		Set<CubeDimensionMeta> dims = new HashSet<CubeDimensionMeta>();
		Set<LookupTableMeta> lookups = new HashSet<LookupTableMeta>();
		
		for(int i = 0 ; i < DIM_NUMBER ; ++ i) {
			dims.add(dimensions.get(rand.nextInt(dimensions.size())));
		}
		
		for(CubeDimensionMeta dim : dims) {
			for(LookupTableMeta lookup : cubeModel.getLookupTables()) {
				if(lookup.getTable().getFullTableName().endsWith(dim.getColumn().getTable().getFullTableName())) {
					lookups.add(lookup);
					break;
				}
			}
		}
		
    	TableMeta factTable = cubeModel.getFactTable();
		Map<String, String> aliasMap = new HashMap<String, String>();
		String joinSql = getJoinSql(factTable, lookups, aliasMap);
		
		StringBuffer sql = new StringBuffer("SELECT %s, %s FROM ").append(joinSql).
				append(" GROUP BY %s ORDER BY %s %s LIMIT 10;");
		
		StringBuffer groupby = new StringBuffer();
		int i = 0;
		for(CubeDimensionMeta dim : dims) {
			if(i ++ != 0) {
				groupby.append(",");
			}
			groupby.append(aliasMap.get(dim.getColumn().getTable().getFullTableName()) + "." + dim.getColumn().getName());
		}
		
		String orderBy = measures.get(rand.nextInt(measures.size())).toAggerateExpression();
		String sqlString = String.format(sql.toString(), groupby.toString(), measureString, groupby.toString(), 
				orderBy, rand.nextBoolean() ? "DESC" : "ASC");
		
		return sqlString;
	}
	
	private static String getJoinSql(TableMeta fact, Set<LookupTableMeta> lookups, Map<String, String> alias) {
		String factTableName = fact.getName();
		alias.put(fact.getFullTableName(), "FACT");
		int lookupIndex = 1;
		
		StringBuffer sb = new StringBuffer();
		sb.append(factTableName).append(" AS FACT ");
		
		for(LookupTableMeta lookup : lookups) {
			String aliasName = "L" + (lookupIndex ++);
			sb.append(lookup.getType()).append(" JOIN ").append(lookup.getTable().getName()).
				append(" AS " + aliasName).append(" ON ");
			for(int i = 0 ; i < lookup.getPrimaryKeys().size() ; ++ i) {
				if(i != 0) {
					sb.append(" AND ");
				}
				sb.append("FACT." + lookup.getForeignKeys().get(i).getName()).append(" = ").
					append(aliasName + "." + lookup.getPrimaryKeys().get(i).getName()).append(" ");
			}
			alias.put(lookup.getTable().getFullTableName(), aliasName);
		}
		
		return sb.toString();
	}
	
	public static void executeSql(String projectName, Kylin kylin, String sql) {
		Connection conn = kylin.getJdbcConnection(projectName);
		if(conn == null) {
			System.err.println("Create jdbc connection for project " + projectName + " error");
			return;
		}
		Statement statement = null;
		ResultSet result = null;
		try {
			statement = conn.createStatement();
			result = statement.executeQuery(sql);
			if(result.next()) {
				System.out.println("success ...");
			} else {
				System.out.println("failed ...");
			}
		} catch (SQLException e) {
			e.printStackTrace();
			return;
		} finally {
			Utils.close(result, statement, conn);
		}
	}
	
}
