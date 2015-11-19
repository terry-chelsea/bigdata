package org.apache.kylin.client;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.httpclient.HttpClient;
import org.apache.kylin.client.meta.CubeDescMeta;
import org.apache.kylin.client.meta.CubeMeta;
import org.apache.kylin.client.meta.CubeModelMeta;
import org.apache.kylin.client.meta.ProjectMeta;
import org.apache.kylin.client.meta.TableMeta;
import org.apache.kylin.client.method.KylinDeleteMethod;
import org.apache.kylin.client.method.KylinGetMethod;
import org.apache.kylin.client.method.KylinJdbcMethod;
import org.apache.kylin.client.method.KylinPostMethod;
import org.apache.kylin.client.method.Utils;
import org.apache.log4j.Logger;

public class Kylin {
	private static Logger logger = Logger.getLogger(Kylin.class);

	private static String DEFAULT_USERNAME = "ADMIN";
	private static String DEFAULT_PASSWORD = "KYLIN";
	private HttpClient httpClient = null;

	private KylinGetMethod kylinGetMethod = null;
    private KylinPostMethod kylinPostMethod = null;
    private KylinDeleteMethod kylinDeleteMethod = null;
    private KylinJdbcMethod kylinJdbcMethod = null;
    private List<ProjectMeta> projectCache = null;

	public Kylin(String hostname, int port, String username, String password) {
		httpClient = new HttpClient();
		kylinGetMethod = new KylinGetMethod(httpClient, hostname, port, username, password);
		kylinPostMethod = new KylinPostMethod(httpClient, hostname, port, username,password);
		kylinDeleteMethod = new KylinDeleteMethod(httpClient, hostname, port, username,password);
		kylinJdbcMethod = new KylinJdbcMethod(hostname, port, username, password);
	}
	
	public Kylin(String ip, int port) {
		this(ip, port, DEFAULT_USERNAME, DEFAULT_PASSWORD);
	}
	
	public List<ProjectMeta> getAllProjects() throws KylinClientException {
		List<ProjectMeta> projects =  getAllProjects(0, Integer.MAX_VALUE);
		this.projectCache = projects;
		return projects;
	}
	
	public List<ProjectMeta> getAllProjects(int offset, int limit)
			throws KylinClientException {
		List<ProjectMeta> projectMetas = kylinGetMethod.getAllProject(offset, limit);
		logger.info("Get " + projectMetas.size() + " projects");

		for(ProjectMeta project : projectMetas) {
			String projectName = project.getProjectName();
			List<TableMeta> tables = kylinJdbcMethod.getProjectMetaTables(projectName);
			if(tables == null) {
				logger.warn("Get tables and columns from project " + projectName + " error !");
				continue;
			}
			
			project.setTableMap(tables);
		}
		
		return projectMetas;
	}
	
	public List<CubeMeta> getProjectCubes(ProjectMeta project) 
			throws KylinClientException {
		return getProjectCubes(project, 0, Integer.MAX_VALUE);
	}
	
	public ProjectMeta getProjectByName(String projectName) 
			throws KylinClientException {
		boolean loaded = false;
		if(this.projectCache == null) {
			getAllProjects();
			loaded = true;
		}
		
		//cache中找不到再load一次
		ProjectMeta ret = getProjectFromCache(projectName);
		if(loaded || ret != null) {
			return ret;
		} else {
			logger.info("Can not find project " + projectName + " in cache");
			getAllProjects();
		}
		return getProjectFromCache(projectName);
	}
	
	public ProjectMeta getProjectFromCache(String projectName) {
		ProjectMeta ret = null;
		if(this.projectCache == null) {
			logger.warn("Get all projects return null");
			return null;
		}
		for(ProjectMeta project : this.projectCache) {
			if(project != null && project.getProjectName().equals(projectName)) {
				ret = project;
				break;
			}
		}
		return ret;
	}
	public CubeMeta getCubeByName(ProjectMeta project, String cubeName) 
			throws KylinClientException {
		return kylinGetMethod.getCubeByName(project, cubeName);
	}
	
	public List<CubeMeta> getProjectCubes(ProjectMeta project, int offset, int limit) 
			throws KylinClientException{
		return kylinGetMethod.getProjectCubes(project, offset, limit);
	}
	
	public CubeDescMeta getCubeDescription(CubeMeta cube) 
			throws KylinClientException {
		return kylinGetMethod.getCubeDescription(cube);
	}
	
	public CubeModelMeta getCubeModel(CubeMeta cube) 
			throws KylinClientException {
		return kylinGetMethod.getCubeModel(cube);
	}
	
	public Connection getJdbcConnection(String projectName) 
			throws KylinClientException {
		return kylinJdbcMethod.getJdbcConnection(projectName);
	}
	
    public static void main(String[] args) throws Exception {
    	Kylin kylin = new Kylin("10.164.96.37", 7070);
    	List<ProjectMeta> projects = kylin.getAllProjects();
    	
    	for(ProjectMeta project : projects) {
    		System.out.println("Porject " + project.getProjectName() + " : \n\t" + project);
    		List<CubeMeta> cubes = kylin.getProjectCubes(project);
    		TableMeta factTable = null;
    		for(CubeMeta cube : cubes) {
    			System.out.println("\tCube " + cube.getCubeName() + " info : " + cube);
    			CubeDescMeta cubeDesc = kylin.getCubeDescription(cube);
    			System.out.println("\tCube " + cube.getCubeName() + " description : " + cubeDesc);
    			CubeModelMeta cubeModel = kylin.getCubeModel(cube);
    			System.out.println("\tCube " + cube.getCubeName() + " model : " + cubeModel);
    			if(cube.isEnable() && factTable == null) {
    				factTable = cubeModel.getFactTable();
    			}
    		}
    		if(factTable == null)
    			continue;
    		
    		Connection conn = kylin.getJdbcConnection(project.getProjectName());
    		if(conn == null) {
    			System.err.println("Create jdbc connection for project " + project.getProjectName() + " error");
    			continue;
    		}
    		Statement statement = null;
    		ResultSet result = null;
    		try {
    			statement = conn.createStatement();
    			result = statement.executeQuery("select count(1) from " + factTable.getName() + ";");
    			while(result.next()) {
    				int count = result.getInt(1);
    				System.out.println("\tSum rows in fact table " + factTable + " is " + count);
    			}
			} catch (SQLException e) {
				e.printStackTrace();
				continue;
			} finally {
				Utils.close(result, statement, conn);
			}
    	}
    }
}
