package org.apache.kylin.client;

import java.io.IOException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.httpclient.HttpClient;
import org.apache.kylin.client.meta.CreateCubeMeta;
import org.apache.kylin.client.meta.CubeDescMeta;
import org.apache.kylin.client.meta.CubeMeta;
import org.apache.kylin.client.meta.CubeModelMeta;
import org.apache.kylin.client.meta.ProjectMeta;
import org.apache.kylin.client.meta.TableMeta;
import org.apache.kylin.client.method.KylinDeleteMethod;
import org.apache.kylin.client.method.KylinGetMethod;
import org.apache.kylin.client.method.KylinJdbcMethod;
import org.apache.kylin.client.method.KylinPostMethod;
import org.apache.kylin.client.method.KylinPutMethod;
import org.apache.kylin.client.method.Utils;
import org.apache.kylin.job.constant.JobStatusEnum;
import org.apache.log4j.Logger;

public class Kylin {
	private static Logger logger = Logger.getLogger(Kylin.class);

	private static String DEFAULT_USERNAME = "ANALYST";
	private static String DEFAULT_PASSWORD = "ANALYST";
	private HttpClient httpClient = null;

	private KylinGetMethod kylinGetMethod = null;
    private KylinPostMethod kylinPostMethod = null;
    private KylinDeleteMethod kylinDeleteMethod = null;
    private KylinJdbcMethod kylinJdbcMethod = null;
    private KylinPutMethod kylinPutMethod = null;
    private Map<String, ProjectMeta> projectCache = new HashMap<String, ProjectMeta>();

	public Kylin(String hostname, int port, String username, String password) {
		httpClient = new HttpClient();
		if(username == null)
			username = DEFAULT_USERNAME;
		if(password == null)
			password = DEFAULT_PASSWORD;
		kylinGetMethod = new KylinGetMethod(httpClient, hostname, port, username, password);
		kylinPostMethod = new KylinPostMethod(httpClient, hostname, port, username,password);
		kylinDeleteMethod = new KylinDeleteMethod(httpClient, hostname, port, username,password);
		kylinJdbcMethod = new KylinJdbcMethod(hostname, port, username, password);
		kylinPutMethod = new KylinPutMethod(httpClient, hostname, port, username,password);
	}
	
	public Kylin(String ip, int port) {
		this(ip, port, DEFAULT_USERNAME, DEFAULT_PASSWORD);
	}

	public List<String> getAllHiveName() throws KylinClientException {
		return kylinGetMethod.getHiveNames();
	}
	
	public boolean auth() throws IOException {
		return kylinPostMethod.auth();
	}
	
	public List<ProjectMeta> getAllProjects() throws KylinClientException {
		List<ProjectMeta> projects =  getAllProjects(0, Integer.MAX_VALUE);
		return projects;
	}
	
	public List<ProjectMeta> getAllProjects(int offset, int limit)
			throws KylinClientException {
		List<ProjectMeta> projectMetas = kylinGetMethod.getAllProject(offset, limit);
		logger.info("Get " + projectMetas.size() + " projects");

		for(ProjectMeta project : projectMetas) {
			String projectName = project.getProjectName();
			List<TableMeta> tables = kylinGetMethod.getProjectSourceTables(projectName);
			if(tables == null) {
				logger.warn("Get tables and columns from project " + projectName + " error !");
				continue;
			}
			
			project.setTableMap(tables);
			synchronized(projectCache) {
				projectCache.put(project.getProjectName(), project);
			}
		}
		
		return projectMetas;
	}
	
	public ProjectMeta getProjectByName(String projectName) 
			throws KylinClientException {
		ProjectMeta ret = getProjectFromCache(projectName);
		if(ret == null) {
			getAllProjects();
		} else {
			return ret;
		}
		
		//cache中找不到再load一次
		ret = getProjectFromCache(projectName);
		if(ret != null) {
			return ret;
		} else {
			throw new KylinClientException("Can not find project " + projectName + " in cache");
		}
	}
	
	private ProjectMeta getProjectFromCache(String projectName) {
		ProjectMeta ret = null;
		if(this.projectCache == null) {
			logger.warn("Get all projects return null");
			return null;
		}
		synchronized(projectCache) {
			ret = projectCache.get(projectName);
		}
		return ret;
	}
	
	public void cleanProjectCache() {
		synchronized(projectCache) {
			this.projectCache.clear();
		}
	}
	
	public List<CubeMeta> getCubes(String projectName) 
			throws KylinClientException {
		return getCubes(projectName, 0, Integer.MAX_VALUE);
	}
	
	public CubeMeta getCubeByName(String projectName, String cubeName) 
			throws KylinClientException {
		ProjectMeta project = getProjectByName(projectName);
		return kylinGetMethod.getCubeByName(project, cubeName);
	}
	
	public List<CubeMeta> getCubes(String projectName, int offset, int limit) 
			throws KylinClientException{
		ProjectMeta project = getProjectByName(projectName);
		return kylinGetMethod.getProjectCubes(project, offset, limit);
	}
	
	public CubeDescMeta getCubeDescription(String projectName, String cubeName)
			throws KylinClientException {
		ProjectMeta project = getProjectByName(projectName);
		return kylinGetMethod.getCubeDescription(project, cubeName);
	}
	
	public CubeModelMeta getCubeModel(String projectName, String cubeName) 
			throws KylinClientException {
		ProjectMeta project = getProjectByName(projectName);
		return kylinGetMethod.getCubeModel(project, cubeName);
	}
	
	public Connection getJdbcConnection(String projectName) 
			throws KylinClientException {
		return kylinJdbcMethod.getJdbcConnection(projectName);
	}
	
	public void createProject(ProjectMeta project) throws KylinClientException {
		kylinPostMethod.createProject(project);
	}
	
	public void createCube(CreateCubeMeta cubeMeta, String projectName) throws KylinClientException {
		kylinPostMethod.createNewCube(cubeMeta, projectName);
	}
	
	public List<String> loadTable(List<String> tableNames, String projectName) throws KylinClientException {
		return kylinPostMethod.loadTable(tableNames, projectName);
	}
	
	public String buildCube(String cubeName, long start) throws KylinClientException {
		return kylinPutMethod.buildNewSegment(cubeName, start, System.currentTimeMillis());
	}
	
	public String buildCube(String cubeName, long start, long end) throws KylinClientException {
		return kylinPutMethod.buildNewSegment(cubeName, start, end);
	}
	
	public JobStatusEnum getJobStatus(String jobId) throws KylinClientException {
		return kylinGetMethod.getJobStatue(jobId);
	}
}
