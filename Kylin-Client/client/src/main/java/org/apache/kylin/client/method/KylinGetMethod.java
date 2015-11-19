package org.apache.kylin.client.method;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

import org.apache.commons.httpclient.HttpClient;
import org.apache.commons.httpclient.methods.GetMethod;
import org.apache.kylin.client.meta.ColumnMeta;
import org.apache.kylin.client.meta.CubeDescMeta;
import org.apache.kylin.client.meta.CubeDimensionMeta;
import org.apache.kylin.client.meta.CubeMeasureMeta;
import org.apache.kylin.client.meta.CubeMeta;
import org.apache.kylin.client.meta.CubeModelMeta;
import org.apache.kylin.client.meta.LookupTableMeta;
import org.apache.kylin.client.meta.ProjectMeta;
import org.apache.kylin.client.meta.TableMeta;
import org.apache.kylin.cube.CubeInstance;
import org.apache.kylin.cube.CubeSegment;
import org.apache.kylin.cube.model.CubeDesc;
import org.apache.kylin.cube.model.DimensionDesc;
import org.apache.kylin.metadata.model.DataModelDesc;
import org.apache.kylin.metadata.model.FunctionDesc;
import org.apache.kylin.metadata.model.JoinDesc;
import org.apache.kylin.metadata.model.LookupDesc;
import org.apache.kylin.metadata.model.MeasureDesc;
import org.apache.kylin.metadata.model.ParameterDesc;
import org.apache.kylin.metadata.model.SegmentStatusEnum;
import org.apache.kylin.metadata.project.ProjectInstance;
import org.apache.log4j.Logger;

import com.fasterxml.jackson.core.type.TypeReference;

public class KylinGetMethod extends KylinMethod {
	private static Logger logger = Logger.getLogger(KylinGetMethod.class);
	private HttpClient httpClient = null;
	
	public KylinGetMethod(HttpClient httpClient, String hostname, int port, String username, String password) {
		super(hostname, port, username, password);
		this.httpClient = httpClient;
	}
	
	public List<ProjectMeta> getAllProject() {
		return getAllProject(0, Integer.MAX_VALUE);
	}
	
	public List<ProjectMeta> getAllProject(int offset, int limit) {
		String url = String.format("%s/kylin/api/projects?limit=%d&offset=%d", toBaseUrl(), limit, offset);
		InputStream is = getRequest(url);
		if(is == null) 
			return null;
		
		List<ProjectInstance> projects = null;
		try {
			projects = jsonMapper.readValue(is, new TypeReference<List<ProjectInstance>>() {});
		} catch (IOException e) {
			logger.error("Get all projects response from " + url + " parse with json error !", e);
			return null;
		} finally {
			Utils.close(is);
		}
		List<ProjectMeta> projectMetas = new ArrayList<ProjectMeta>(projects.size());
		for(ProjectInstance project : projects) {
			if(project == null)
				continue;
			
			projectMetas.add(new ProjectMeta(project.getName(), project.getDescription(), "NULL"));
		}
		return projectMetas;
	}
	
	public List<CubeMeta> getProjectCubes(ProjectMeta project) {
		return getProjectCubes(project, 0, Integer.MAX_VALUE);
	}
	
	public CubeMeta getCubeByName(ProjectMeta project, String cubeName) {
		String projectName = project.getProjectName();
		String url = String.format("%s/kylin/api/cubes?limit=%d&offset=%d&projectName=%s&cubeName=%s", 
				toBaseUrl(), 1, 0, projectName, cubeName);
		List<CubeMeta> cubes = getCubeMetas(url, project);
		if(cubes == null || cubes.isEmpty()) {
			logger.warn("Can not find cube by name " + cubeName);
			return null;
		}
		
		return cubes.get(0);
	}
	public List<CubeMeta> getProjectCubes(ProjectMeta project, int offset, int limit) {
		String projectName = project.getProjectName();
		String url = String.format("%s/kylin/api/cubes?limit=%d&offset=%d&projectName=%s", 
				toBaseUrl(), limit, offset, projectName);
		
		return getCubeMetas(url, project);
	}
	
	public CubeDescMeta getCubeDescription(CubeMeta cube) {
		String cubeName = cube.getCubeName();
		ProjectMeta project = cube.getProject();
		String url = String.format("%s/kylin/api/cube_desc/%s", toBaseUrl(), cubeName);
		
		InputStream is = getRequest(url);
		if(is == null) 
			return null;
		CubeDesc[] cubeDescs = null;
		try {
			cubeDescs = jsonMapper.readValue(is, new TypeReference<CubeDesc[]>() {});
		} catch (IOException e) {
			logger.error("Get cube description response from " + url + " parse with json error !", e);
			return null;
		} finally {
			Utils.close(is);
		}
		
		if(cubeDescs == null || cubeDescs.length == 0) {
			logger.warn("Get cube desc from server is null");
			return null;
		}
		CubeDesc cubeDesc = cubeDescs[0];
		List<CubeMeasureMeta> measures = getMeasures(cubeDesc);
		List<CubeDimensionMeta> dimensions = getDimensions(project, cubeDesc);
		
		return new CubeDescMeta(cube, dimensions, measures);
	}
	
	public CubeModelMeta getCubeModel(CubeMeta cube) {
		ProjectMeta project = cube.getProject();
		String cubeName = cube.getCubeName();
		String url = String.format("%s/kylin/api/model/%s", toBaseUrl(), cubeName);
		InputStream is = getRequest(url);
		if(is == null) 
			return null;
		
		DataModelDesc modelDesc = null;
		try {
			modelDesc = jsonMapper.readValue(is, new TypeReference<DataModelDesc>() {});
		} catch (IOException e) {
			logger.error("Get all cubes response from " + url + " parse with json error !", e);
			return null;
		} finally {
			Utils.close(is);
		}
		if(modelDesc == null) {
			logger.warn("Get cube model from server is null");
			return null;
		}
		
		String factTable = modelDesc.getFactTable();
		TableMeta factTableMeta = project.getTable(factTable);
		if(factTableMeta == null) {
			logger.warn("Can not find fact table " + factTable + "in project " + project);
			return null;
		}
		String filter = modelDesc.getFilterCondition();
		List<LookupTableMeta> lookupTables = getLookupTables(project, modelDesc, factTableMeta);

		return new CubeModelMeta(cubeName, factTableMeta, lookupTables, filter);
	}
	
	private List<CubeMeta> getCubeMetas(String url, ProjectMeta project) {
		InputStream is = getRequest(url);
		if(is == null) 
			return null;
		
		List<CubeInstance> cubes = null;
		try {
			cubes = jsonMapper.readValue(is, new TypeReference<List<CubeInstance>>() {});
		} catch (IOException e) {
			logger.error("Get all cubes response from " + url + " parse with json error !", e);
			return null;
		} finally {
			Utils.close(is);
		}
		
		List<CubeMeta> cubeMetas = new ArrayList<CubeMeta>(cubes.size());
		for(CubeInstance cube : cubes) {
			if(cube == null)
				continue;
			
			String cubeName = cube.getName();
			boolean enable = cube.isReady();
			long createTime = cube.getCreateTimeUTC();
			long retentionRange = cube.getRetentionRange();
			long cubeSize = cube.getSizeKB();
			List<CubeSegment> segments = cube.getSegment(SegmentStatusEnum.READY);
			long rangeStart = 0l;
			long rangeEnd = 0l;
			if(!segments.isEmpty()) {
				rangeStart = segments.get(0).getDateRangeStart();
				rangeEnd = segments.get(segments.size() - 1).getDateRangeEnd();
			}
			
			CubeMeta cubeMeta = new CubeMeta(cubeName, enable, createTime, retentionRange, project.getProjectName(),
					cubeSize, rangeStart, rangeEnd);
			cubeMeta.setProjectName(project);
			cubeMetas.add(cubeMeta);
		}
		return cubeMetas;
	}
	
	private List<CubeMeasureMeta> getMeasures(CubeDesc cubeDesc) {
		List<CubeMeasureMeta> dimensions = new LinkedList<CubeMeasureMeta>();
		for(MeasureDesc measure : cubeDesc.getMeasures()) {
			String measureName = measure.getName();
			FunctionDesc func = measure.getFunction();
			if(func == null) {
				logger.warn("Can not find function in measure " + measure);
				continue;
			}
			String expression = func.getExpression();
			String returnType = func.getReturnType();
			ParameterDesc param = func.getParameter();
			dimensions.add(new CubeMeasureMeta(measureName, expression, param.getValue(), returnType));
		}
		
		return dimensions;
	}
	
	private List<CubeDimensionMeta> getDimensions(ProjectMeta project, CubeDesc cubeDesc) {
		List<CubeDimensionMeta> dimensions = new LinkedList<CubeDimensionMeta>();
		for(DimensionDesc dim : cubeDesc.getDimensions()) {
			String dimName = dim.getName();
			String tableName = dim.getTable();
			TableMeta tableMeta = project.getTable(tableName);
			if(tableMeta == null) {
				logger.warn("Table " + tableName + " in dimension " + dim + " not exist in " + project);
				continue;
			}
			
			String[] columns = null;
			if(dim.isDerived()) {
				columns = dim.getDerived();
			} else {
				columns = dim.getColumn();
			}
			if(columns == null || columns.length == 0) {
				logger.warn("Can not get column in dimension " + dim);
				continue;
			}
			for(String column : columns) {
				ColumnMeta meta = tableMeta.getColumn(column);
				if(meta == null) {
					logger.warn("Can not find column " + column + " in table " + tableMeta);
					continue;
				}
				dimensions.add(new CubeDimensionMeta(dimName, meta));
			}
		}
		return dimensions;
	}
	
	private List<LookupTableMeta> getLookupTables(ProjectMeta project, DataModelDesc modelDesc, TableMeta factTable) {
		List<LookupTableMeta> lookupMetas = new LinkedList<LookupTableMeta>();
		LookupDesc[] lookups = modelDesc.getLookups();
		for(LookupDesc lookup : lookups) {
			String tableName = lookup.getTable();
			TableMeta tableMeta = project.getTable(tableName);
			if(tableMeta == null) {
				logger.warn("Can not find lookup table " + tableName + " in project " + project);
				continue;
			}
			JoinDesc joinDesc = lookup.getJoin();
			if(joinDesc == null) {
				logger.warn("Can not find join info in lookup table " + tableName);
				continue;
			}
			String joinType = joinDesc.getType();
			String[] foreignKeys = joinDesc.getForeignKey();
			String[] primaryKeys = joinDesc.getPrimaryKey();
			List<ColumnMeta> foreignKeyColumns = factTable.getColumns(foreignKeys);
			List<ColumnMeta> primaryKeyColumns = tableMeta.getColumns(primaryKeys);
			
			if(foreignKeyColumns == null || primaryKeyColumns == null) {
				logger.warn("Can not find foreign key or primary key for lookup table " + lookup + " in project " + project);
				continue;
			}
			lookupMetas.add(new LookupTableMeta(tableMeta, joinType, primaryKeyColumns, foreignKeyColumns));
		}
		return lookupMetas;
	}
    
    private InputStream getRequest(String url) {
    	if(this.httpClient == null)
    		return null;
    	
    	GetMethod get = new GetMethod(url);
        addHttpHeaders(get);

        logger.debug("Get URL " + url);

        try {
        	httpClient.executeMethod(get);
		} catch (IOException e) {
			logger.error("Get exception while execute get method to " + url, e);
			return null;
		}

        if (get.getStatusCode() != 200 && get.getStatusCode() != 201) {
        	logger.error("Get url " + url + " get error code " + get.getStatusCode());
        	return null;
        }

        try {
			return get.getResponseBodyAsStream();
		} catch (IOException e) {
			logger.error("Get response of getting url " + url + " error !", e);
			return null;
		}
    }
    
}
