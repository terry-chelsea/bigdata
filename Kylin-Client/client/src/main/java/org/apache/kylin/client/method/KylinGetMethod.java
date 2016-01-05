package org.apache.kylin.client.method;

import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

import org.apache.commons.httpclient.HttpClient;
import org.apache.commons.httpclient.methods.GetMethod;
import org.apache.kylin.client.KylinClientException;
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
import org.apache.kylin.job.JobInstance;
import org.apache.kylin.job.constant.JobStatusEnum;
import org.apache.kylin.metadata.model.ColumnDesc;
import org.apache.kylin.metadata.model.DataModelDesc;
import org.apache.kylin.metadata.model.FunctionDesc;
import org.apache.kylin.metadata.model.JoinDesc;
import org.apache.kylin.metadata.model.LookupDesc;
import org.apache.kylin.metadata.model.MeasureDesc;
import org.apache.kylin.metadata.model.ParameterDesc;
import org.apache.kylin.metadata.model.PartitionDesc;
import org.apache.kylin.metadata.model.SegmentStatusEnum;
import org.apache.kylin.metadata.model.TableDesc;
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
	
	public List<String> getHiveNames() throws KylinClientException {
		String url = String.format("%s/kylin/api/projects/hives", toBaseUrl());
		String jsonData = null;
		List<String> hiveNames = new LinkedList<String>();
		try {
			jsonData = getRequest(url);
			hiveNames = jsonMapper.readValue(jsonData, new TypeReference<List<String>>() {});
		} catch (IOException e) {
			throw createJsonError(url, jsonData, e);
		} 
		return hiveNames;
	}
	
	public List<ProjectMeta> getAllProject() throws KylinClientException {
		return getAllProject(0, Integer.MAX_VALUE);
	}
	
	public List<ProjectMeta> getAllProject(int offset, int limit) 
			throws KylinClientException{
		String url = String.format("%s/kylin/api/projects?limit=%d&offset=%d", toBaseUrl(), limit, offset);
		
		List<ProjectInstance> projects = null;
		String jsonData = null;
		try {
			jsonData = getRequest(url);
			projects = jsonMapper.readValue(jsonData, 
					new TypeReference<List<ProjectInstance>>() {});
		} catch (IOException e) {
			throw createJsonError(url, jsonData, e);
		} 
		List<ProjectMeta> projectMetas = new ArrayList<ProjectMeta>(projects.size());
		for(ProjectInstance project : projects) {
			if(project == null)
				continue;
			
			String hiveName = (String) Utils.checkMethod(ProjectInstance.class, project, "getHiveName");
			if(hiveName == null)
				hiveName = "NULL";
			projectMetas.add(new ProjectMeta(project.getName(), project.getDescription(), hiveName));
		}
		return projectMetas;
	}
	
	
	public List<TableMeta> getProjectSourceTables(String projectName) throws KylinClientException {
		String url = String.format("%s/kylin/api/tables?ext=false&project=%s", toBaseUrl(), projectName);
		String jsonData = null;
		List<TableDesc> tables = null;
		try {
			jsonData = getRequest(url);
			tables = jsonMapper.readValue(jsonData, new TypeReference<List<TableDesc>>() {});
		} catch (IOException e) {
			throw createJsonError(url, jsonData, e);
		} 
		List<TableMeta> tableMetas = new ArrayList<TableMeta>(tables.size());
		for(TableDesc table : tables) {
			String database = table.getDatabase();
			String tableName = table.getName();
			TableMeta meta = new TableMeta(database, tableName);
			
			if(table.getColumns() == null)
				continue;
			
			for(ColumnDesc column : table.getColumns()) {
				meta.addColumn(new ColumnMeta(column.getName(), column.getDatatype()));
			}
			tableMetas.add(meta);
		}
		return tableMetas;
	}
	
	public List<CubeMeta> getProjectCubes(ProjectMeta project) 
			throws KylinClientException {
		return getProjectCubes(project, 0, Integer.MAX_VALUE);
	}
	
	public CubeMeta getCubeByName(ProjectMeta project, String cubeName) 
			throws KylinClientException {
		String projectName = project.getProjectName();
		String url = String.format("%s/kylin/api/cubes?limit=%d&offset=%d&projectName=%s&cubeName=%s", 
				toBaseUrl(), 1, 0, projectName, cubeName);
		List<CubeMeta> cubes = getCubeMetas(url, project);
		if(cubes == null || cubes.isEmpty()) {
			throw cannotFindError("CUBE", cubeName);
		}
		
		return cubes.get(0);
	}
	
	public List<CubeMeta> getProjectCubes(ProjectMeta project, int offset, int limit) 
			throws KylinClientException {
		String projectName = project.getProjectName();
		String url = String.format("%s/kylin/api/cubes?limit=%d&offset=%d&projectName=%s", 
				toBaseUrl(), limit, offset, projectName);
		
		return getCubeMetas(url, project);
	}
	
	public CubeDescMeta getCubeDescription(ProjectMeta project, String cubeName) 
			throws KylinClientException {
		String url = String.format("%s/kylin/api/cube_desc/%s", toBaseUrl(), cubeName);
		
		CubeDesc[] cubeDescs = null;
		String jsonData = null;
		try {
			jsonData = getRequest(url);
			cubeDescs = jsonMapper.readValue(jsonData, new TypeReference<CubeDesc[]>() {});
		} catch (IOException e) {
			throw createJsonError(url, jsonData, e);
		} 
		
		if(cubeDescs == null || cubeDescs.length == 0) {
			throw cannotFindError("CUBE_DESC", cubeName);
		}
		CubeDesc cubeDesc = cubeDescs[0];
		List<CubeMeasureMeta> measures = getMeasures(cubeDesc);
		List<CubeDimensionMeta> dimensions = getDimensions(project, cubeDesc);
		
		return new CubeDescMeta(cubeName, dimensions, measures);
	}
	
	public CubeModelMeta getCubeModel(ProjectMeta project, String cubeName) 
			throws KylinClientException {
		String url = String.format("%s/kylin/api/model/%s", toBaseUrl(), cubeName);
		
		DataModelDesc modelDesc = null;
		String jsonData = null;
		try {
			jsonData = getRequest(url);
			modelDesc = jsonMapper.readValue(jsonData, new TypeReference<DataModelDesc>() {});
		} catch (IOException e) {
			throw createJsonError(url, jsonData, e);
		} 
		if(modelDesc == null) {
			throw cannotFindError("CUBE_MODEL", cubeName);
		}
		
		String factTable = modelDesc.getFactTable();
		TableMeta factTableMeta = project.getTable(factTable);
		if(factTableMeta == null) {
			throw cannotFindError("CUBE_FACT_TABLE", cubeName);
		}
		String filter = modelDesc.getFilterCondition();
		List<LookupTableMeta> lookupTables = getLookupTables(project, modelDesc, factTableMeta);
		
		String partitionColumn = null;
		PartitionDesc partition = modelDesc.getPartitionDesc();
		if(partition != null && partition.getPartitionDateColumn() != null && !partition.getPartitionDateColumn().isEmpty()) {
			partitionColumn = partition.getPartitionDateColumn();
		}
		return new CubeModelMeta(cubeName, factTableMeta, lookupTables, filter, partitionColumn);
	}
	
	private List<CubeMeta> getCubeMetas(String url, ProjectMeta project) 
			throws KylinClientException {
		List<CubeInstance> cubes = null;
		String jsonData = null;
		try {
			jsonData = getRequest(url);
			cubes = jsonMapper.readValue(jsonData, new TypeReference<List<CubeInstance>>() {});
		} catch (IOException e) {
			throw createJsonError(url, jsonData, e);
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
			boolean partition = false;
			if(!segments.isEmpty()) {
				CubeSegment firstSegment = null;
				CubeSegment lastSegment = null;
				for(CubeSegment segment : segments) {
					if(segment == null)
						continue;
					if(segment.getStatus() == SegmentStatusEnum.READY) {
						if(firstSegment == null) {
							firstSegment = segment;
						}
						lastSegment = segment;
					}
				}
				if(firstSegment != null) {
					rangeStart = firstSegment.getDateRangeStart();
				}
				if(lastSegment != null) {
					rangeEnd = lastSegment.getDateRangeEnd();
				}
			}
			
			CubeMeta cubeMeta = new CubeMeta(cubeName, enable, createTime, retentionRange, project.getProjectName(),
					cubeSize, rangeStart, rangeEnd, cube.getSegments());
			cubeMeta.setPartition(partition);
			cubeMetas.add(cubeMeta);
		}
		return cubeMetas;
	}
	
	private List<CubeMeasureMeta> getMeasures(CubeDesc cubeDesc) 
			throws KylinClientException {
		List<CubeMeasureMeta> dimensions = new LinkedList<CubeMeasureMeta>();
		for(MeasureDesc measure : cubeDesc.getMeasures()) {
			String measureName = measure.getName();
			FunctionDesc func = measure.getFunction();
			if(func == null) {
				throw cannotFindError("DIMENSION_FUNCTION_DESC", measureName);
			}
			String expression = func.getExpression();
			String returnType = func.getReturnType();
			ParameterDesc param = func.getParameter();
			dimensions.add(new CubeMeasureMeta(measureName, expression, param.getType(), param.getValue(), returnType));
		}
		
		return dimensions;
	}
	
	private List<CubeDimensionMeta> getDimensions(ProjectMeta project, CubeDesc cubeDesc) 
			throws KylinClientException {
		List<CubeDimensionMeta> dimensions = new LinkedList<CubeDimensionMeta>();
		for(DimensionDesc dim : cubeDesc.getDimensions()) {
			String dimName = dim.getName();
			String tableName = dim.getTable();
			TableMeta tableMeta = project.getTable(tableName);
			if(tableMeta == null) {
				throw cannotFindError("TABLE_IN_PROJECT", tableName);
			}
			
			String[] columns = null;
			if(dim.isDerived()) {
				columns = dim.getDerived();
			} else {
				columns = dim.getColumn();
			}
			if(columns == null || columns.length == 0) {
				throw cannotFindError("COLUMN_IN_DIMENSION", dimName);
			}
			for(String column : columns) {
				ColumnMeta meta = tableMeta.getColumn(column);
				if(meta == null) {
					throw cannotFindError("DIMENSION_COLUMN_IN_TABLE", column);
				}
				dimensions.add(new CubeDimensionMeta(dimName, meta));
			}
		}
		return dimensions;
	}
	
	private List<LookupTableMeta> getLookupTables(ProjectMeta project, DataModelDesc modelDesc, TableMeta factTable) 
			throws KylinClientException {
		List<LookupTableMeta> lookupMetas = new LinkedList<LookupTableMeta>();
		LookupDesc[] lookups = modelDesc.getLookups();
		for(LookupDesc lookup : lookups) {
			String tableName = lookup.getTable();
			TableMeta tableMeta = project.getTable(tableName);
			if(tableMeta == null) {
				throw cannotFindError("LOOKUP_TABLE_IN_PROJECT", tableName);
			}
			JoinDesc joinDesc = lookup.getJoin();
			if(joinDesc == null) {
				throw cannotFindError("JOIN_DESC_IN_LOOKUP", tableName);
			}
			String joinType = joinDesc.getType();
			String[] foreignKeys = joinDesc.getForeignKey();
			String[] primaryKeys = joinDesc.getPrimaryKey();
			List<ColumnMeta> foreignKeyColumns = factTable.getColumns(foreignKeys);
			List<ColumnMeta> primaryKeyColumns = tableMeta.getColumns(primaryKeys);
			
			if(foreignKeyColumns == null || primaryKeyColumns == null) {
				throw cannotFindError("PK_OR_FK_IN_LOOKUP", tableName);
			}
			lookupMetas.add(new LookupTableMeta(tableMeta, joinType, primaryKeyColumns, foreignKeyColumns));
		}
		return lookupMetas;
	}
	
	public JobStatusEnum getJobStatue(String jobId) throws KylinClientException {
		String url = String.format("%s/kylin/api/jobs/%s", toBaseUrl(), jobId);
		
		JobInstance job = null;
		String jsonData = null;
		try {
			jsonData = getRequest(url);
			job = jsonMapper.readValue(jsonData, JobInstance.class);
		} catch (IOException e) {
			throw createJsonError(url, jsonData, e);
		} 
		return job.getStatus();
	}
    
    private String getRequest(String url) 
    		throws KylinClientException {
    	if(this.httpClient == null)
    		return null;
    	
    	GetMethod get = new GetMethod(url);
        addHttpHeaders(get);

        logger.debug("Get URL " + url);
        synchronized(httpClient) {
	        try {
	        	httpClient.executeMethod(get);
			} catch (IOException e) {
				throw executeMethodError(url, e);
			}
	
	        if (get.getStatusCode() != 200 && get.getStatusCode() != 201) {
	        	throw errorCodeError(url, get.getStatusCode());
	        }
	
	        String response = null;
	        try {
	        	response = get.getResponseBodyAsString();
			} catch (IOException e) {
				throw createInputStreamError(url, e);
			}
	        return response;
        }
    }
    
    protected String getMethodName() {
    	return "GET";
    }
}
