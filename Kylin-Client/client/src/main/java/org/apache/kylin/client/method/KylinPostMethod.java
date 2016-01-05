package org.apache.kylin.client.method;

import java.io.IOException;
import java.io.InputStream;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import org.apache.commons.httpclient.HttpClient;
import org.apache.commons.httpclient.methods.GetMethod;
import org.apache.commons.httpclient.methods.PostMethod;
import org.apache.commons.httpclient.methods.StringRequestEntity;
import org.apache.kylin.client.KylinClientException;
import org.apache.kylin.client.meta.CreateCubeMeta;
import org.apache.kylin.client.meta.CreateDimensionMeta;
import org.apache.kylin.client.meta.CreateDimensionMeta.DimensionType;
import org.apache.kylin.client.meta.ColumnMeta;
import org.apache.kylin.client.meta.CreateMeasureMeta;
import org.apache.kylin.client.meta.CubeMeasureMeta;
import org.apache.kylin.client.meta.CubeModelMeta;
import org.apache.kylin.client.meta.CubeRowKeyMeta;
import org.apache.kylin.client.meta.CubeRowKeyMeta.ColDesc;
import org.apache.kylin.client.meta.LookupTableMeta;
import org.apache.kylin.client.meta.ProjectMeta;
import org.apache.kylin.client.request.CreateProjectRequest;
import org.apache.kylin.client.request.CubeRequest;
import org.apache.kylin.cube.model.CubeDesc;
import org.apache.kylin.cube.model.DimensionDesc;
import org.apache.kylin.cube.model.HBaseColumnDesc;
import org.apache.kylin.cube.model.HBaseColumnFamilyDesc;
import org.apache.kylin.cube.model.HBaseMappingDesc;
import org.apache.kylin.cube.model.RowKeyDesc;
import org.apache.kylin.metadata.model.DataModelDesc;
import org.apache.kylin.metadata.model.DataModelDesc.RealizationCapacity;
import org.apache.kylin.metadata.model.DataType;
import org.apache.kylin.metadata.model.FunctionDesc;
import org.apache.kylin.metadata.model.JoinDesc;
import org.apache.kylin.metadata.model.LookupDesc;
import org.apache.kylin.metadata.model.MeasureDesc;
import org.apache.kylin.metadata.model.ParameterDesc;
import org.apache.kylin.metadata.model.PartitionDesc;
import org.apache.kylin.metadata.model.PartitionDesc.PartitionType;
import org.apache.kylin.metadata.project.ProjectInstance;
import org.apache.log4j.Logger;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;

public class KylinPostMethod extends KylinMethod {
	private Logger logger = Logger.getLogger(KylinPostMethod.class);
	private HttpClient httpClient = null;
	
	public KylinPostMethod(HttpClient httpClient, String hostname, int port, String username, String password) {
		super(hostname, port, username, password);
		this.httpClient = httpClient;
	}
	
    public boolean auth() throws IOException {
        PostMethod post = new PostMethod(toBaseUrl() + "/kylin/api/user/authentication");
        addHttpHeaders(post);
        StringRequestEntity requestEntity = new StringRequestEntity("{}", "application/json", "UTF-8");
        post.setRequestEntity(requestEntity);

        httpClient.executeMethod(post);

        if (post.getStatusCode() != 200 && post.getStatusCode() != 201) {
            logger.error("Authentication to kylin server response code " + post.getStatusCode() + 
            		", response string : " + getMethodResponseString(post));
            return false;
        }
        return true;
    }
    
    public ProjectInstance createProject(ProjectMeta project) throws KylinClientException {
    	CreateProjectRequest projectObj = new CreateProjectRequest();
    	projectObj.setDescription(project.getDescription());
    	projectObj.setHiveName(project.getHiveName());
    	projectObj.setName(project.getProjectName());
    	String jsonData = null;
    	
    	String url = String.format("%s/kylin/api/projects", toBaseUrl());
    	try {
			jsonData = postRequest(url, projectObj);
			ProjectInstance projectInstance = jsonMapper.readValue(jsonData, ProjectInstance.class);
			logger.debug("Create project " + projectInstance + " success...");
			return projectInstance;
		} catch (IOException e) {
			throw createJsonError(url, jsonData, e);
		} 
    }
    
    public List<String> loadTable(List<String> tables, String projectName) throws KylinClientException {
    	if(tables == null || tables.isEmpty())
    		return tables;
    	StringBuffer tableNames = new StringBuffer();
    	for(String table : tables) {
    		tableNames.append(table).append(",");
    	}
    	
    	String tableNameStr = tableNames.substring(0, tableNames.length() - 1);
    	String url = String.format("%s/kylin/api/tables/%s/%s", toBaseUrl(), tableNameStr, projectName);
    	String jsonData = null;
    	try {
			jsonData = postRequest(url, null);
			Map<String, List<String>> response = jsonMapper.readValue(jsonData, Map.class);
			List<String> succTables = response.get("result.loaded");
			logger.debug("Load table " + succTables + " success...");
			return succTables;
		} catch (IOException e) {
			throw createJsonError(url, jsonData, e);
		} 
    }
    
    public CubeRequest createNewCube(CreateCubeMeta cube, String projectName)
    		throws KylinClientException {
    	String uuid = UUID.randomUUID().toString();
    	DataModelDesc modelDesc = transformModel(cube);
    	
    	CubeDesc desc = transformCubeDesc(cube);
    	
    	CubeRequest request = new CubeRequest();
    	try {
			request.setCubeDescData(jsonMapper.writeValueAsString(desc));
			request.setCubeDescName(null);
			request.setCubeName(null);
			request.setMessage(null);
			request.setModelDescData(jsonMapper.writeValueAsString(modelDesc));
			request.setProject(projectName);
			request.setRetentionRange(cube.getRetentionRange());
			request.setSuccessful(true);
			request.setUuid(uuid);
		} catch (JsonProcessingException e) {
			throw new IllegalArgumentException("write cube desc " + desc + " and model " + modelDesc + " to json error !");
		} 
    	
    	String url = String.format("%s/kylin/api/cubes", toBaseUrl());
    	String jsonData = null;
    	try {
			jsonData = postRequest(url, request);
			CubeRequest cubeInstance = jsonMapper.readValue(jsonData, CubeRequest.class);
			logger.debug("Create cube " + cubeInstance + " success...");
			return cubeInstance;
		} catch (IOException e) {
			throw createJsonError(url, jsonData, e);
		} 
    }
    
    private CubeDesc transformCubeDesc(CreateCubeMeta cube) {
    	List<CreateDimensionMeta> dimensions = cube.getDimensions();
    	List<CreateMeasureMeta> measures = cube.getMeasures();
    	if(dimensions == null || dimensions.isEmpty()) {
    		throw new IllegalArgumentException("Can not find dimensions in cube " + cube.getCubeName());
    	}
    	if(measures == null || measures.isEmpty()) {
    		throw new IllegalArgumentException("Can not find measures in cube " + cube.getCubeName());
    	}
    	CubeDesc desc = new CubeDesc();
    	
    	desc.setDescription(cube.getDescription());
    	desc.setName(cube.getCubeName());
       	
    	desc.setNotifyList(cube.getNotificationList());
    	
    	desc.setLastModified(0l);
    	desc.setModelName(cube.getCubeName());
    	desc.setName(cube.getCubeName());
    	desc.setNullStrings(null);
    	desc.setRetentionRange(cube.getRetentionRange());
    	desc.setSignature(null);
    	desc.setUuid(null);
    	
    	List<DimensionDesc> dimDescs = new ArrayList<DimensionDesc>(dimensions.size());
    	for(int i = 0 ; i < dimensions.size() ; ++ i) {
    		DimensionDesc dimDesc = new DimensionDesc();
    		CreateDimensionMeta dimMeta = dimensions.get(i);
    		dimDesc.setId(i + 1);
    		dimDesc.setName(dimMeta.getName());
    		dimDesc.setTable(dimMeta.getTableName());
    		
    		if(dimMeta.getType().equals(CreateDimensionMeta.DimensionType.NORMAL)) {
    			dimDesc.setColumn(dimMeta.getColumns().toArray(new String[0]));
    			dimDesc.setDerived(null);
    		} else if(dimMeta.getType().equals(CreateDimensionMeta.DimensionType.DERIVED)) {
    			dimDesc.setColumn(null);
    			dimDesc.setDerived(dimMeta.getColumns().toArray(new String[0]));
    		} else if(dimMeta.getType().equals(CreateDimensionMeta.DimensionType.HIERARCHY)) {
    			dimDesc.setColumn(dimMeta.getColumns().toArray(new String[0]));
    			dimDesc.setDerived(null);
    			dimDesc.setHierarchy(true);
    		} else {
    			logger.warn("Undefined dimension type " + dimMeta.getType());
    			continue;
    		}
    		dimDescs.add(dimDesc);
    	}
    	desc.setDimensions(dimDescs);
    	
    	List<MeasureDesc> measureDescs = new LinkedList<MeasureDesc>();
    	int count = 0;
    	for(CreateMeasureMeta measureMeta : measures) {
    		List<String> expressions = measureMeta.getExpressions();
    		if(expressions == null)
    			continue;
    		
    		for(String expr : expressions) {
    			MeasureDesc measureDesc = new MeasureDesc();
    			measureDesc.setDependentMeasureRef(null);
    			measureDesc.setId(count ++);
    			measureDesc.setName(measureMeta.getName() + "_" + expr.toUpperCase());
    			FunctionDesc func = new FunctionDesc();
    			func.setExpression(expr);
    			String dataType = measureMeta.getDataType();
    			if(DataType.INTEGER_FAMILY.contains(dataType)) {
    				func.setReturnType("bigint");
    			} else if(DataType.NUMBER_FAMILY.contains(dataType)) {
    				func.setReturnType("double");
    			} else {
    				throw new IllegalArgumentException("measure column must be integer type or number type, actually is " + dataType);
    			}
    		
    			ParameterDesc parameter = new ParameterDesc();
    			parameter.setType(measureMeta.getType().toString());
    			parameter.setValue(measureMeta.getValue());
    			func.setParameter(parameter);
    			measureDesc.setFunction(func);
    		
    			measureDescs.add(measureDesc);
    		}
    	}
    	
    	desc.setHbaseMapping(getDefaultHbaseFamlily(measureDescs));
    	desc.setRowkey(createRowKey(cube));

    	desc.setMeasures(measureDescs);
		return desc;
    }
    
    private HBaseMappingDesc getDefaultHbaseFamlily(List<MeasureDesc> measures) {
    	HBaseMappingDesc mapping = new HBaseMappingDesc();
    	HBaseColumnFamilyDesc desc = new HBaseColumnFamilyDesc();
    	
    	desc.setName("f1");
    	HBaseColumnDesc column = new HBaseColumnDesc();
    	column.setQualifier("m");
    	String[] measureNames = new String[measures.size()];
    	for(int i = 0 ; i < measureNames.length ; ++ i) {
    		measureNames[i] = measures.get(i).getName();
    	}
    	column.setMeasureRefs(measureNames);
    	desc.setColumns(new HBaseColumnDesc[]{column});
    	mapping.setColumnFamily(new HBaseColumnFamilyDesc[]{desc});
    	
    	return mapping;
    }
    
    private RowKeyDesc createRowKey(CreateCubeMeta cube) {
    	Map<String, String> columnToTable = new HashMap<String, String>();
    	for(CreateDimensionMeta dimension : cube.getDimensions()) {
    		for(String column : dimension.getColumns()) {
    			String tableName = columnToTable.get(column);
    			String useTable = dimension.getTableName();
    			if(tableName != null && !tableName.equals(useTable)) {
    				throw new IllegalArgumentException("Two dimension use the same column name " + column + 
    						" with different table " + tableName + " and " + useTable);
    			}
    			columnToTable.put(column, useTable);
    		}
    	}
    	
    	CubeModelMeta modelMeta = cube.getCubeModel();
    	Map<String, LookupTableMeta> lookupMap = new HashMap<String, LookupTableMeta>();
    	if(modelMeta.getLookupTables() != null) {
    		for(LookupTableMeta lookup : modelMeta.getLookupTables()) {
    			lookupMap.put(lookup.getTable().getFullTableName(), lookup);
    		}
    	}
    	
    	Set<String> mandatorySet = new HashSet<String>();
    	if(cube.getMandatoryDimension() != null) {
    		mandatorySet.addAll(cube.getMandatoryDimension());
    	}
    	List<ColDesc> rowKeyColDesc = new LinkedList<ColDesc>();
    	Set<String> derivedColumns = new HashSet<String>();
    	for(CreateDimensionMeta dimension : cube.getDimensions()) {
    		if(dimension == null)
    			continue;
    		
    		List<String> columns = new LinkedList<String>();
    		if(dimension.getType() == DimensionType.DERIVED) {
    			LookupTableMeta lookup = lookupMap.get(dimension.getTableName().toUpperCase());
    			if(lookup == null) {
    				throw new IllegalArgumentException("Can not find lookup table for dimension " + dimension + ", model " + modelMeta);
    			}
    			for(ColumnMeta columnMeta : lookup.getForeignKeys()) {
    				columns.add(columnMeta.getName());
    			}
    			derivedColumns.addAll(dimension.getColumns());
    		} else {
    			columns = dimension.getColumns();
    		}
    		
    		for(String column : columns) {
    			if(mandatorySet.contains(column)) {
    				rowKeyColDesc.add(new ColDesc(column, 0, "true", true));
    			} else {
    				rowKeyColDesc.add(new ColDesc(column, 0, "true", false));
    			}
    		}
    	}
    	
    	if(cube.getGroups() == null) {
    		//如果不指定则使用全部的column作为一个group
    		List<String> group = new ArrayList<String>(columnToTable.keySet());
    		cube.setGroups(Arrays.asList(group));
    	}
    	
    	int groupCount = cube.getGroups().size();
    	String[][] groupNames = new String[groupCount][];
    	int i = 0;
    	for(List<String> group : cube.getGroups()) {
    		Set<String> realGroup = new HashSet<String>();
    		for(String column : group) {
    			if(derivedColumns.contains(column)) {
    				String tableName = columnToTable.get(column).toUpperCase();
    				if(tableName == null) {
    					throw new IllegalArgumentException("Can not find table for dimension column " + column + 
    							",dimensions " + cube.getDimensions() + ", model " + modelMeta);
    				}
    				LookupTableMeta lookup = lookupMap.get(tableName);
        			if(lookup == null) {
        				throw new IllegalArgumentException("Can not find lookup table for dimension column" + column + 
        						",dimensions " + cube.getDimensions() + ", model " + modelMeta);
        			}
        			for(ColumnMeta columnMeta : lookup.getForeignKeys()) {
        				realGroup.add(columnMeta.getName());
        			}
    			} else if(!mandatorySet.contains(column)) {
    				realGroup.add(column);
    			}
    		}
    		groupNames[i ++] = realGroup.toArray(new String[0]);
    	}
    	CubeRowKeyMeta rowKey = new CubeRowKeyMeta(rowKeyColDesc.toArray(new ColDesc[0]), groupNames);
    	
    	try {
        	String rowKeyJson = jsonMapper.writeValueAsString(rowKey);
        	RowKeyDesc rowkeyDesc = jsonMapper.readValue(rowKeyJson, RowKeyDesc.class);
        	return rowkeyDesc;
		} catch (IOException e) {
			throw new IllegalArgumentException("Can not transform row key from " + rowKey);
		}
    }
    
    private DataModelDesc transformModel(CreateCubeMeta cube) {
    	CubeModelMeta model = cube.getCubeModel();
    	DataModelDesc modelDesc = new DataModelDesc();
    	modelDesc.setFactTable(model.getFactTable().getFullTableName());
    	modelDesc.setCapacity(RealizationCapacity.valueOf(cube.getCubeSize()));
    	modelDesc.setFilterCondition(model.getFilter());
    	modelDesc.setLastModified(0);
    	modelDesc.setName(cube.getCubeName());
    	List<LookupTableMeta> lookupTables = model.getLookupTables();
    	if(lookupTables != null) {
    		LookupDesc[] lookups = new LookupDesc[lookupTables.size()];
    		for(int i = 0 ; i < lookupTables.size() ; ++ i) {
    			LookupTableMeta meta = lookupTables.get(i);
    			lookups[i] = new LookupDesc();
    			
    			JoinDesc join = new JoinDesc();
    			join.setType(meta.getType());
    			String[] foreignKeys = new String[meta.getForeignKeys().size()];
    			String[] primaryKeys = new String[meta.getPrimaryKeys().size()];
    			for(int j = 0 ; j < foreignKeys.length ; ++ j) {
    				foreignKeys[j] = meta.getForeignKeys().get(j).getName();
    				primaryKeys[j] = meta.getPrimaryKeys().get(j).getName();
    			}
    			join.setForeignKey(foreignKeys);
    			join.setPrimaryKey(primaryKeys);
    			
    			lookups[i].setJoin(join);
    			lookups[i].setTable(meta.getTable().getFullTableName());
    		}
    		modelDesc.setLookups(lookups);
    	}
    	PartitionDesc partition = new PartitionDesc();
    	partition.setCubePartitionType(PartitionType.APPEND);
    	partition.setPartitionDateColumn(cube.getPartitionKey());
    	partition.setPartitionDateStart(cube.getStartTime());
    	modelDesc.setPartitionDesc(partition);
    	
    	return modelDesc;
    }

    
    @SuppressWarnings("deprecation")
	protected String postRequest(String url, Object obj) 
    		throws KylinClientException {
    	if(this.httpClient == null)
    		return null;
    	
    	PostMethod post = new PostMethod(url);
        addHttpHeaders(post);
        if(obj != null) {
	        try {
	        	String param = jsonMapper.writeValueAsString(obj);
	        	logger.debug("Post parameter : " + param);
				post.setRequestEntity(new StringRequestEntity(param, null, "UTF-8"));
			} catch (JsonProcessingException | UnsupportedEncodingException e1) {
				throw createJsonError(url, obj.toString(), e1);
			}
        }

        logger.debug("POST URL " + url);

        synchronized(httpClient) {
	        try {
	        	httpClient.executeMethod(post);
			} catch (IOException e) {
				throw executeMethodError(url, e);
			}
	
	        if (post.getStatusCode() != 200) {
	        	throw errorCodeError(url, post.getStatusCode());
	        }
	
	        String response = null;
	        try {
	        	response = post.getResponseBodyAsString();
			} catch (IOException e) {
				throw createInputStreamError(url, e);
			}
	        return response;
        }
    }
    
	@Override
	protected String getMethodName() {
		return "POST";
	}
}
