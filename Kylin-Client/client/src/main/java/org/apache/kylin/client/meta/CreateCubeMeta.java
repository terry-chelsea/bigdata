package org.apache.kylin.client.meta;

import java.util.List;

public class CreateCubeMeta {
	private String cubeName;
	private List<String> notificationList;
	private String description;
	private List<CreateDimensionMeta> dimensions;
	private List<CreateMeasureMeta> measures;
	private CubeModelMeta cubeModel;
	
	private String partitionKey;
	private long startTime;
	
	private String cubeSize;
	private int retentionRange;
	private List<List<String>> groups;
	
	private List<String> mandatoryDimension;
	
	private int refreshInterval;
	
	public CreateCubeMeta() {
	}

	public CreateCubeMeta(String cubeName, CubeModelMeta model, List<String> notificationList,
			String description, List<CreateDimensionMeta> dimensions,
			List<CreateMeasureMeta> measures, String partitionKey,
			long startTime, String cubeSize, int retentionRange,
			List<List<String>> groups, List<String> mandatoryDimension,
			int refreshInterval) {
		this.cubeName = cubeName;
		this.notificationList = notificationList;
		this.description = description;
		this.dimensions = dimensions;
		this.measures = measures;
		this.partitionKey = partitionKey;
		this.startTime = startTime;
		this.cubeSize = cubeSize;
		this.retentionRange = retentionRange;
		this.groups = groups;
		this.mandatoryDimension = mandatoryDimension;
		this.cubeModel = model;
		this.refreshInterval = refreshInterval;
	}

	public String getCubeName() {
		return cubeName;
	}

	public void setCubeName(String cubeName) {
		this.cubeName = cubeName;
	}

	public List<String> getNotificationList() {
		return notificationList;
	}

	public void setNotificationList(List<String> notificationList) {
		this.notificationList = notificationList;
	}

	public String getDescription() {
		return description;
	}

	public void setDescription(String description) {
		this.description = description;
	}

	public List<CreateDimensionMeta> getDimensions() {
		return dimensions;
	}

	public void setDimensions(List<CreateDimensionMeta> dimensions) {
		this.dimensions = dimensions;
	}

	public List<CreateMeasureMeta> getMeasures() {
		return measures;
	}

	public void setMeasures(List<CreateMeasureMeta> measures) {
		this.measures = measures;
	}

	public String getPartitionKey() {
		return partitionKey;
	}

	public void setPartitionKey(String partitionKey) {
		this.partitionKey = partitionKey;
	}

	public long getStartTime() {
		return startTime;
	}

	public void setStartTime(long startTime) {
		this.startTime = startTime;
	}

	public String getCubeSize() {
		return cubeSize;
	}

	public void setCubeSize(String cubeSize) {
		this.cubeSize = cubeSize;
	}

	public int getRetentionRange() {
		return retentionRange;
	}

	public void setRetentionRange(int retentionRange) {
		this.retentionRange = retentionRange;
	}

	public List<List<String>> getGroups() {
		return groups;
	}

	public void setGroups(List<List<String>> groups) {
		this.groups = groups;
	}

	public List<String> getMandatoryDimension() {
		return mandatoryDimension;
	}

	public void setMandatoryDimension(List<String> mandatoryDimension) {
		this.mandatoryDimension = mandatoryDimension;
	}
	
	public CubeModelMeta getCubeModel() {
		return cubeModel;
	}

	public void setCubeModel(CubeModelMeta cubeModel) {
		this.cubeModel = cubeModel;
	}
	
	public int getRefreshInterval() {
		return refreshInterval;
	}

	public void setRefreshInterval(int refreshInterval) {
		this.refreshInterval = refreshInterval;
	}

	@Override
	public String toString() {
		return "CreateCubeMeta [cubeName=" + cubeName + ", notificationList="
				+ notificationList + ", description=" + description
				+ ", dimensions=" + dimensions + ", measures=" + measures
				+ ", partitionKey=" + partitionKey
				+ ", startTime=" + startTime + ", cubeSize=" + cubeSize
				+ ", retentionRange=" + retentionRange + ", groups=" + groups
				+ ", mandatoryDimension=" + mandatoryDimension + "]";
	}
	
}
