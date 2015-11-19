package org.apache.kylin.client.meta;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class ProjectMeta {
	//project name
	private String projectName;
	private String description;
	//which hive the project used as source data
	private String hiveName;
	private Map<String, TableMeta> tableMap = new HashMap<String, TableMeta>();;
	
	public ProjectMeta(String projectName, String description, String hiveName) {
		this.projectName = projectName;
		this.description = description;
		this.hiveName = hiveName;
	}
	
	public String getProjectName() {
		return projectName;
	}
	public void setProjectName(String projectName) {
		this.projectName = projectName;
	}
	public String getDescription() {
		return description;
	}
	public void setDescription(String description) {
		this.description = description;
	}
	public String getHiveName() {
		return hiveName;
	}
	public void setHiveName(String hiveName) {
		this.hiveName = hiveName;
	}
	
	public void setTableMap(List<TableMeta> tables) {
		for(TableMeta table : tables) {
			tableMap.put(table.getFullTableName(), table);
		}
	}
	
	public TableMeta getTable(String tableName) {
		return this.tableMap.get(tableName.toUpperCase());
	}
	
	public Set<String> getTables() {
		return this.tableMap.keySet();
	}

	@Override
	public String toString() {
		return "ProjectMeta [projectName=" + projectName + ", description="
				+ description + ", hiveName=" + hiveName + "]";
	}
	
}
