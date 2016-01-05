package org.apache.kylin.client.meta;

import java.util.List;

public class CreateDimensionMeta {
	private DimensionType type;
	private String tableName;
	private String name;
	private List<String> columns;
	
	public static enum DimensionType {
		NORMAL, HIERARCHY, DERIVED;
	}
	
	public CreateDimensionMeta(DimensionType type, String tableName, String name,
			List<String> columns) {
		this.type = type;
		this.tableName = tableName;
		this.name = name;
		this.columns = columns;
	}

	public DimensionType getType() {
		return type;
	}

	public void setType(DimensionType type) {
		this.type = type;
	}

	public String getTableName() {
		return tableName;
	}

	public void setTableName(String tableName) {
		this.tableName = tableName;
	}

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public List<String> getColumns() {
		return columns;
	}

	public void setColumns(List<String> columns) {
		this.columns = columns;
	}

	@Override
	public String toString() {
		return "CreateDimensionMeta [type=" + type + ", tableName=" + tableName
				+ ", name=" + name + ", columns=" + columns + "]";
	}
	
}
