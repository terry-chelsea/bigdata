package org.apache.kylin.client.meta;

import java.util.List;

public class LookupTableMeta {
	private TableMeta table;
	private String type;
	private List<ColumnMeta> primaryKeys;
	private List<ColumnMeta> foreignKeys;
	
	public LookupTableMeta(TableMeta table, String type,
			List<ColumnMeta> primaryKeys, List<ColumnMeta> foreignKeys) {
		super();
		this.table = table;
		this.type = type;
		this.primaryKeys = primaryKeys;
		this.foreignKeys = foreignKeys;
	}

	public TableMeta getTable() {
		return table;
	}

	public void setTable(TableMeta table) {
		this.table = table;
	}

	public String getType() {
		return type;
	}

	public void setType(String type) {
		this.type = type;
	}

	public List<ColumnMeta> getPrimaryKeys() {
		return primaryKeys;
	}

	public void setPrimaryKeys(List<ColumnMeta> primaryKeys) {
		this.primaryKeys = primaryKeys;
	}

	public List<ColumnMeta> getForeignKeys() {
		return foreignKeys;
	}

	public void setForeignKeys(List<ColumnMeta> foreignKeys) {
		this.foreignKeys = foreignKeys;
	}

	@Override
	public String toString() {
		return "LookupTableMeta [table=" + table + ", type=" + type
				+ ", primaryKeys=" + primaryKeys + ", foreignKeys="
				+ foreignKeys + "]";
	}
}
