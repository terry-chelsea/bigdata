package org.apache.kylin.client.meta;

import java.util.List;

public class CubeModelMeta {
	private String cubeName;
	private TableMeta factTable;
	private List<LookupTableMeta> lookupTables;
	private String filter;
	private String partitionKey = null;
	
	public CubeModelMeta(String cubeName, TableMeta factTable,
			List<LookupTableMeta> lookupTables, String filter, String partitionKey) {
		super();
		this.cubeName = cubeName;
		this.factTable = factTable;
		this.lookupTables = lookupTables;
		this.filter = filter;
		this.partitionKey = partitionKey;
	}
	
	public String getPartitionKey() {
		return partitionKey;
	}

	public void setPartitionKey(String partitionKey) {
		this.partitionKey = partitionKey;
	}

	public String getCubeName() {
		return cubeName;
	}

	public void setCubeName(String cubeName) {
		this.cubeName = cubeName;
	}

	public TableMeta getFactTable() {
		return factTable;
	}

	public void setFactTable(TableMeta factTable) {
		this.factTable = factTable;
	}

	public List<LookupTableMeta> getLookupTables() {
		return lookupTables;
	}

	public void setLookupTables(List<LookupTableMeta> lookupTables) {
		this.lookupTables = lookupTables;
	}

	public String getFilter() {
		return filter;
	}

	public void setFilter(String filter) {
		this.filter = filter;
	}

	@Override
	public String toString() {
		return "CubeModelMeta [cubeName=" + cubeName + ", factTable="
				+ factTable + ", lookupTables=" + lookupTables + ", filter="
				+ filter + "]";
	}
	
}
