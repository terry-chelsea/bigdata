package org.apache.kylin.client.meta;

import java.util.List;

public class CubeModelMeta {
	private String cubeName;
	private TableMeta factTable;
	private List<LookupTableMeta> lookupTables;
	private String filter;
	
	private CubeMeta cubeDesc;

	public CubeModelMeta(String cubeName, TableMeta factTable,
			List<LookupTableMeta> lookupTables, String filter) {
		super();
		this.cubeName = cubeName;
		this.factTable = factTable;
		this.lookupTables = lookupTables;
		this.filter = filter;
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

	public CubeMeta getCubeDesc() {
		return cubeDesc;
	}

	public void setCubeDesc(CubeMeta cubeDesc) {
		this.cubeDesc = cubeDesc;
	}

	@Override
	public String toString() {
		return "CubeModelMeta [cubeName=" + cubeName + ", factTable="
				+ factTable + ", lookupTables=" + lookupTables + ", filter="
				+ filter + ", cubeDesc=" + cubeDesc + "]";
	}
	
}
