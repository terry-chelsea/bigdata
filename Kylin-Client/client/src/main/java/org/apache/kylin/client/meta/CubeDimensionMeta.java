package org.apache.kylin.client.meta;

public class CubeDimensionMeta {
	private String name;
	private ColumnMeta column;

	public CubeDimensionMeta(String name, ColumnMeta column) {
		this.name = name;
		this.column = column;
	}
	
	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public ColumnMeta getColumn() {
		return column;
	}

	public void setColumn(ColumnMeta column) {
		this.column = column;
	}

	@Override
	public String toString() {
		return "CubeDimensionMeta [column=" + column + "]";
	}
	
}
