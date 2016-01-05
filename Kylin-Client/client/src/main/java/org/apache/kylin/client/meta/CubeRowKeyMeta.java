package org.apache.kylin.client.meta;

import java.util.Arrays;

public class CubeRowKeyMeta {
    private ColDesc[] rowkey_columns;
    private String[][] aggregation_groups;
    
	public CubeRowKeyMeta(ColDesc[] rowkeyColumns, String[][] aggregationGroups) {
		super();
		this.rowkey_columns = rowkeyColumns;
		this.aggregation_groups = aggregationGroups;
	}
	
	public ColDesc[] getRowkey_columns() {
		return rowkey_columns;
	}

	public void setRowkey_columns(ColDesc[] rowkey_columns) {
		this.rowkey_columns = rowkey_columns;
	}

	public String[][] getAggregation_groups() {
		return aggregation_groups;
	}

	public void setAggregation_groups(String[][] aggregation_groups) {
		this.aggregation_groups = aggregation_groups;
	}


	public static class ColDesc {
	    public String column;
	    public int length;
	    public String dictionary;
	    public boolean mandatory = false;
	    
		public ColDesc(String column, int length, String dictionary,
				boolean mandatory) {
			this.column = column;
			this.length = length;
			this.dictionary = dictionary;
			this.mandatory = mandatory;
		}
		public String getColumn() {
			return column;
		}
		public void setColumn(String column) {
			this.column = column;
		}
		public int getLength() {
			return length;
		}
		public void setLength(int length) {
			this.length = length;
		}
		public String getDictionary() {
			return dictionary;
		}
		public void setDictionary(String dictionary) {
			this.dictionary = dictionary;
		}
		public boolean isMandatory() {
			return mandatory;
		}
		public void setMandatory(boolean mandatory) {
			this.mandatory = mandatory;
		}
		@Override
		public String toString() {
			return "ColDesc [column=" + column + ", length=" + length
					+ ", dictionary=" + dictionary + ", mandatory=" + mandatory
					+ "]";
		}
	}

	@Override
	public String toString() {
		return "CubeRowKeyMeta [rowkeyColumns="
				+ Arrays.toString(rowkey_columns) + ", aggregationGroups="
				+ Arrays.toString(aggregation_groups) + "]";
	}
	
	
}
