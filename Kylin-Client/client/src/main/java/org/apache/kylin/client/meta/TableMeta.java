package org.apache.kylin.client.meta;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

public class TableMeta {
    private String name;
    private String database;
    private List<ColumnMeta> columns = new LinkedList<ColumnMeta>();
    
    public TableMeta(String database, String tablename) {
        this.database = database.toUpperCase();
        this.name = tablename.toUpperCase();
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getDatabase() {
        return database;
    }

    public void setDatabase(String database) {
        this.database = database;
    }

    public List<ColumnMeta> getColumns() {
        return columns;
    }

    public void setColumns(List<ColumnMeta> columns) {
        this.columns = columns;
    }
    
    public String getFullTableName() {
    	return database + "." + name;
    }
    
    public void addColumn(ColumnMeta column) {
    	column.setTable(this);
    	this.columns.add(column);
    }
    
    public ColumnMeta getColumn(String columnName) {
		for(ColumnMeta column : columns) {
			if(column.getName().equalsIgnoreCase(columnName)) {
				return column;
			}
		}
    	return null;
    }
    
    public List<ColumnMeta> getColumns(String[] columnNames) {
    	List<ColumnMeta> metas = new ArrayList<ColumnMeta>();
    	for(String columnName : columnNames) {
    		ColumnMeta meta = getColumn(columnName);
    		if(meta != null) {
    			metas.add(meta);
    		}
    	}
    	return metas;
    }

    @Override
    public String toString() {
        return "Table [name=" + name + ", database=" + database + ", columns=" + columns + "]";
    }
}