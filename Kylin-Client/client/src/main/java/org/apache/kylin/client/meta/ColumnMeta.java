package org.apache.kylin.client.meta;

public class ColumnMeta {
    private String name;
    private String type;
    private TableMeta table;
    
    public ColumnMeta(String name, String type) {
        this.name = name.toUpperCase();
        this.type = type;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }
    
    public TableMeta getTable() {
		return table;
	}

	public void setTable(TableMeta table) {
		this.table = table;
	}

	@Override
	public String toString() {
		return "ColumnMeta [name=" + name + ", type=" + type + "]";
	}
}
