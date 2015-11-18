package com.terry.netease.calcite.test;

import java.util.HashMap;
import java.util.Map;

import org.apache.calcite.schema.Table;
import org.apache.calcite.schema.impl.AbstractSchema;

import com.terry.netease.calcite.test.MemoryData.Database;
import com.terry.netease.calcite.test.table.MemoryTable;

public class MemorySchema extends AbstractSchema {
    private String dbName;
    public MemorySchema(String dbName) {
        this.dbName = dbName;
    }
    
    @Override
    public Map<String, Table> getTableMap() {
        Map<String, Table> tables = new HashMap<String, Table>();
        Database database = MemoryData.MAP.get(this.dbName);
        if(database == null)
        	return tables;
        for(MemoryData.Table table : database.tables) {
            tables.put(table.tableName, new MemoryTable(table));
        }
        
        return tables;
    }
}
