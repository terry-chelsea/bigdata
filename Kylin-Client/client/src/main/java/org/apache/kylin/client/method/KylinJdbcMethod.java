package org.apache.kylin.client.method;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.kylin.client.meta.ColumnMeta;
import org.apache.kylin.client.meta.TableMeta;
import org.apache.kylin.jdbc.Driver;
import org.apache.log4j.Logger;

public class KylinJdbcMethod extends KylinMethod {
	private static Logger logger = Logger.getLogger(KylinJdbcMethod.class);
	
	public KylinJdbcMethod(String hostname, int port, String username, String password) {
		super(hostname, port, username, password);
	}
	
	public Connection getJdbcConnection(String projectName) {
		Connection conn = null;
		
		Driver driver = null;
        try {
            driver = (Driver) Class.forName("org.apache.kylin.jdbc.Driver").newInstance();
        } catch (InstantiationException | IllegalAccessException | ClassNotFoundException e) {
            logger.error("Create new driver instance failed !", e);
            return null;
        }
        
        Properties info = new Properties();
        info.put("user", this.username);
        info.put("password", this.password);
        String jdbcUrl = toJdbcUrl(projectName);
        try {
            conn = driver.connect(jdbcUrl, info);
        } catch (Exception e) {
        	logger.error("Create connecton to jdbc url " + jdbcUrl, e);
        }
		return conn;
	}
	
    public List<TableMeta> getProjectMetaTables(String projectName) {
    	ResultSet columnMeta = null;
    	ResultSet JDBCTableMeta = null;
    	Connection conn = null;
    	try {
	    	conn = getJdbcConnection(projectName);
	    	if(conn == null) {
	    		logger.error("Get connection to project " + projectName + " error !");
	    		return null;
	    	}
	        DatabaseMetaData metaData = conn.getMetaData();
	    	logger.debug("getting table metas");
	        JDBCTableMeta = metaData.getTables(null, null, null, null);
	
	        List<TableMeta> tableMetas = new LinkedList<TableMeta>();
	        Map<String, TableMeta> tableMap = new HashMap<String, TableMeta>();
	        while (JDBCTableMeta.next()) {
	        	String catalogName = JDBCTableMeta.getString(1);
	            String schemaName = JDBCTableMeta.getString(2);
	            String tableName = JDBCTableMeta.getString(3);
	            
	            TableMeta tblMeta = new TableMeta(schemaName, tableName);
	            tableMetas.add(tblMeta);
	            tableMap.put(catalogName + "#" + tblMeta.getDatabase() + "." + tblMeta.getName(), tblMeta);
	        }
	
	        logger.debug("getting column metas");
	        columnMeta = metaData.getColumns(null, null, null, null);
	
	        while (columnMeta.next()) {
	            String catalogName = columnMeta.getString(1);
	            String schemaName = columnMeta.getString(2);
	            String tableName = columnMeta.getString(3);
	            String columnName = columnMeta.getString(4);
	            String columnType = columnMeta.getString(6).split("\\s+")[0];
	
	            // kylin(optiq) is not strictly following JDBC specification
	            ColumnMeta colmnMeta = new ColumnMeta(columnName, columnType);
	
	            tableMap.get(catalogName + "#" + schemaName + "." + tableName).addColumn(colmnMeta);
	        }
	        logger.debug("done column metas");
	        return tableMetas;
	    } catch(SQLException e) {
	    	logger.error("Exception in fetching project meta data, project name : " + projectName, e);
	    	return null;
	    } finally {
	        Utils.close(columnMeta, null, conn);
	        Utils.close(JDBCTableMeta, null, null);
	    }
    }
	
}
