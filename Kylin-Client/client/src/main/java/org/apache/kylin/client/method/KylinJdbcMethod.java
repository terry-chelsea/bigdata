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

import org.apache.kylin.client.KylinClientException;
import org.apache.kylin.client.meta.ColumnMeta;
import org.apache.kylin.client.meta.TableMeta;
import org.apache.kylin.jdbc.Driver;
import org.apache.log4j.Logger;

public class KylinJdbcMethod extends KylinMethod {
	private static Logger logger = Logger.getLogger(KylinJdbcMethod.class);
	private static String driverName = "org.apache.kylin.jdbc.Driver";
	
	public KylinJdbcMethod(String hostname, int port, String username, String password) {
		super(hostname, port, username, password);
	}
	
	public Connection getJdbcConnection(String projectName) 
			throws KylinClientException {
		Connection conn = null;
		
		Driver driver = null;
        try {
            driver = (Driver) Class.forName(driverName).newInstance();
        } catch (InstantiationException | IllegalAccessException | ClassNotFoundException e) {
        	throw driverError(driverName, e);
        }
        
        Properties info = new Properties();
        info.put("user", this.username);
        info.put("password", this.password);
        info.put("timezone", "GMT");
        String jdbcUrl = toJdbcUrl(projectName);
        try {
            conn = driver.connect(jdbcUrl, info);
        } catch (Exception e) {
        	throw createConnectionError(jdbcUrl, e);
        }
		return conn;
	}
	
    public List<TableMeta> getProjectMetaTables(String projectName) 
    		throws KylinClientException {
    	ResultSet columnMeta = null;
    	ResultSet JDBCTableMeta = null;
    	Connection conn = null;
    	try {
	    	conn = getJdbcConnection(projectName);
	        DatabaseMetaData metaData = conn.getMetaData();
	    	logger.debug("Getting table metas from project " + projectName);
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
	
	        logger.debug("Getting column metas from project " + projectName);
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
	        logger.debug("Done Table metas and Column metas");
	        return tableMetas;
	    } catch(SQLException e) {
	    	throw executeQueryError(projectName, "FETCH TABLES AND COLUMNS", e);
	    } finally {
	        Utils.close(columnMeta, null, conn);
	        Utils.close(JDBCTableMeta, null, null);
	    }
    }
	
    private KylinClientException driverError(String driverName, Throwable t) {
    	String errorMsg = "Create new driver " + driverName + "instance error !";
    	logger.error(errorMsg, t);
    	
    	return new KylinClientException(errorMsg, t);
    }
    
    private KylinClientException createConnectionError(String url, Throwable t) {
    	String errorMsg = "Create Connection to jdbc url " + url + " error";
    	logger.error(errorMsg, t);
    	
    	return new KylinClientException(errorMsg, t);
    }
    
    private KylinClientException executeQueryError(String project, String sql, Throwable t) {
    	String errorMsg = "Fetching data of executing " + sql + " from project " + project + " error";
    	logger.error(errorMsg, t);
    	
    	return new KylinClientException(errorMsg, t);
    }

	@Override
	protected String getMethodName() {
		return "JDBC";
	}
}
