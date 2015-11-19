package org.apache.kylin.client.method;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import org.apache.commons.httpclient.HttpMethod;
import org.apache.commons.httpclient.methods.PostMethod;
import org.apache.log4j.Logger;

public class Utils {
    private static Logger logger = Logger.getLogger(Utils.class);
    
    public static void close(ResultSet resultSet, Statement stat, Connection conn) {
        if (resultSet != null)
            try {
                resultSet.close();
            } catch (SQLException e) {
                logger.warn("failed to close", e);
            }
        if (stat != null)
            try {
                stat.close();
            } catch (SQLException e) {
                logger.warn("failed to close", e);
            }
        if (conn != null)
            try {
                conn.close();
            } catch (SQLException e) {
                logger.warn("failed to close", e);
            }
    }
    
    public static void close(InputStream is) {
    	if(is != null) {
    		try {
				is.close();
			} catch (IOException e) {
				logger.warn("Close inputstream filed", e);
			}
    	}
    }

}
