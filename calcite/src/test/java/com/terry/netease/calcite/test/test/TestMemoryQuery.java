package com.terry.netease.calcite.test.test;

import java.sql.Connection;
import java.sql.Date;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;

import org.apache.calcite.jdbc.CalciteConnection;
import org.apache.calcite.schema.impl.ScalarFunctionImpl;

import com.terry.netease.calcite.test.function.TimeOperator;

public class TestMemoryQuery {
    public static void main(String[] args) throws Exception {
    	if(args.length != 1) {
    		System.out.println("./cmd json_model");
    		return ;
    	}
    	try {
			Class.forName("org.apache.calcite.jdbc.Driver");
		} catch (ClassNotFoundException e1) {
			e1.printStackTrace();
		}
    	
        Properties info = new Properties();
        try {
            Connection connection =
                DriverManager.getConnection("jdbc:calcite:model=" + args[0], info);
            CalciteConnection calciteConn = connection.unwrap(CalciteConnection.class);
            calciteConn.getRootSchema().add("THE_YEAR", ScalarFunctionImpl.create(TimeOperator.class.getMethod("THE_YEAR", Date.class)));
            ResultSet result = connection.getMetaData().getTables(null, null, null, null);
            while(result.next()) {
                System.out.println("Catalog : " + result.getString(1) + ",Database : " + result.getString(2) + ",Table : " + result.getString(3));
            }
            result.close();
            result = connection.getMetaData().getColumns(null, null, "Student", null);
            while(result.next()) {
                System.out.println("name : " + result.getString(4) + ", type : " + result.getString(5) + ", typename : " + result.getString(6));
            }
            result.close();
            
            Statement st = connection.createStatement();
            result = st.executeQuery("select \"birthday\", 1 , count(1) from \"Student\" as S "
            		+ "INNER JOIN \"Class\" as C on S.\"classId\" = C.\"id\" group by \"birthday\"");
            while(result.next()) {
            	System.out.println(result.getString(1) + "\t" + result.getString(2) + "\t" + result.getString(3));
            }
            result.close();
            connection.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }
}
