package com.terry.netease.calcite.test.test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;

public class TestMemoryQuery {
    public static void main(String[] args) {
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
            result = st.executeQuery("select THE_YEAR(\"birthday\"), 1 , count(1) from \"Student\" as S "
            		+ "INNER JOIN \"Class\" as C on S.\"classId\" = C.\"id\" group by THE_YEAR(\"birthday\")");
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
