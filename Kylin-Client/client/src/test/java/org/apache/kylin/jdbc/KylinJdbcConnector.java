/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * 
 *     http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

//by hzfengyu
package org.apache.kylin.jdbc;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Scanner;

import org.apache.kylin.jdbc.Driver;

public class KylinJdbcConnector {
    private static List<Table> tables = new LinkedList<Table>();
    
    public static void main(String[] args) {
        if(args.length != 1) {
            System.out.println("./cmd kylin_jdbc_url");
            System.exit(-1);
        }
        String kylinUrl = args[0];
        
        Driver driver = null;
        try {
            driver = (Driver) Class.forName("org.apache.kylin.jdbc.Driver").newInstance();
        } catch (InstantiationException | IllegalAccessException | ClassNotFoundException e) {
            System.err.println("Create new driver instance failed !");
            e.printStackTrace();
        }
        
        Properties info = new Properties();
        info.put("user", "ADMIN");
        info.put("password", "KYLIN");
        Connection conn = null;
        Statement state = null;
        try {
            conn = driver.connect(kylinUrl, info);
            state = conn.createStatement();
        } catch (SQLException e) {
            System.err.println("Connection to kylin server " + kylinUrl + " failed !");
            e.printStackTrace();
        }
        
        DatabaseMetaData meta;
        try {
            meta = conn.getMetaData();
            ResultSet result = meta.getTables(null, null, null, null);
           
            while(result.next()) {
                String databaseName = result.getString(2);
                String tableName = result.getString(3);
                
                List<Column> columns = new LinkedList<Column>();
                ResultSet columnResult = meta.getColumns(null, databaseName, tableName, null);
                while(columnResult.next()) {
                    String columnName = columnResult.getString(4);
                    String columnType = columnResult.getString(6);
                    columnType = columnType.split("\\s+")[0];
                    columns.add(new Column(columnName, columnType));
                }
                
                tables.add(new Table(databaseName, tableName, columns));
            }
        } catch (SQLException e) {
            System.out.println("Load current project table infomations failed !");
            e.printStackTrace();
        }
        
        System.out.println("Connected to kylin server " + kylinUrl);
        System.out.println("Support SQL : ");
        System.out.println("show tables");
        System.out.println("describe database.tablename");
        System.out.println("select [[distinct](dimension)], [aggerate(measure)] from FactTableName [inner/left/right join] LookupTablebName where "
                + "... group by dimension [having aggerate(measure)] [order by aggerate(measure)] [asc|desc] [limit n] [offset m]");
        
        Scanner sc = new Scanner(System.in);
        String line = nextLine(sc);
        StringBuffer sqlLine = new StringBuffer();
        while(line != null) {
            sqlLine.append(" " + line);
            if(line.trim().endsWith(";")) {
                if(!doSqlQuery(state, sqlLine.toString()))
                    break;
                sqlLine.delete(0, sqlLine.length());
            }
            line = nextLine(sc);
        }
        
        try {
            state.close();
            conn.close();
        } catch (SQLException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }

    }
    
    private static boolean doSqlQuery(Statement state, String sql) {
        sql = sql.trim();
        int lastIndex = sql.length();
        while(lastIndex >= 0 && sql.charAt(lastIndex - 1) == ';') {
            lastIndex --;
        }
        if(lastIndex == 0) {
            System.err.println("Empty query sql !");
            return true;
        }
        sql = sql.substring(0, lastIndex).trim();
        if(sql.equalsIgnoreCase("quit") || sql.equalsIgnoreCase("exit")) {
            return false;
        }
        String[] splits = sql.split("\\s+");
        if(splits.length == 2) {
            if(splits[0].trim().equalsIgnoreCase("show") && splits[1].trim().equalsIgnoreCase("tables")) {
                return showTables();
            } else if(splits[0].trim().equalsIgnoreCase("describe")) {
                String tableName = splits[1].trim();
                return describeTable(tableName);
            }
        }
        if(!sql.toLowerCase().startsWith("select")) {
            System.err.println("Not support SQL " + sql);
            return true;
        }
        
        return querySql(state, sql);
    }
    
    private static boolean querySql(Statement state, String sql) {
        try {
            long start = System.currentTimeMillis();
            ResultSet result = state.executeQuery(sql);
            ResultSetMetaData meta = result.getMetaData();
            List<String> headers = new LinkedList<String>();
            int columnSize = meta.getColumnCount();
            for(int i = 0 ; i < columnSize ; ++ i) {
                headers.add(meta.getColumnName(i + 1));
            }
            List<List<String>> datas = new LinkedList<List<String>>();

            while(result.next()) {
                List<String> data = new ArrayList<String>(columnSize);
                for(int i = 0 ; i < columnSize ; ++ i) {
                    data.add(result.getString(i + 1));
                }
                datas.add(data);
            }
            long end = System.currentTimeMillis();
            printResult(headers, datas, (end - start) / (double) 1000);
            result.close();
        } catch (SQLException e) {
            System.out.println("Execute SQL failed : " + sql);
            System.out.println(e.getMessage());
        }
        return true;
    }
    
    private static boolean showTables() {
        List<List<String>> datas = new LinkedList<List<String>>();
        for(Table table : tables) {
            datas.add(Arrays.asList(table.database, table.name));
            printResult(Arrays.asList("Database", "Table"), datas, 0.01);
        }
        return true;
    }
    
    private static boolean describeTable(String tableName) {
        String tableSplit[] = tableName.split("\\.");
        String dbName = null;
        if(tableSplit.length == 2) {
            dbName = tableSplit[0];
            tableName = tableSplit[1];
        } else {
            System.out.println("Table name " + tableName + " is illegal !");
            return true;
        }
        
        boolean find = false;
        for(Table table : tables) {
            if(dbName.equalsIgnoreCase(table.database) && tableName.equalsIgnoreCase(table.name)) {
                find = true;
                List<Column> columns = table.columns;
                List<List<String>> datas = new LinkedList<List<String>>();
                for(Column column : columns) {
                    datas.add(Arrays.asList(column.name, column.type));
                }
                printResult(Arrays.asList("Name", "Type"), datas, 0.01);
            }
        }
        if(!find) {
            System.err.println("Can not find table " + tableName);
        }
        return true;
    }
    
    private static int length(String str) {
        int len = str.length();
        if(len != str.getBytes().length) {
            return len * 2;
        } else {
            return len;
        }
    }
    
    //打印结果，每一行的每一列之间空四个格
    private static void printResult(List<String> headers, List<List<String>> datas, double sec) {
        int SPACE_GAP_COUNT = 4;
        List<Integer> columnLength = new ArrayList<Integer>(headers.size());
        int index = 0;
        for(String header : headers) {
            columnLength.add(index);
            index += (SPACE_GAP_COUNT + length(header));
        }
        StringBuffer spaces = new StringBuffer();
        for(int i = 0 ; i < index ; ++ i) {
            spaces.append(" ");
        }
        
        for(List<String> data : datas) {
            index = 0;
            for(int i = 0 ; i < columnLength.size() ; ++ i) {
                if(i > 0)
                    index = columnLength.get(i - 1) + SPACE_GAP_COUNT + length(data.get(i - 1));
                    
                if(index > columnLength.get(i)) {
                    columnLength.set(i, index);
                }
                if(data.get(i) == null) {
                    data.set(i, "NULL");
                }
            }
        }
        
        StringBuffer line = new StringBuffer();
        for(int i = 0 ; i < columnLength.size() ; ++ i) {
            int gap = 0;
            if(i > 0) {
                gap = columnLength.get(i) - (columnLength.get(i - 1) + length(headers.get(i - 1)));
            }
            if(gap > 0) {
                line.append(spaces.subSequence(0, gap));
            }
            line.append(headers.get(i));
        }
        System.out.println(line);
        System.out.println();
        
        for(List<String> data : datas) {
            line = new StringBuffer();
            for(int i = 0 ; i < columnLength.size() ; ++ i) {
                int gap = 0;
                if(i > 0) {
                    gap = columnLength.get(i) - (columnLength.get(i - 1) + length(data.get(i - 1)));
                }
                if(gap > 0) {
                    line.append(spaces.subSequence(0, gap));
                }
                line.append(data.get(i));
            }
            System.out.println(line);
        }
        System.out.println(String.format("%d rows in set (%.2f seconds)", datas.size(), sec));
    }
    
    private static String nextLine(Scanner sc) {
        System.out.print(">> ");
        System.out.flush();
        String line = sc.nextLine();
        return line;
    }
    
    private static class Column {
        private String name;
        private String type;
        
        public Column(String name, String type) {
            this.name = name;
            this.type = type;
        }
    }
    
    private static class Table {
        private String name;
        private String database;
        private List<Column> columns;
        
        public Table(String database, String tablename, List<Column> columns) {
            this.database = database;
            this.name = tablename;
            this.columns = columns;
        }
    }
}
