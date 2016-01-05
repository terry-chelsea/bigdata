package org.apache.kylin.jdbc;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;

import org.apache.kylin.client.Kylin;
import org.apache.kylin.client.meta.CubeDescMeta;
import org.apache.kylin.client.meta.CubeMeta;
import org.apache.kylin.client.meta.CubeModelMeta;
import org.apache.kylin.client.meta.ProjectMeta;
import org.apache.kylin.client.meta.TableMeta;
import org.apache.kylin.client.method.Utils;
import org.apache.kylin.cube.CubeSegment;

public class GetAllProjectTest {
    public static void main(String[] args) throws Exception {
    	Kylin kylin = new Kylin("172.17.3.102", 7070);
    	List<ProjectMeta> projects = kylin.getAllProjects();
    	
    	for(ProjectMeta project : projects) {
    		System.out.println("Porject " + project.getProjectName() + " : \n\t" + project);
    		List<CubeMeta> cubes = kylin.getCubes(project.getProjectName());
    		TableMeta factTable = null;
    		for(CubeMeta cube : cubes) {
    			System.out.println("\tCube " + cube.getCubeName() + " info : " + cube);
    			for(CubeSegment segment : cube.getSegments()) {
    				System.out.println("\t\tSegment " + segment.getName() + " info : " + segment);
    			}
    			CubeDescMeta cubeDesc = kylin.getCubeDescription(project.getProjectName(), cube.getCubeName());
    			System.out.println("\tCube " + cube.getCubeName() + " description : " + cubeDesc);
    			CubeModelMeta cubeModel = kylin.getCubeModel(project.getProjectName(), cube.getCubeName());
    			System.out.println("\tCube " + cube.getCubeName() + " model : " + cubeModel);
    			if(cube.isEnable() && factTable == null) {
    				factTable = cubeModel.getFactTable();
    			}
    		}
    		if(factTable == null)
    			continue;
    		
    		Connection conn = kylin.getJdbcConnection(project.getProjectName());
    		if(conn == null) {
    			System.err.println("Create jdbc connection for project " + project.getProjectName() + " error");
    			continue;
    		}
    		Statement statement = null;
    		ResultSet result = null;
    		try {
    			statement = conn.createStatement();
    			result = statement.executeQuery("select count(1) from " + factTable.getName() + ";");
    			while(result.next()) {
    				int count = result.getInt(1);
    				System.out.println("\tSum rows in fact table " + factTable + " is " + count);
    			}
			} catch (SQLException e) {
				e.printStackTrace();
				continue;
			} finally {
				Utils.close(result, statement, conn);
			}
    	}
    }
}
