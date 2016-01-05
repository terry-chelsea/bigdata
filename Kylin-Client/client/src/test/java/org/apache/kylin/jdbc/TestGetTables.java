package org.apache.kylin.jdbc;

import java.util.List;

import org.apache.kylin.client.Kylin;
import org.apache.kylin.client.meta.ProjectMeta;

public class TestGetTables {
	public static void main(String[] args) throws Exception {
		if(args.length < 2) {
			System.out.println("./cmd ip port");
			return ;
		}
		
		Kylin kylin = new Kylin(args[0], Integer.valueOf(args[1]));
		List<ProjectMeta> projects = kylin.getAllProjects();
		for(ProjectMeta project : projects) {
			System.out.println("Project " + project.getProjectName() + ", use hive name " + project.getHiveName());
			System.out.println("\tProject tables : ");
			for(String table : project.getTables()) {
				System.out.println("\t\tTable " + table + " : " + project.getTable(table));
			}
		}
	}
}
