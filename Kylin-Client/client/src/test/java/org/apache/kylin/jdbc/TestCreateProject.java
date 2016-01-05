package org.apache.kylin.jdbc;

import java.util.List;
import java.util.Random;

import org.apache.kylin.client.Kylin;
import org.apache.kylin.client.meta.ProjectMeta;

public class TestCreateProject {
	public static void main(String[] args) throws Exception {
		if(args.length < 2) {
			System.out.println("./cmd hostname port");
			return;
		}
		Random rand = new Random();
		
		Kylin kylin = new Kylin(args[0], Integer.valueOf(args[1]));
		List<String> hiveNames = kylin.getAllHiveName();
		String hiveName = hiveNames.get(rand.nextInt(hiveNames.size()));
		if(hiveName.equalsIgnoreCase("default"))
			hiveName = null;
		
		hiveName = "hive-binjiang-default";
		String projectName = "CloudComb";
		String description = "project for CloudComb";
		ProjectMeta project = new ProjectMeta(projectName, description, hiveName);
		kylin.createProject(project);
		
		System.out.println("Create project " + project + " success !");
	}
	
	private static String getRandomName(Random rand) {
		String PREFIX = "TEST_";
		String SUFFIX = "_PROJECT";
		int len = 10;
		String strs = "abcdefghijklmnopqrstuvwxyz";
		StringBuffer sb = new StringBuffer();
		for(int i = 0 ; i < len ; ++ i)
			sb.append(strs.charAt(rand.nextInt(strs.length())));
		
		return PREFIX + sb.toString() + SUFFIX;
	}
}
