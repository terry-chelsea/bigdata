package org.apache.kylin.client.script;

import java.io.IOException;
import java.util.List;

import org.apache.kylin.client.Kylin;
import org.apache.kylin.client.KylinClientException;
import org.apache.kylin.client.meta.ProjectMeta;

public class CreateProject {
	public static void main(String[] args) throws KylinClientException {
		if(args.length < 5) {
			System.out.println("./cmd hostname port username password project_name [hive_name] [description]");
			return;
		}
		String hostname = args[0];
		int port = Integer.valueOf(args[1]);
		String username = args[2];
		String password = args[3];
		String projectName = args[4];
		
		String hiveName = null;
		if(args.length > 5) {
			hiveName = args[5];
		}
		String description = null;
		if(args.length > 6) {
			description = args[6];
		}
		
		Kylin kylin = new Kylin(hostname, port, username, password);
		boolean auth = true;
		try {
			auth = kylin.auth();
		} catch (IOException e) {
			e.printStackTrace();
			auth = false;
		}
		if(!auth) {
			System.err.println("Username " + username + " and password " + password + " illegal !");
			return ;
		}
		List<String> hiveNames = kylin.getAllHiveName();
		if(hiveName != null && !hiveNames.contains(hiveName)) {
			System.err.println("Can not find hive with name " + hiveName + ",effective hives : " + hiveNames);
			return ;
		}
		
		ProjectMeta project = new ProjectMeta(projectName, description, hiveName);
		kylin.createProject(project);
		
		System.out.println("Create project " + project + " success !");
	}
}
