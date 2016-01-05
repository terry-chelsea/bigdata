package org.apache.kylin.client.script;

import java.io.File;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.apache.kylin.client.Kylin;
import org.apache.kylin.client.KylinClientException;
import org.apache.kylin.client.meta.CubeMeta;
import org.apache.kylin.client.meta.CubeModelMeta;
import org.apache.kylin.client.meta.ProjectMeta;
import org.apache.kylin.client.method.Utils;
import org.apache.kylin.client.script.DailyBuildScript.BuildThread;
import org.apache.kylin.job.constant.JobStatusEnum;
import org.apache.log4j.Logger;

public class BuildCubesOnTime {
	private String hostname;
	private int port;
	private String username;
	private String password;
	private String configFile;
	
	private static int RUNNING_JOB_COUNTER = 2;
	private static Logger logger = Logger.getLogger(BuildCubesOnTime.class);
	
	public BuildCubesOnTime(String hostname, int port, String username,
			String password, String configFile) {
		super();
		this.hostname = hostname;
		this.port = port;
		this.username = username;
		this.password = password;
		this.configFile = configFile;
	}
	
	public void run(int threadNum) {
		ExecutorService executorService = Executors.newFixedThreadPool(threadNum);
		File config = new File(this.configFile);
		long lastModify = 0;
		while(true) {
			if(lastModify < config.lastModified()) {
				
			}
			try {
				Thread.sleep(60000);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
	}

	public static void main(String[] args) {
		if(args.length < 5) {
			System.out.println("./cmd hostname port username password config_file [cocurrent_number]");
			return ;
		}
		
		System.out.println("Config file format : cube_name=build_time(hh:mm:ss)"
				+ "\nFor example : qingguo=09:00:00 means build cube named qingguo at 09:00:00 every day."
				+ "\nIf the build_time is a accurate time, the cube will build once at that time!");
		
		int cocurrentNumber = RUNNING_JOB_COUNTER;
		if(args.length >= 6) {
			cocurrentNumber = Integer.valueOf(args[5]);
		}
		logger.info("Start Cube Build Script, Concurrent build cube number " + cocurrentNumber);
		
		String hostname = args[0];
		int port = Integer.valueOf(args[1]);
		String username = args[2];
		String password = args[3];
		String configFile = args[4];
		
		BuildCubesOnTime builder = new BuildCubesOnTime(hostname, port, username, password, configFile);
		
		while(true) {
			builder.run(cocurrentNumber);
			logger.error("---------------------run returned !");
		}
	}
	
	public List<CubeMeta> getAllBuildCubes() {
		List<CubeMeta> buildCubes = new LinkedList<CubeMeta>();
		Kylin kylin = new Kylin(hostname, port, username, password);
		try {
			List<ProjectMeta> projects = kylin.getAllProjects();
			for(ProjectMeta project : projects) {
				List<CubeMeta> cubes = kylin.getCubes(project.getProjectName());
				for(CubeMeta cube : cubes) {
					CubeModelMeta model = kylin.getCubeModel(project.getProjectName(), cube.getCubeName());
					if(cube.isEnable() && model.getPartitionKey() != null) {
						buildCubes.add(cube);
					}
				}
			}
		} catch (KylinClientException e) {
			logger.error("Get All Cubes error !", e);
		}
		return buildCubes;
	}
}