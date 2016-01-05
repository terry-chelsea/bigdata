package org.apache.kylin.client.script;

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
import org.apache.kylin.job.constant.JobStatusEnum;
import org.apache.log4j.Logger;

public class DailyBuildScript {
	private static int RUNNING_JOB_COUNTER = 2;
	private static Map<CubeMeta, JobStatusEnum> results = new HashMap<CubeMeta, JobStatusEnum>();
	private static Logger logger = Logger.getLogger(DailyBuildScript.class);
	
	public static void main(String[] args) {
		if(args.length < 4) {
			System.out.println("./cmd hostname port username password");
			return ;
		}
		
		int cocurrentNumber = RUNNING_JOB_COUNTER;
		if(args.length >= 5) {
			cocurrentNumber = Integer.valueOf(args[4]);
		}
		logger.info("Start Daily Cube Build Script, Concurrent build cube number " + cocurrentNumber);
		
		String hostname = args[0];
		int port = Integer.valueOf(args[1]);
		String username = args[2];
		String password = args[3];
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
			return ;
		}
		
		logger.info("Start submit all job building task, build " + buildCubes.size() + " cubes !");
		ExecutorService executorService = Executors.newFixedThreadPool(cocurrentNumber);
		Collections.shuffle(buildCubes);
		for(CubeMeta cube : buildCubes) {
			logger.info("Need to build cube : " + cube.getCubeName() + ", project : " + cube.getProjectName());
			executorService.submit(new BuildThread(kylin, cube));
		}
		
		executorService.shutdown();
		
		boolean isFinish = false;
		while(!isFinish) {
			try {
				isFinish = executorService.awaitTermination(1, TimeUnit.HOURS);
				logger.info("Check whether thread pool is completed, result " + isFinish);
			} catch (InterruptedException e) {
				logger.warn("Await threadpool finish interrupted !", e);
				continue;
			}
		}
		
		logger.info("Finish build " + results.size() + " cubes, build statistics : ");
		for(CubeMeta cube : results.keySet()) {
			JobStatusEnum status = results.get(cube);
			logger.info("\tCube " + cube.getCubeName() + " in project " + cube.getProjectName() + " finish with " + 
					(status == null ? "SUBMIT FAILED !" : status));
		}
	}
	
	public static void addResult(CubeMeta cube, JobStatusEnum status) {
		synchronized(results) {
			results.put(cube, status);
		}
	} 
	
	public static class BuildThread implements Runnable {
		private CubeMeta cube;
		private Kylin kylin;
		public BuildThread(Kylin kylin, CubeMeta cube) {
			this.cube = cube;
			this.kylin = kylin;
		}
		
		@Override
		public void run() {
			long startTime = cube.getRangeEnd();
			long currentTime = System.currentTimeMillis();
			
			if(startTime >= currentTime) {
				logger.warn("Cube build start time " + startTime + ", current time " + currentTime + " is illegal, skip it !");
				return ;
			}
			logger.info("Start build cube " + cube.getCubeName() + " in project " + cube.getProjectName());
			String jobId = null;
			try {
				jobId = kylin.buildCube(cube.getCubeName(), startTime, currentTime);
			} catch (KylinClientException e) {
				logger.error("Build cube " + cube.getCubeName() + " from " + startTime + " to " + currentTime + " error !", e);
				addResult(cube, null);
				return ;
			}
			
			logger.info("Build cube " + cube.getCubeName() + " from " + startTime + "(" + Utils.formatToDateStr(startTime) + ")" + 
					" to " + currentTime + "(" + Utils.formatToDateStr(currentTime) + ")" + " submit success !");
			
			JobStatusEnum status = null;
			while(true) {
				try {
					status = kylin.getJobStatus(jobId);
					if(status.isComplete()) {
						break;
					} else {
						logger.info("Check job " + jobId + " status for build cube " + cube.getCubeName() + " status " + status);
					}
				} catch (KylinClientException e) {
					logger.error("Get job status for job " + jobId + ", cube name " + cube.getCubeName() + " error !", e);
				}
				
				try {
					Thread.sleep(60000);
				} catch (InterruptedException e) {
					logger.warn("Thread sleep interruptted for building cube " + cube.getCubeName(), e);
				}
			}
			
			logger.info("Cube " + cube.getCubeName() + " in project " + cube.getProjectName() + " build finished , status = " + status + 
					", Cost time " + (System.currentTimeMillis() - currentTime) / (1000 * 60) + " mins.");
			addResult(cube, status);
		}
	}
}
