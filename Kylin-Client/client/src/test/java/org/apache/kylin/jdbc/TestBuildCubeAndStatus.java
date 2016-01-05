package org.apache.kylin.jdbc;

import org.apache.kylin.client.Kylin;
import org.apache.kylin.client.KylinClientException;
import org.apache.kylin.client.meta.CubeMeta;
import org.apache.kylin.client.meta.ProjectMeta;
import org.apache.kylin.client.method.Utils;
import org.apache.kylin.job.constant.JobStatusEnum;
import org.apache.kylin.job.execution.ExecutableState;

public class TestBuildCubeAndStatus {
	private static String cubeName = "nos_speed";
	private static String projectName = "NOS_STAT";
	
	public static void main(String[] args) throws Exception {
		if(args.length < 2) {
			System.out.println("./cmd ip port [username] [password]");
			return ;
		}
		
		String username = null;
		if(args.length >= 3)
			username = args[2];
		String password = null;
		if(args.length >= 4)
			password = args[3];
		
		Kylin kylin = new Kylin(args[0], Integer.valueOf(args[1]), username, password);
		ProjectMeta project = kylin.getProjectByName(projectName);
		CubeMeta cube = kylin.getCubeByName(project.getProjectName(), cubeName);
		long startTime = cube.getRangeStart();
		long start = startTime;
		String jobId = kylin.buildCube(cubeName, start, System.currentTimeMillis() - (3 * 24 * 60 * 60 * 1000));
		System.out.println("New build job ID : " + jobId);
		while(true) {
			JobStatusEnum status = kylin.getJobStatus(jobId);
			System.out.println("Check job status : " + status);
			if(status.isComplete())
				break;
			Thread.sleep(30000);
		}
	}
}
