package com.terry.netease.calcite.test.test;

public class TestReplace {
	public static void main(String[] args) {
		String text = "<div><b>Build Result of Job ${job_name}</b><pre><ul>" + "<li>Build Result: <b>${result}</b></li>" + "<li>Job Engine: ${job_engine}</li>" + "<li>Cube Name: ${cube_name}</li>" + "<li>Source Records Count: ${source_records_count}</li>" + "<li>Start Time: ${start_time}</li>" + "<li>Duration: ${duration}</li>" + "<li>MR Waiting: ${mr_waiting}</li>" + "<li>Last Update Time: ${last_update_time}</li>" + "<li>Submitter: ${submitter}</li>" + "<li>Error Log: ${error_log}</li>" + "</ul></pre><div/>";
		String errorLog = "test$test";
		String DOLLAR_CHARACTER = "KYLIN_RDS_CHAR_DOLLAR";
		errorLog = errorLog.replaceAll("\\$", DOLLAR_CHARACTER);
		System.out.println(text.replaceAll("\\$\\{error_log\\}", errorLog).replaceAll(DOLLAR_CHARACTER, "\\$"));
	}
}
