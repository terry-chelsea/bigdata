package com.netease.impala.client;

import java.util.List;

public interface OutputHandler {
	/**
	 * 
	 * @param header, the first line to output
	 * @param datas, last lines to output
	 * 
	 * @return lines count that output.
	 */
	public int handleResult(List<String> headers, List<List<String>> datas);
	/**
	 * 
	 * @param line
	 */
	public void handleOut(String line);
	
	public void handleErr(String err);
}