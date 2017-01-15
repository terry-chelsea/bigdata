package com.netease.impala.client;

import java.util.LinkedList;
import java.util.List;

public class Result {
	private List<String> header;
	private List<List<String>> data = new LinkedList<List<String>>();
	
	public List<String> getHeader() {
		return header;
	}
	public void setHeader(List<String> header) {
		this.header = header;
	}
	public List<List<String>> getData() {
		return data;
	}
	public void setData(List<List<String>> data) {
		this.data = data;
	}
	
	public void addData(List<String> line) {
		data.add(line);
	}
}
