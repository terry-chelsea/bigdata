package com.netease.impala.client;

import java.io.PrintStream;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.List;

public class SystemOutHandler implements OutputHandler{
	private int indents = 0;
	private PrintStream writer = System.out;
	private PrintStream err = System.err;
	
	private static int SPACE_BEFORE_AND_AFTER = 1;
	public SystemOutHandler() {
		try {
			writer = new PrintStream(System.out, true, "UTF-8");
		} catch (UnsupportedEncodingException e) {
			e.printStackTrace();
		}
	}
	
	public SystemOutHandler(PrintStream writer, PrintStream err, int indents) {
		this.indents = indents;
		this.writer = writer;
	}
	
	@Override
	public int handleResult(List<String> headers, List<List<String>> datas) {
        List<Integer> delimIndex = new ArrayList<Integer>(headers.size() + 1);
        List<Integer> columnMaxLength = new ArrayList<Integer>(headers.size());
        delimIndex.add(0);
        int index = 0;
        for(int i = 0 ; i < headers.size() ; ++ i) {
        	if(headers.get(i) == null)
        		headers.set(i, "NULL");
        	String header = headers.get(i);
            index += (SPACE_BEFORE_AND_AFTER * 2 + length(header) + 1);
        	delimIndex.add(index);
        	columnMaxLength.add(length(header));
        }
        
        delimIndex.set(0, 0);
        for(List<String> data : datas) {
        	index = 0;
        	for(int i = 0 ; i < delimIndex.size() - 1 ; ++ i) {
        		if(data.get(i) == null)
        			data.set(i, "NULL");
        		
        		int dataLen = length(data.get(i));
        		if(dataLen > columnMaxLength.get(i)) {
        			columnMaxLength.set(i, dataLen);
        		}
        		index = delimIndex.get(i) + SPACE_BEFORE_AND_AFTER * 2 + columnMaxLength.get(i) + 1;
        		if(index > delimIndex.get(i + 1)) {
        			delimIndex.set(i + 1, index);
        		}
        	}
        }
        StringBuffer spaces = new StringBuffer();
        for(int i = 0 ; i < delimIndex.get(delimIndex.size() - 1) ; ++ i) {
            spaces.append(" ");
        }
        String spaceStr = spaces.toString();
        String indentStr = spaces.substring(0, indents);
        
        String headerLine = indentStr + getHeaderLine(delimIndex);
        this.writer.println(headerLine);
        String headerDataLine = indentStr + getDataLine(headers, delimIndex, spaceStr);
        this.writer.println(headerDataLine);
        this.writer.println(headerLine);
        for(List<String> data : datas) {
        	String dataLine = indentStr +getDataLine(data, delimIndex, spaceStr);
        	this.writer.println(dataLine);
        }
        this.writer.println(headerLine);
        
        return datas.size();
	}
	
	@Override
	public void handleOut(String line) {
		this.writer.println(line);
	}
	
    private int length(String str) {
        int len = str.length();
        int sumLen = 0;
        for(int i = 0 ; i < len ; ++ i) {
        	char ch = str.charAt(i);
        	if((int)ch > 0 && (int)ch < 128) {
        		sumLen ++;
        	} else {
        		//every letter width is 2 except ascii
        		sumLen += 2;
        	}
        }
        return sumLen;
    }
    
	
	private String getDataLine(List<String> data, List<Integer> indes, String spaces) {
		StringBuffer sb = new StringBuffer();
		for(int i = 0 ; i < indes.size() - 1; ++ i) {
			sb.append("|");
			int gap = indes.get(i + 1) - indes.get(i) - 1 - length(data.get(i));
			int headSpace = gap / 2;
			int tailSpace = gap - headSpace;
			sb.append(spaces.substring(0, headSpace)).append(data.get(i)).append(spaces.substring(0, tailSpace));
		}
		sb.append("|");
		return sb.toString();
	}
	
	private String getHeaderLine(List<Integer> indes) {
		StringBuffer sb = new StringBuffer();
		for(int i = 0 ; i < indes.size() - 1; ++ i) {
			sb.append("+");
			for(int j = indes.get(i) + 1 ; j < indes.get(i + 1) ; ++ j) {
				sb.append("-");
			}
		}
		sb.append("+");
		return sb.toString();
	}
	@Override
	public void handleErr(String errLine) {
		this.err.println(errLine);
	}
}