package org.apache.kylin.client.method;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.PrintStream;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.httpclient.HttpMethod;
import org.apache.commons.httpclient.methods.PostMethod;
import org.apache.log4j.Logger;

public class Utils {
    private static Logger logger = Logger.getLogger(Utils.class);
    
    public static void close(ResultSet resultSet, Statement stat, Connection conn) {
        if (resultSet != null)
            try {
                resultSet.close();
            } catch (SQLException e) {
                logger.warn("failed to close", e);
            }
        if (stat != null)
            try {
                stat.close();
            } catch (SQLException e) {
                logger.warn("failed to close", e);
            }
        if (conn != null)
            try {
                conn.close();
            } catch (SQLException e) {
                logger.warn("failed to close", e);
            }
    }
    
    public static void close(InputStream is) {
    	if(is != null) {
    		try {
				is.close();
			} catch (IOException e) {
				logger.warn("Close inputstream filed", e);
			}
    	}
    }

	public static PrintStream RESULT_OUT = System.out;
	
	//print result with table, just like mysql
	public static int printResultWithTable(List<String> headers, List<List<String>> datas, int indents) {
		int SPACE_BEFORE_AND_AFTER = 1;
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
        RESULT_OUT.println(headerLine);
        String headerDataLine = indentStr + getDataLine(headers, delimIndex, spaceStr);
        RESULT_OUT.println(headerDataLine);
        RESULT_OUT.println(headerLine);
        for(List<String> data : datas) {
        	String dataLine = indentStr +getDataLine(data, delimIndex, spaceStr);
        	RESULT_OUT.println(dataLine);
        }
        RESULT_OUT.println(headerLine);
        
        return datas.size() - 1;
	}
	
	public static String getDataLine(List<String> data, List<Integer> indes, String spaces) {
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
	
	public static String getHeaderLine(List<Integer> indes) {
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
	
    //打印结果，每一行的每一列之间空四个格
    public int printResult(List<String> headers, List<List<String>> datas, int indents) {
        int SPACE_GAP_COUNT = 4;
        List<Integer> columnLength = new ArrayList<Integer>(headers.size());
        int index = 0;
        for(String header : headers) {
            columnLength.add(index);
            index += (SPACE_GAP_COUNT + length(header));
        }
        
        for(List<String> data : datas) {
            index = 0;
            for(int i = 0 ; i < columnLength.size() ; ++ i) {
                if(i > 0)
                    index = columnLength.get(i - 1) + SPACE_GAP_COUNT + length(data.get(i - 1));
                    
                if(index > columnLength.get(i)) {
                    columnLength.set(i, index);
                }
                if(data.get(i) == null) {
                    data.set(i, "NULL");
                }
            }
        }
        StringBuffer spaces = new StringBuffer();
        for(int i = 0 ; i < columnLength.get(columnLength.size() - 1) ; ++ i) {
            spaces.append(" ");
        }
        if(indents < 0) {
        	indents = 0;
        }
        String indentStr = spaces.substring(0, indents);
        
        StringBuffer line = new StringBuffer();
        for(int i = 0 ; i < columnLength.size() ; ++ i) {
            int gap = 0;
            if(i > 0) {
                gap = columnLength.get(i) - (columnLength.get(i - 1) + length(headers.get(i - 1)));
            }
            if(gap > 0) {
                line.append(spaces.subSequence(0, gap));
            }
            line.append(headers.get(i));
        }
        RESULT_OUT.println(indentStr + line);
        
        for(List<String> data : datas) {
            line = new StringBuffer();
            for(int i = 0 ; i < columnLength.size() ; ++ i) {
                int gap = 0;
                if(i > 0) {
                    gap = columnLength.get(i) - (columnLength.get(i - 1) + length(data.get(i - 1)));
                }
                if(gap > 0) {
                    line.append(spaces.subSequence(0, gap));
                }
                line.append(data.get(i));
            }
            RESULT_OUT.println(indentStr + line);
        }
        return datas.size();
    }
    
    private static int length(String str) {
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
    
    public static List<String> extractCloumns(String sql) {
    	String sample = sql.toUpperCase();
    	sample = sample.trim();
    	if(!sample.startsWith("SELECT")) {
    		return null;
    	}
    	int firstFromIndex = sample.indexOf("FROM");
    	if(firstFromIndex < 0) {
    		return null;
    	}
      
    	String arrays[]= sample.substring("SELECT".length(), firstFromIndex).split(",");

    	//需要考虑带不带AS等情况
        List<String> columns=new ArrayList<String>();
        for(String d:arrays){
            String name[]=  d.split("\\s"); //按空白分隔符划分
            //防止只有一个空格的情况
            if(name.length < 1) 
            	continue;
            columns.add(name[name.length-1]);
        }
        //不能不指定一列
        if(columns.isEmpty()) {
        	return null;
        }
        return columns;
    }
}
